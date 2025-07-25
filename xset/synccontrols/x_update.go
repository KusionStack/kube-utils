/*
Copyright 2024-2025 The KusionStack Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package synccontrols

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	appsv1alpha1 "kusionstack.io/kube-api/apps/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"kusionstack.io/kube-utils/controller/expectations"
	"kusionstack.io/kube-utils/controller/merge"
	controllerutils "kusionstack.io/kube-utils/controller/utils"
	"kusionstack.io/kube-utils/xset/api"
	"kusionstack.io/kube-utils/xset/opslifecycle"
	"kusionstack.io/kube-utils/xset/resourcecontexts"
	"kusionstack.io/kube-utils/xset/xcontrol"
)

const UnknownRevision = "__unknownRevision__"

func (r *RealSyncControl) attachTargetUpdateInfo(xsetObject api.XSetObject, syncContext *SyncContext) []*targetUpdateInfo {
	activeTargets := FilterOutActiveTargetWrappers(syncContext.TargetWrappers)
	targetUpdateInfoList := make([]*targetUpdateInfo, len(activeTargets))

	for i, target := range activeTargets {
		updateInfo := &targetUpdateInfo{
			targetWrapper:        &syncContext.TargetWrappers[i],
			InPlaceUpdateSupport: true,
		}

		updateInfo.UpdateRevision = syncContext.UpdatedRevision
		// decide this target current revision, or nil if not indicated
		if target.GetLabels() != nil {
			currentRevisionName, exist := target.GetLabels()[appsv1.ControllerRevisionHashLabelKey]
			if exist {
				if currentRevisionName == syncContext.UpdatedRevision.GetName() {
					updateInfo.IsUpdatedRevision = true
					updateInfo.CurrentRevision = syncContext.UpdatedRevision
				} else {
					updateInfo.IsUpdatedRevision = false
					for _, rv := range syncContext.Revisions {
						if currentRevisionName == rv.GetName() {
							updateInfo.CurrentRevision = rv
						}
					}
				}
			}
		}

		// default CurrentRevision is an empty revision
		if updateInfo.CurrentRevision == nil {
			updateInfo.CurrentRevision = &appsv1.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{
					Name: UnknownRevision,
				},
			}
			r.Recorder.Eventf(target.Object,
				corev1.EventTypeWarning,
				"TargetCurrentRevisionNotFound",
				"target is going to be updated by recreate because: (1) controller-revision-hash label not found, or (2) not found in history revisions")
		}

		spec := r.xsetController.GetXSetSpec(xsetObject)
		// decide whether the TargetOpsLifecycle is during ops or not
		updateInfo.IsDuringOps = target.IsDuringUpdateOps
		updateInfo.RequeueForOperationDelay, updateInfo.IsAllowOps = opslifecycle.AllowOps(r.updateConfig.opsLifecycleMgr, r.updateLifecycleAdapter, RealValue(spec.UpdateStrategy.OperationDelaySeconds), target)

		targetUpdateInfoList[i] = updateInfo
	}

	// attach replace info
	targetUpdateInfoMap := make(map[string]*targetUpdateInfo)
	for _, targetUpdateInfo := range targetUpdateInfoList {
		targetUpdateInfoMap[targetUpdateInfo.GetName()] = targetUpdateInfo
	}
	for originTargetName, replacePairNewTarget := range syncContext.replacingMap {
		originTargetInfo := targetUpdateInfoMap[originTargetName]
		if replacePairNewTarget != nil {
			originTargetInfo.IsInReplacing = true
			// replace origin target not go through lifecycle, mark  during ops manual
			originTargetInfo.IsDuringOps = true
			originTargetInfo.IsAllowOps = true
			ReplacePairNewTargetInfo := targetUpdateInfoMap[replacePairNewTarget.GetName()]
			ReplacePairNewTargetInfo.IsInReplacing = true
			ReplacePairNewTargetInfo.ReplacePairOriginTargetName = originTargetName
			originTargetInfo.ReplacePairNewTargetInfo = ReplacePairNewTargetInfo
		} else {
			_, replaceIndicated := originTargetInfo.GetLabels()[TargetReplaceIndicationLabelKey]
			_, replaceByReplaceUpdate := originTargetInfo.GetLabels()[TargetReplaceByReplaceUpdateLabelKey]
			if replaceIndicated && replaceByReplaceUpdate {
				originTargetInfo.IsInReplacing = true
				originTargetInfo.IsDuringOps = true
				originTargetInfo.IsAllowOps = true
			}
		}
	}

	// join PlaceHolder targets in updating
	for _, target := range syncContext.TargetWrappers {
		if !target.PlaceHolder {
			continue
		}
		updateInfo := &targetUpdateInfo{
			targetWrapper:        &target,
			InPlaceUpdateSupport: true,
			UpdateRevision:       syncContext.UpdatedRevision,
		}
		if revision, exist := target.ContextDetail.Data[resourcecontexts.RevisionContextDataKey]; exist &&
			revision == syncContext.UpdatedRevision.GetName() {
			updateInfo.IsUpdatedRevision = true
		}
		targetUpdateInfoList = append(targetUpdateInfoList, updateInfo)
	}

	return targetUpdateInfoList
}

func filterOutPlaceHolderUpdateInfos(targets []*targetUpdateInfo) []*targetUpdateInfo {
	var filteredTargetUpdateInfos []*targetUpdateInfo
	for _, target := range targets {
		if target.PlaceHolder {
			continue
		}
		filteredTargetUpdateInfos = append(filteredTargetUpdateInfos, target)
	}
	return filteredTargetUpdateInfos
}

func decideTargetToUpdate(xsetController api.XSetController, xset api.XSetObject, targetInfos []*targetUpdateInfo) []*targetUpdateInfo {
	spec := xsetController.GetXSetSpec(xset)
	if spec.UpdateStrategy.RollingUpdate != nil && spec.UpdateStrategy.RollingUpdate.ByLabel != nil {
		activeTargetInfos := filterOutPlaceHolderUpdateInfos(targetInfos)
		return decideTargetToUpdateByLabel(xset, activeTargetInfos)
	}

	return decideTargetToUpdateByPartition(xsetController, xset, targetInfos)
}

func decideTargetToUpdateByLabel(_ api.XSetObject, targetInfos []*targetUpdateInfo) (targetToUpdate []*targetUpdateInfo) {
	for i := range targetInfos {
		if _, exist := targetInfos[i].GetLabels()[XSetUpdateIndicateLabelKey]; exist {
			// filter target which is in replace update and is the new created target
			if targetInfos[i].IsInReplacing && targetInfos[i].ReplacePairOriginTargetName != "" {
				continue
			}
			targetToUpdate = append(targetToUpdate, targetInfos[i])
			continue
		}

		// already in replace update.
		if targetInfos[i].IsInReplacing && targetInfos[i].ReplacePairNewTargetInfo != nil {
			targetToUpdate = append(targetToUpdate, targetInfos[i])
			continue
		}
	}
	return targetToUpdate
}

func decideTargetToUpdateByPartition(xsetController api.XSetController, xset api.XSetObject, targetInfos []*targetUpdateInfo) []*targetUpdateInfo {
	spec := xsetController.GetXSetSpec(xset)
	replicas := ptr.Deref(spec.Replicas, 0)
	partition := int32(0)

	if spec.UpdateStrategy.RollingUpdate != nil && spec.UpdateStrategy.RollingUpdate.ByPartition != nil {
		partition = ptr.Deref(spec.UpdateStrategy.RollingUpdate.ByPartition.Partition, 0)
	}

	filteredTargetInfos := getTargetsUpdateTargets(targetInfos)
	// update all or not update any replicas
	if partition == 0 {
		return filteredTargetInfos
	}
	if partition >= replicas {
		return nil
	}

	// partial update replicas
	ordered := newOrderedTargetUpdateInfos(filteredTargetInfos, xsetController.CheckReady)
	sort.Sort(ordered)
	targetToUpdate := ordered.targets[:replicas-partition]
	return targetToUpdate
}

// when sort targets to choose update, only sort (1) replace origin targets, (2) non-exclude targets
func getTargetsUpdateTargets(targetInfos []*targetUpdateInfo) (filteredTargetInfos []*targetUpdateInfo) {
	for _, targetInfo := range targetInfos {
		if targetInfo.IsInReplacing && targetInfo.ReplacePairOriginTargetName != "" {
			continue
		}

		if targetInfo.PlaceHolder {
			_, isReplaceNewTarget := targetInfo.ContextDetail.Data[ReplaceOriginTargetIDContextDataKey]
			_, isReplaceOriginTarget := targetInfo.ContextDetail.Data[ReplaceNewTargetIDContextDataKey]
			if isReplaceNewTarget || isReplaceOriginTarget {
				continue
			}
		}

		filteredTargetInfos = append(filteredTargetInfos, targetInfo)
	}
	return filteredTargetInfos
}

func newOrderedTargetUpdateInfos(targetInfos []*targetUpdateInfo, checkReadyFunc func(object client.Object) bool) *orderByDefault {
	return &orderByDefault{
		targets:        targetInfos,
		checkReadyFunc: checkReadyFunc,
	}
}

type orderByDefault struct {
	targets        []*targetUpdateInfo
	checkReadyFunc func(object client.Object) bool
}

func (o *orderByDefault) Len() int {
	return len(o.targets)
}

func (o *orderByDefault) Swap(i, j int) { o.targets[i], o.targets[j] = o.targets[j], o.targets[i] }

func (o *orderByDefault) Less(i, j int) bool {
	l, r := o.targets[i], o.targets[j]
	if l.IsUpdatedRevision != r.IsUpdatedRevision {
		return l.IsUpdatedRevision
	}

	if l.PlaceHolder != r.PlaceHolder {
		return r.PlaceHolder
	}

	if l.PlaceHolder && r.PlaceHolder {
		return true
	}

	if l.IsDuringOps != r.IsDuringOps {
		return l.IsDuringOps
	}

	lReady, rReady := o.checkReadyFunc(l.Object), o.checkReadyFunc(r.Object)
	if lReady != rReady {
		return lReady
	}

	lCreationTime := l.Object.GetCreationTimestamp().Time
	rCreationTime := r.Object.GetCreationTimestamp().Time
	return lCreationTime.Before(rCreationTime)
}

type UpdateConfig struct {
	xsetController api.XSetController
	client         client.Client
	targetControl  xcontrol.TargetControl
	recorder       record.EventRecorder

	opsLifecycleMgr         api.LifeCycleLabelManager
	scaleInLifecycleAdapter api.LifecycleAdapter
	updateLifecycleAdapter  api.LifecycleAdapter

	cacheExpectation *expectations.CacheExpectation
	targetGVK        schema.GroupVersionKind
}

type TargetUpdater interface {
	Setup(config *UpdateConfig, xset api.XSetObject)
	FulfillTargetUpdatedInfo(ctx context.Context, revision *appsv1.ControllerRevision, targetUpdateInfo *targetUpdateInfo) error
	BeginUpdateTarget(ctx context.Context, syncContext *SyncContext, targetCh chan *targetUpdateInfo) (bool, error)
	FilterAllowOpsTargets(ctx context.Context, targetToUpdate []*targetUpdateInfo, ownedIDs map[int]*appsv1alpha1.ContextDetail, syncContext *SyncContext, targetCh chan *targetUpdateInfo) (*time.Duration, error)
	UpgradeTarget(ctx context.Context, targetInfo *targetUpdateInfo) error
	GetTargetUpdateFinishStatus(ctx context.Context, targetUpdateInfo *targetUpdateInfo) (bool, string, error)
	FinishUpdateTarget(ctx context.Context, targetInfo *targetUpdateInfo) error
}

type GenericTargetUpdater struct {
	OwnerObject api.XSetObject

	*UpdateConfig
}

func (u *GenericTargetUpdater) Setup(config *UpdateConfig, xset api.XSetObject) {
	u.UpdateConfig = config
	u.OwnerObject = xset
}

func (u *GenericTargetUpdater) BeginUpdateTarget(_ context.Context, syncContext *SyncContext, targetCh chan *targetUpdateInfo) (bool, error) {
	succCount, err := controllerutils.SlowStartBatch(len(targetCh), controllerutils.SlowStartInitialBatchSize, false, func(int, error) error {
		targetInfo := <-targetCh
		u.recorder.Eventf(targetInfo.Object, corev1.EventTypeNormal, "TargetUpdateLifecycle", "try to begin TargetOpsLifecycle for updating Target of XSet")

		if updated, err := opslifecycle.BeginWithCleaningOld(u.opsLifecycleMgr, u.client, u.updateLifecycleAdapter, targetInfo.Object, func(obj client.Object) (bool, error) {
			if !targetInfo.OnlyMetadataChanged && !targetInfo.InPlaceUpdateSupport {
				return opslifecycle.WhenBeginDelete(u.opsLifecycleMgr, obj)
			}
			return false, nil
		}); err != nil {
			return fmt.Errorf("fail to begin TargetOpsLifecycle for updating Target %s/%s: %s", targetInfo.GetNamespace(), targetInfo.GetName(), err.Error())
		} else if updated {
			// add an expectation for this target update, before next reconciling
			if err := u.cacheExpectation.ExpectUpdation(u.targetGVK, targetInfo.GetNamespace(), targetInfo.GetName(), targetInfo.GetResourceVersion()); err != nil {
				return err
			}
		}

		return nil
	})

	updating := succCount > 0
	if err != nil {
		AddOrUpdateCondition(syncContext.NewStatus, api.XSetUpdate, err, "UpdateFailed", err.Error())
		return updating, err
	} else {
		AddOrUpdateCondition(syncContext.NewStatus, api.XSetUpdate, nil, "Updated", "")
	}
	return updating, nil
}

func (u *GenericTargetUpdater) FilterAllowOpsTargets(_ context.Context, candidates []*targetUpdateInfo, ownedIDs map[int]*appsv1alpha1.ContextDetail, _ *SyncContext, targetCh chan *targetUpdateInfo) (*time.Duration, error) {
	var recordedRequeueAfter *time.Duration
	needUpdateContext := false
	for i := range candidates {
		targetInfo := candidates[i]

		if !targetInfo.PlaceHolder {
			if !targetInfo.IsAllowOps {
				continue
			}
			if targetInfo.RequeueForOperationDelay != nil {
				u.recorder.Eventf(targetInfo, corev1.EventTypeNormal, "TargetUpdateLifecycle", "delay Target update for %f seconds", targetInfo.RequeueForOperationDelay.Seconds())
				if recordedRequeueAfter == nil || *targetInfo.RequeueForOperationDelay < *recordedRequeueAfter {
					recordedRequeueAfter = targetInfo.RequeueForOperationDelay
				}
				continue
			}
		}

		targetInfo.IsAllowOps = true

		if targetInfo.IsUpdatedRevision {
			continue
		}

		if _, exist := ownedIDs[targetInfo.ID]; !exist {
			u.recorder.Eventf(u.OwnerObject, corev1.EventTypeWarning, "TargetBeforeUpdate", "target %s/%s is not allowed to update because cannot find context id %s in resourceContext", targetInfo.GetNamespace(), targetInfo.GetName(), targetInfo.GetLabels()[TargetInstanceIDLabelKey])
			continue
		}

		if !ownedIDs[targetInfo.ID].Contains(resourcecontexts.RevisionContextDataKey, targetInfo.UpdateRevision.GetName()) {
			needUpdateContext = true
			ownedIDs[targetInfo.ID].Put(resourcecontexts.RevisionContextDataKey, targetInfo.UpdateRevision.GetName())
		}

		spec := u.xsetController.GetXSetSpec(u.OwnerObject)

		// mark targetContext "TargetRecreateUpgrade" if upgrade by recreate
		isRecreateUpdatePolicy := spec.UpdateStrategy.UpdatePolicy == api.XSetRecreateTargetUpdateStrategyType
		if (!targetInfo.OnlyMetadataChanged && !targetInfo.InPlaceUpdateSupport) || isRecreateUpdatePolicy {
			ownedIDs[targetInfo.ID].Put(resourcecontexts.RecreateUpdateContextDataKey, "true")
		}

		if targetInfo.PlaceHolder {
			continue
		}

		// if Target has not been updated, update it.
		targetCh <- candidates[i]
	}
	// mark Target to use updated revision before updating it.
	if needUpdateContext {
		u.recorder.Eventf(u.OwnerObject, corev1.EventTypeNormal, "UpdateToTargetContext", "try to update ResourceContext for XSet")
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			return resourcecontexts.UpdateToTargetContext(u.xsetController, u.client, u.cacheExpectation, u.OwnerObject, ownedIDs)
		})
		return recordedRequeueAfter, err
	}
	return recordedRequeueAfter, nil
}

func (u *GenericTargetUpdater) FinishUpdateTarget(_ context.Context, targetInfo *targetUpdateInfo) error {
	if updated, err := opslifecycle.Finish(u.opsLifecycleMgr, u.client, u.updateLifecycleAdapter, targetInfo.Object); err != nil {
		return fmt.Errorf("failed to finish TargetOpsLifecycle for updating Target %s/%s: %s", targetInfo.GetNamespace(), targetInfo.GetName(), err.Error())
	} else if updated {
		// add an expectation for this target update, before next reconciling
		if err := u.cacheExpectation.ExpectUpdation(u.targetGVK, targetInfo.GetNamespace(), targetInfo.GetName(), targetInfo.GetResourceVersion()); err != nil {
			return err
		}
		u.recorder.Eventf(targetInfo.Object,
			corev1.EventTypeNormal,
			"UpdateReady", "target %s/%s update finished", targetInfo.GetNamespace(), targetInfo.GetName())
	}
	return nil
}

// Support users to define inPlaceOnlyTargetUpdater and register through RegisterInPlaceOnlyUpdater
var inPlaceOnlyTargetUpdater TargetUpdater

func RegisterInPlaceOnlyUpdater(targetUpdater TargetUpdater) {
	inPlaceOnlyTargetUpdater = targetUpdater
}

func (r *RealSyncControl) newTargetUpdater(xset api.XSetObject) TargetUpdater {
	spec := r.xsetController.GetXSetSpec(xset)
	var targetUpdater TargetUpdater
	switch spec.UpdateStrategy.UpdatePolicy {
	case api.XSetRecreateTargetUpdateStrategyType:
		targetUpdater = &recreateTargetUpdater{}
	case api.XSetInPlaceOnlyTargetUpdateStrategyType:
		if inPlaceOnlyTargetUpdater != nil {
			targetUpdater = inPlaceOnlyTargetUpdater
		} else {
			// In case of using native K8s, Target is only allowed to update with container image, so InPlaceOnly policy is
			// implemented with InPlaceIfPossible policy as default for compatibility.
			targetUpdater = &inPlaceIfPossibleUpdater{}
		}
	case api.XSetReplaceTargetUpdateStrategyType:
		targetUpdater = &replaceUpdateTargetUpdater{}
	default:
		targetUpdater = &inPlaceIfPossibleUpdater{}
	}
	targetUpdater.Setup(r.updateConfig, xset)
	return targetUpdater
}

type TargetStatus struct {
	ContainerStates map[string]*ContainerStatus `json:"containerStates,omitempty"`
}

type ContainerStatus struct {
	LatestImage string `json:"latestImage,omitempty"`
	LastImageID string `json:"lastImageID,omitempty"`
}

type inPlaceIfPossibleUpdater struct {
	GenericTargetUpdater
}

func (u *inPlaceIfPossibleUpdater) FulfillTargetUpdatedInfo(_ context.Context, revision *appsv1.ControllerRevision, targetUpdateInfo *targetUpdateInfo) error {
	// 1. build target from current and updated revision
	// TODO: use cache
	currentTarget, err := NewTargetFrom(u.xsetController, u.OwnerObject, targetUpdateInfo.CurrentRevision, targetUpdateInfo.ID)
	if err != nil {
		return fmt.Errorf("fail to build Target from current revision %s: %v", targetUpdateInfo.CurrentRevision.GetName(), err.Error())
	}

	// TODO: use cache

	UpdatedTarget, err := NewTargetFrom(u.xsetController, u.OwnerObject, targetUpdateInfo.UpdateRevision, targetUpdateInfo.ID)
	if err != nil {
		return fmt.Errorf("fail to build Target from updated revision %s: %v", targetUpdateInfo.UpdateRevision.GetName(), err.Error())
	}

	newUpdatedTarget := targetUpdateInfo.targetWrapper.Object.DeepCopyObject().(client.Object)
	if err = merge.ThreeWayMergeToTarget(currentTarget, UpdatedTarget, newUpdatedTarget, u.xsetController.EmptyXObject()); err != nil {
		return fmt.Errorf("fail to patch Target %s/%s: %v", targetUpdateInfo.GetNamespace(), targetUpdateInfo.GetName(), err.Error())
	}
	targetUpdateInfo.UpdatedTarget = newUpdatedTarget

	return nil
}

func (u *inPlaceIfPossibleUpdater) UpgradeTarget(ctx context.Context, targetInfo *targetUpdateInfo) error {
	if targetInfo.OnlyMetadataChanged || targetInfo.InPlaceUpdateSupport {
		// if target template changes only include metadata or support in-place update, just apply these changes to target directly
		if err := u.targetControl.UpdateTarget(ctx, targetInfo.UpdatedTarget); err != nil {
			return fmt.Errorf("fail to update Target %s/%s when updating by in-place: %s", targetInfo.GetNamespace(), targetInfo.GetName(), err.Error())
		} else {
			targetInfo.Object = targetInfo.UpdatedTarget
			u.recorder.Eventf(targetInfo.Object,
				corev1.EventTypeNormal,
				"UpdateTarget",
				"succeed to update Target %s/%s to from revision %s to revision %s by in-place",
				targetInfo.GetNamespace(), targetInfo.GetName(),
				targetInfo.CurrentRevision.GetName(),
				targetInfo.UpdateRevision.GetName())
		}
	} else {
		// if target has changes not in-place supported, recreate it
		return u.GenericTargetUpdater.RecreateTarget(ctx, targetInfo)
	}
	return nil
}

func (u *GenericTargetUpdater) RecreateTarget(ctx context.Context, targetInfo *targetUpdateInfo) error {
	if err := u.targetControl.DeleteTarget(ctx, targetInfo.Object); err != nil {
		return fmt.Errorf("fail to delete Target %s/%s when updating by recreate: %v", targetInfo.GetNamespace(), targetInfo.GetName(), err.Error())
	}

	id, err := GetInstanceID(targetInfo.Object)
	if err != nil {
		return fmt.Errorf("fail to get instance id for target %s/%s: %v", targetInfo.GetNamespace(), targetInfo.GetName(), err.Error())
	}

	newObj, err := NewTargetFrom(u.xsetController, u.OwnerObject, targetInfo.UpdateRevision, id)
	if err != nil {
		return fmt.Errorf("fail to build Target from updated revision %s: %v", targetInfo.UpdateRevision.GetName(), err.Error())
	}
	if _, err := u.targetControl.CreateTarget(ctx, newObj); err != nil {
		return fmt.Errorf("fail to create Target %s/%s when updating by recreate: %v", targetInfo.GetNamespace(), targetInfo.GetName(), err.Error())
	}

	u.recorder.Eventf(targetInfo.Object,
		corev1.EventTypeNormal,
		"UpdateTarget",
		"succeed to update Target %s/%s to from revision %s to revision %s by recreate",
		targetInfo.GetNamespace(),
		targetInfo.GetName(),
		targetInfo.CurrentRevision.GetName(),
		targetInfo.UpdateRevision.GetName())

	return nil
}

func (u *inPlaceIfPossibleUpdater) GetTargetUpdateFinishStatus(_ context.Context, targetUpdateInfo *targetUpdateInfo) (finished bool, msg string, err error) {
	if targetUpdateInfo.GetAnnotations() == nil {
		return false, "no annotations for last container status", nil
	}

	targetLastState := &TargetStatus{}
	if lastStateJson, exist := targetUpdateInfo.GetAnnotations()[LastTargetStatusAnnotationKey]; !exist {
		return false, "no target last state annotation", nil
	} else if err := json.Unmarshal([]byte(lastStateJson), targetLastState); err != nil {
		msg := fmt.Sprintf("malformat target last state annotation [%s]: %s", lastStateJson, err.Error())
		return false, msg, errors.New(msg)
	}

	if targetLastState.ContainerStates == nil {
		return true, "empty last container state recorded", nil
	}

	return true, "", nil
}

type recreateTargetUpdater struct {
	GenericTargetUpdater
}

func (u *recreateTargetUpdater) FulfillTargetUpdatedInfo(_ context.Context, _ *appsv1.ControllerRevision, _ *targetUpdateInfo) error {
	return nil
}

func (u *recreateTargetUpdater) UpgradeTarget(ctx context.Context, targetInfo *targetUpdateInfo) error {
	return u.GenericTargetUpdater.RecreateTarget(ctx, targetInfo)
}

func (u *recreateTargetUpdater) GetTargetUpdateFinishStatus(_ context.Context, targetInfo *targetUpdateInfo) (finished bool, msg string, err error) {
	// Recreate policy always treat Target as update not finished
	return targetInfo.IsUpdatedRevision, "", nil
}

type replaceUpdateTargetUpdater struct {
	GenericTargetUpdater
}

func (u *replaceUpdateTargetUpdater) Setup(config *UpdateConfig, xset api.XSetObject) {
	u.GenericTargetUpdater.Setup(config, xset)
}

func (u *replaceUpdateTargetUpdater) BeginUpdateTarget(ctx context.Context, syncContext *SyncContext, targetCh chan *targetUpdateInfo) (bool, error) {
	succCount, err := controllerutils.SlowStartBatch(len(targetCh), controllerutils.SlowStartInitialBatchSize, false, func(int, error) error {
		targetInfo := <-targetCh
		if targetInfo.ReplacePairNewTargetInfo != nil {
			replacePairNewTarget := targetInfo.ReplacePairNewTargetInfo.Object
			newTargetRevision, exist := replacePairNewTarget.GetLabels()[appsv1.ControllerRevisionHashLabelKey]
			if exist && newTargetRevision == targetInfo.UpdateRevision.GetName() {
				return nil
			}
			if _, exist := replacePairNewTarget.GetLabels()[TargetDeletionIndicationLabelKey]; exist {
				return nil
			}

			u.recorder.Eventf(targetInfo.Object,
				corev1.EventTypeNormal,
				"ReplaceUpdateTarget",
				"label to-delete on new pair target %s/%s because it is not updated revision, current revision: %s, updated revision: %s",
				replacePairNewTarget.GetNamespace(),
				replacePairNewTarget.GetName(),
				newTargetRevision,
				syncContext.UpdatedRevision.GetName())
			patch := client.RawPatch(types.MergePatchType, []byte(fmt.Sprintf(`{"metadata":{"labels":{%q:"%d"}}}`, TargetDeletionIndicationLabelKey, time.Now().UnixNano())))
			if patchErr := u.client.Patch(ctx, targetInfo.ReplacePairNewTargetInfo.Object, patch); patchErr != nil {
				err := fmt.Errorf("failed to delete replace pair new target %s/%s %s",
					targetInfo.ReplacePairNewTargetInfo.GetNamespace(), targetInfo.ReplacePairNewTargetInfo.GetName(), patchErr.Error())
				return err
			}
		}
		return nil
	})

	return succCount > 0, err
}

func (u *replaceUpdateTargetUpdater) FilterAllowOpsTargets(_ context.Context, candidates []*targetUpdateInfo, _ map[int]*appsv1alpha1.ContextDetail, _ *SyncContext, targetCh chan *targetUpdateInfo) (requeueAfter *time.Duration, err error) {
	activeTargetToUpdate := filterOutPlaceHolderUpdateInfos(candidates)
	for i, targetInfo := range activeTargetToUpdate {
		if targetInfo.IsUpdatedRevision {
			continue
		}

		targetCh <- activeTargetToUpdate[i]
	}
	return nil, err
}

func (u *replaceUpdateTargetUpdater) FulfillTargetUpdatedInfo(_ context.Context, _ *appsv1.ControllerRevision, _ *targetUpdateInfo) (err error) {
	return
}

func (u *replaceUpdateTargetUpdater) UpgradeTarget(ctx context.Context, targetInfo *targetUpdateInfo) error {
	// add replace labels and wait to replace when syncTargets
	_, replaceIndicate := targetInfo.Object.GetLabels()[TargetReplaceIndicationLabelKey]
	_, replaceByUpdate := targetInfo.Object.GetLabels()[TargetReplaceByReplaceUpdateLabelKey]
	if !replaceIndicate || !replaceByUpdate {
		// need replace update target, label target with replace-indicate and replace-update
		now := time.Now().UnixNano()
		patch := client.RawPatch(types.MergePatchType, []byte(fmt.Sprintf(`{"metadata":{"labels":{%q:"%v", %q: "%v"}}}`, TargetReplaceIndicationLabelKey, now, TargetReplaceByReplaceUpdateLabelKey, targetInfo.UpdateRevision.GetName())))
		if err := u.client.Patch(ctx, targetInfo.Object, patch); err != nil {
			return fmt.Errorf("fail to label origin target %s/%s with replace indicate label by replaceUpdate: %s", targetInfo.GetNamespace(), targetInfo.GetName(), err.Error())
		}
		u.recorder.Eventf(targetInfo.Object,
			corev1.EventTypeNormal,
			"UpdateTarget",
			"succeed to update Target %s/%s by label to-replace",
			targetInfo.GetNamespace(),
			targetInfo.GetName(),
		)
	}
	return nil
}

func (u *replaceUpdateTargetUpdater) GetTargetUpdateFinishStatus(_ context.Context, targetUpdateInfo *targetUpdateInfo) (finished bool, msg string, err error) {
	replaceNewTargetInfo := targetUpdateInfo.ReplacePairNewTargetInfo
	if replaceNewTargetInfo == nil {
		return
	}

	return u.isTargetUpdatedServiceAvailable(replaceNewTargetInfo)
}

func (u *replaceUpdateTargetUpdater) FinishUpdateTarget(ctx context.Context, targetInfo *targetUpdateInfo) error {
	ReplacePairNewTargetInfo := targetInfo.ReplacePairNewTargetInfo
	if ReplacePairNewTargetInfo != nil {
		if _, exist := targetInfo.GetLabels()[TargetDeletionIndicationLabelKey]; !exist {
			patch := client.RawPatch(types.MergePatchType, []byte(fmt.Sprintf(`{"metadata":{"labels":{%q:"%d"}}}`, TargetDeletionIndicationLabelKey, time.Now().UnixNano())))
			if err := u.targetControl.PatchTarget(ctx, targetInfo.Object, patch); err != nil {
				return fmt.Errorf("failed to delete replace pair origin target %s/%s %s", targetInfo.GetNamespace(), targetInfo.ReplacePairNewTargetInfo.GetName(), err.Error())
			}
		}
	}
	return nil
}

func (u *GenericTargetUpdater) isTargetUpdatedServiceAvailable(targetInfo *targetUpdateInfo) (finished bool, msg string, err error) {
	if targetInfo.GetLabels() == nil {
		return false, "no labels on target", nil
	}
	if targetInfo.IsInReplacing && targetInfo.ReplacePairNewTargetInfo != nil {
		return false, "replace origin target", nil
	}

	if serviceAvailable := opslifecycle.IsServiceAvailable(u.opsLifecycleMgr, targetInfo.Object); serviceAvailable {
		return true, "", nil
	}

	return false, "target not service available", nil
}
