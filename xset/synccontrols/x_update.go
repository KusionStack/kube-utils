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
	"sigs.k8s.io/controller-runtime/pkg/client"

	clientutil "kusionstack.io/kube-utils/client"
	"kusionstack.io/kube-utils/controller/expectations"
	controllerutils "kusionstack.io/kube-utils/controller/utils"
	"kusionstack.io/kube-utils/xset/api"
	"kusionstack.io/kube-utils/xset/opslifecycle"
	"kusionstack.io/kube-utils/xset/resourcecontexts"
	"kusionstack.io/kube-utils/xset/subresources"
	"kusionstack.io/kube-utils/xset/xcontrol"
)

const UnknownRevision = "__unknownRevision__"

func (r *RealSyncControl) attachTargetUpdateInfo(ctx context.Context, xsetObject api.XSetObject, syncContext *SyncContext) ([]*TargetUpdateInfo, error) {
	activeTargets := FilterOutActiveTargetWrappers(syncContext.TargetWrappers)
	targetUpdateInfoList := make([]*TargetUpdateInfo, len(activeTargets))

	for i, target := range activeTargets {
		updateInfo := &TargetUpdateInfo{
			TargetWrapper: syncContext.TargetWrappers[i],
		}

		// check for decoration changed
		if decoration, ok := r.xsetController.(api.DecorationAdapter); ok {
			var err error
			updateInfo.DecorationChanged, err = decoration.IsDecorationChanged(ctx, updateInfo.TargetWrapper.Object)
			if err != nil {
				return nil, err
			}
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

		var err error
		spec := r.xsetController.GetXSetSpec(xsetObject)
		// decide whether the TargetOpsLifecycle is during ops or not
		updateInfo.RequeueForOperationDelay, updateInfo.IsAllowUpdateOps = opslifecycle.AllowOps(r.updateConfig.XsetLabelAnnoMgr, r.updateLifecycleAdapter, ptr.Deref(spec.UpdateStrategy.OperationDelaySeconds, 0), target)
		// check subresource pvc template changed
		if _, enabled := subresources.GetSubresourcePvcAdapter(r.xsetController); enabled {
			updateInfo.PvcTmpHashChanged, err = r.pvcControl.IsTargetPvcTmpChanged(xsetObject, target.Object, syncContext.ExistingPvcs)
			if err != nil {
				return nil, err
			}
		}
		targetUpdateInfoList[i] = updateInfo
	}

	// attach replace info
	targetUpdateInfoMap := make(map[string]*TargetUpdateInfo)
	for _, targetUpdateInfo := range targetUpdateInfoList {
		targetUpdateInfoMap[targetUpdateInfo.GetName()] = targetUpdateInfo
	}
	// originTarget's isAllowUpdateOps depends on these 2 cases:
	// (1) target is during replacing but not during replaceUpdate, keep it legacy value
	// (2) target is during replaceUpdate, set to "true" if newTarget is service available
	for originTargetName, replacePairNewTarget := range syncContext.replacingMap {
		originTargetInfo := targetUpdateInfoMap[originTargetName]
		_, replaceIndicated := r.xsetLabelAnnoMgr.Get(originTargetInfo.GetLabels(), api.XReplaceIndicationLabelKey)
		_, replaceByReplaceUpdate := r.xsetLabelAnnoMgr.Get(originTargetInfo.GetLabels(), api.XReplaceByReplaceUpdateLabelKey)
		isReplaceUpdating := replaceIndicated && replaceByReplaceUpdate

		originTargetInfo.IsInReplace = replaceIndicated
		originTargetInfo.IsInReplaceUpdate = isReplaceUpdating

		if replacePairNewTarget != nil {
			// origin target is allowed to ops if new pod is serviceAvailable
			newTargetSa := r.xsetController.CheckAvailable(replacePairNewTarget.Object)
			originTargetInfo.IsAllowUpdateOps = originTargetInfo.IsAllowUpdateOps || newTargetSa
			// attach replace new target updateInfo
			ReplacePairNewTargetInfo := targetUpdateInfoMap[replacePairNewTarget.GetName()]
			ReplacePairNewTargetInfo.IsInReplace = true
			// in case of to-replace label is removed from origin target, new target is still in replaceUpdate
			ReplacePairNewTargetInfo.IsInReplaceUpdate = replaceByReplaceUpdate

			ReplacePairNewTargetInfo.ReplacePairOriginTargetName = originTargetName
			originTargetInfo.ReplacePairNewTargetInfo = ReplacePairNewTargetInfo
		}
	}

	// join PlaceHolder targets in updating
	for _, target := range syncContext.TargetWrappers {
		if !target.PlaceHolder {
			continue
		}
		updateInfo := &TargetUpdateInfo{
			TargetWrapper:  target,
			UpdateRevision: syncContext.UpdatedRevision,
		}
		if revision, exist := r.resourceContextControl.Get(target.ContextDetail, api.EnumRevisionContextDataKey); exist &&
			revision == syncContext.UpdatedRevision.GetName() {
			updateInfo.IsUpdatedRevision = true
		}
		targetUpdateInfoList = append(targetUpdateInfoList, updateInfo)
	}

	return targetUpdateInfoList, nil
}

func filterOutPlaceHolderUpdateInfos(targets []*TargetUpdateInfo) []*TargetUpdateInfo {
	var filteredTargetUpdateInfos []*TargetUpdateInfo
	for _, target := range targets {
		if target.PlaceHolder {
			continue
		}
		filteredTargetUpdateInfos = append(filteredTargetUpdateInfos, target)
	}
	return filteredTargetUpdateInfos
}

func (r *RealSyncControl) decideTargetToUpdate(xsetController api.XSetController, xset api.XSetObject, targetInfos []*TargetUpdateInfo) []*TargetUpdateInfo {
	spec := xsetController.GetXSetSpec(xset)
	filteredPodInfos := r.getTargetsUpdateTargets(targetInfos)

	if spec.UpdateStrategy.RollingUpdate != nil && spec.UpdateStrategy.RollingUpdate.ByLabel != nil {
		activeTargetInfos := filterOutPlaceHolderUpdateInfos(filteredPodInfos)
		return r.decideTargetToUpdateByLabel(activeTargetInfos)
	}

	return r.decideTargetToUpdateByPartition(xsetController, xset, filteredPodInfos)
}

func (r *RealSyncControl) decideTargetToUpdateByLabel(targetInfos []*TargetUpdateInfo) (targetToUpdate []*TargetUpdateInfo) {
	for i := range targetInfos {
		if _, exist := r.xsetLabelAnnoMgr.Get(targetInfos[i].GetLabels(), api.XSetUpdateIndicationLabelKey); exist {
			targetToUpdate = append(targetToUpdate, targetInfos[i])
			continue
		}

		// separate decoration and xset update progress
		if targetInfos[i].DecorationChanged {
			if targetInfos[i].IsInReplace {
				continue
			}
			targetInfos[i].IsUpdatedRevision = true
			targetInfos[i].UpdateRevision = targetInfos[i].CurrentRevision
			targetToUpdate = append(targetToUpdate, targetInfos[i])
		}
	}
	return targetToUpdate
}

func (r *RealSyncControl) decideTargetToUpdateByPartition(xsetController api.XSetController, xset api.XSetObject, filteredTargetInfos []*TargetUpdateInfo) []*TargetUpdateInfo {
	spec := xsetController.GetXSetSpec(xset)
	replicas := ptr.Deref(spec.Replicas, 0)
	currentTargetCount := int32(len(filteredTargetInfos))
	partition := int32(0)

	if spec.UpdateStrategy.RollingUpdate != nil && spec.UpdateStrategy.RollingUpdate.ByPartition != nil {
		partition = ptr.Deref(spec.UpdateStrategy.RollingUpdate.ByPartition.Partition, 0)
	}

	// update all or not update any replicas
	if partition == 0 {
		return filteredTargetInfos
	}
	if partition >= replicas {
		return nil
	}

	// partial update replicas
	ordered := newOrderedTargetUpdateInfos(filteredTargetInfos, xsetController.CheckReadyTime)
	sort.Sort(ordered)
	targetToUpdate := ordered.targets[:replicas-partition]
	// separate decoration and xset update progress
	for i := replicas - partition; i < int32Min(replicas, currentTargetCount); i++ {
		if ordered.targets[i].DecorationChanged {
			ordered.targets[i].IsUpdatedRevision = true
			ordered.targets[i].UpdateRevision = ordered.targets[i].CurrentRevision
			targetToUpdate = append(targetToUpdate, ordered.targets[i])
		}
	}
	return targetToUpdate
}

// when sort targets to choose update, only sort (1) replace origin targets, (2) non-exclude targets
func (r *RealSyncControl) getTargetsUpdateTargets(targetInfos []*TargetUpdateInfo) (filteredTargetInfos []*TargetUpdateInfo) {
	for _, targetInfo := range targetInfos {
		if targetInfo.ReplacePairOriginTargetName != "" {
			continue
		}
		if targetInfo.PlaceHolder {
			if _, isReplaceNewTarget := r.resourceContextControl.Get(targetInfo.ContextDetail, api.EnumReplaceOriginTargetIDContextDataKey); isReplaceNewTarget {
				continue
			}
		}
		filteredTargetInfos = append(filteredTargetInfos, targetInfo)
	}
	return filteredTargetInfos
}

func newOrderedTargetUpdateInfos(
	targetInfos []*TargetUpdateInfo,
	checkReadyFunc func(object client.Object) (bool, *metav1.Time),
) *orderByDefault {
	return &orderByDefault{
		targets:        targetInfos,
		checkReadyFunc: checkReadyFunc,
	}
}

type orderByDefault struct {
	targets        []*TargetUpdateInfo
	checkReadyFunc func(object client.Object) (bool, *metav1.Time)
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

	if l.IsDuringUpdateOps != r.IsDuringUpdateOps {
		return l.IsDuringUpdateOps
	}

	if l.IsInReplaceUpdate != r.IsInReplaceUpdate {
		return l.IsInReplaceUpdate
	}

	if l.PlaceHolder != r.PlaceHolder {
		return r.PlaceHolder
	}

	if l.PlaceHolder && r.PlaceHolder {
		return true
	}

	lReady, _ := o.checkReadyFunc(l.Object)
	rReady, _ := o.checkReadyFunc(r.Object)
	if lReady != rReady {
		return lReady
	}

	if l.DecorationChanged != r.DecorationChanged {
		return l.DecorationChanged
	}

	if l.OpsPriority != nil && r.OpsPriority != nil {
		if l.OpsPriority.PriorityClass != r.OpsPriority.PriorityClass {
			return l.OpsPriority.PriorityClass < r.OpsPriority.PriorityClass
		}
		if l.OpsPriority.DeletionCost != r.OpsPriority.DeletionCost {
			return l.OpsPriority.DeletionCost < r.OpsPriority.DeletionCost
		}
	}

	return CompareTarget(l.Object, r.Object, o.checkReadyFunc)
}

type UpdateConfig struct {
	XsetController         api.XSetController
	XsetLabelAnnoMgr       api.XSetLabelAnnotationManager
	Client                 client.Client
	TargetControl          xcontrol.TargetControl
	ResourceContextControl resourcecontexts.ResourceContextControl
	Recorder               record.EventRecorder

	scaleInLifecycleAdapter api.LifecycleAdapter
	updateLifecycleAdapter  api.LifecycleAdapter

	CacheExpectations expectations.CacheExpectationsInterface
	TargetGVK         schema.GroupVersionKind
}

type TargetUpdater interface {
	Setup(config *UpdateConfig, xset api.XSetObject)
	FulfillTargetUpdatedInfo(ctx context.Context, revision *appsv1.ControllerRevision, targetUpdateInfo *TargetUpdateInfo) error
	BeginUpdateTarget(ctx context.Context, syncContext *SyncContext, targetCh chan *TargetUpdateInfo) (bool, error)
	FilterAllowOpsTargets(ctx context.Context, targetToUpdate []*TargetUpdateInfo, ownedIDs map[int]*api.ContextDetail, syncContext *SyncContext, targetCh chan *TargetUpdateInfo) (*time.Duration, error)
	UpgradeTarget(ctx context.Context, targetInfo *TargetUpdateInfo) error
	GetTargetUpdateFinishStatus(ctx context.Context, targetUpdateInfo *TargetUpdateInfo) (bool, string, error)
	FinishUpdateTarget(ctx context.Context, targetInfo *TargetUpdateInfo, finishByCancelUpdate bool) error
}

type GenericTargetUpdater struct {
	OwnerObject api.XSetObject

	*UpdateConfig
}

func (u *GenericTargetUpdater) Setup(config *UpdateConfig, xset api.XSetObject) {
	u.UpdateConfig = config
	u.OwnerObject = xset
}

func (u *GenericTargetUpdater) BeginUpdateTarget(_ context.Context, syncContext *SyncContext, targetCh chan *TargetUpdateInfo) (bool, error) {
	succCount, err := controllerutils.SlowStartBatch(len(targetCh), controllerutils.SlowStartInitialBatchSize, false, func(int, error) error {
		targetInfo := <-targetCh
		u.Recorder.Eventf(targetInfo.Object, corev1.EventTypeNormal, "TargetUpdateLifecycle", "try to begin TargetOpsLifecycle for updating Target of XSet")

		if updated, err := opslifecycle.BeginWithCleaningOld(u.XsetLabelAnnoMgr, u.Client, u.updateLifecycleAdapter, targetInfo.Object, func(obj client.Object) (bool, error) {
			if !targetInfo.OnlyMetadataChanged && !targetInfo.InPlaceUpdateSupport {
				return opslifecycle.WhenBeginDelete(u.XsetLabelAnnoMgr, obj)
			}
			return false, nil
		}); err != nil {
			return fmt.Errorf("fail to begin TargetOpsLifecycle for updating Target %s/%s: %s", targetInfo.GetNamespace(), targetInfo.GetName(), err.Error())
		} else if updated {
			// add an expectation for this target update, before next reconciling
			if err := u.CacheExpectations.ExpectUpdation(clientutil.ObjectKeyString(u.OwnerObject), u.TargetGVK, targetInfo.GetNamespace(), targetInfo.GetName(), targetInfo.GetResourceVersion()); err != nil {
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

func (u *GenericTargetUpdater) FilterAllowOpsTargets(ctx context.Context, candidates []*TargetUpdateInfo, ownedIDs map[int]*api.ContextDetail, _ *SyncContext, targetCh chan *TargetUpdateInfo) (*time.Duration, error) {
	var recordedRequeueAfter *time.Duration
	needUpdateContext := false
	for i := range candidates {
		targetInfo := candidates[i]

		if !targetInfo.PlaceHolder {
			if !targetInfo.IsAllowUpdateOps {
				continue
			}
			if targetInfo.RequeueForOperationDelay != nil {
				u.Recorder.Eventf(targetInfo, corev1.EventTypeNormal, "TargetUpdateLifecycle", "delay Target update for %f seconds", targetInfo.RequeueForOperationDelay.Seconds())
				if recordedRequeueAfter == nil || *targetInfo.RequeueForOperationDelay < *recordedRequeueAfter {
					recordedRequeueAfter = targetInfo.RequeueForOperationDelay
				}
				continue
			}
		}

		targetInfo.IsAllowUpdateOps = true

		if targetInfo.IsUpdatedRevision && !targetInfo.PvcTmpHashChanged && !targetInfo.DecorationChanged {
			continue
		}

		if _, exist := ownedIDs[targetInfo.ID]; !exist {
			u.Recorder.Eventf(u.OwnerObject, corev1.EventTypeWarning, "TargetBeforeUpdate", "target %s/%s is not allowed to update because cannot find context id %s in resourceContext", targetInfo.GetNamespace(), targetInfo.GetName(), targetInfo.GetLabels()[u.XsetLabelAnnoMgr.Value(api.XInstanceIdLabelKey)])
			continue
		}

		if !u.ResourceContextControl.Contains(ownedIDs[targetInfo.ID], api.EnumRevisionContextDataKey, targetInfo.UpdateRevision.GetName()) {
			needUpdateContext = true
			u.ResourceContextControl.Put(ownedIDs[targetInfo.ID], api.EnumRevisionContextDataKey, targetInfo.UpdateRevision.GetName())
		}

		spec := u.XsetController.GetXSetSpec(u.OwnerObject)

		// mark targetContext "TargetRecreateUpgrade" if upgrade by recreate
		isRecreateUpdatePolicy := spec.UpdateStrategy.UpdatePolicy == api.XSetRecreateTargetUpdateStrategyType
		if (!targetInfo.OnlyMetadataChanged && !targetInfo.InPlaceUpdateSupport) || isRecreateUpdatePolicy {
			u.ResourceContextControl.Put(ownedIDs[targetInfo.ID], api.EnumRecreateUpdateContextDataKey, "true")
		}

		// add decoration revision to target context
		if decorationAdapter, ok := u.XsetController.(api.DecorationAdapter); ok && targetInfo.DecorationChanged {
			decorationRevision, err := decorationAdapter.GetDecorationRevisionFromTarget(ctx, targetInfo.Object)
			if err != nil {
				return recordedRequeueAfter, err
			}
			if val, ok := u.ResourceContextControl.Get(ownedIDs[targetInfo.ID], api.EnumTargetDecorationRevisionKey); !ok || val != decorationRevision {
				u.ResourceContextControl.Put(ownedIDs[targetInfo.ID], api.EnumTargetDecorationRevisionKey, decorationRevision)
			}
		}

		if targetInfo.PlaceHolder {
			continue
		}

		// if Target has not been updated, update it.
		targetCh <- candidates[i]
	}
	// mark Target to use updated revision before updating it.
	if needUpdateContext {
		u.Recorder.Eventf(u.OwnerObject, corev1.EventTypeNormal, "UpdateToTargetContext", "try to update ResourceContext for XSet")
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			return u.ResourceContextControl.UpdateToTargetContext(ctx, u.OwnerObject, ownedIDs)
		})
		return recordedRequeueAfter, err
	}
	return recordedRequeueAfter, nil
}

func (u *GenericTargetUpdater) FinishUpdateTarget(_ context.Context, targetInfo *TargetUpdateInfo, finishByCancelUpdate bool) error {
	if finishByCancelUpdate {
		// cancel update lifecycle
		return opslifecycle.CancelOpsLifecycle(u.XsetLabelAnnoMgr, u.Client, u.updateLifecycleAdapter, targetInfo.Object)
	}

	// target is ops finished, finish the lifecycle gracefully
	if updated, err := opslifecycle.Finish(u.XsetLabelAnnoMgr, u.Client, u.updateLifecycleAdapter, targetInfo.Object); err != nil {
		return fmt.Errorf("failed to finish TargetOpsLifecycle for updating Target %s/%s: %s", targetInfo.GetNamespace(), targetInfo.GetName(), err.Error())
	} else if updated {
		// add an expectation for this target update, before next reconciling
		if err := u.CacheExpectations.ExpectUpdation(clientutil.ObjectKeyString(u.OwnerObject), u.TargetGVK, targetInfo.GetNamespace(), targetInfo.GetName(), targetInfo.GetResourceVersion()); err != nil {
			return err
		}
		u.Recorder.Eventf(targetInfo.Object,
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

// Support users to define inPlaceIfPossibleUpdater and register through RegistryInPlaceIfPossibleUpdater
var inPlaceIfPossibleUpdater TargetUpdater

func RegisterInPlaceIfPossibleUpdater(targetUpdater TargetUpdater) {
	inPlaceIfPossibleUpdater = targetUpdater
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
		} else if inPlaceIfPossibleUpdater != nil {
			// In case of using native K8s, Target is only allowed to update with container image, so InPlaceOnly policy is
			// implemented with InPlaceIfPossible policy as default for compatibility.
			targetUpdater = inPlaceIfPossibleUpdater
		} else {
			// if none of InplaceOnly and InplaceIfPossible updater is registered, use default Recreate updater
			targetUpdater = &recreateTargetUpdater{}
		}
	case api.XSetReplaceTargetUpdateStrategyType:
		targetUpdater = &replaceUpdateTargetUpdater{}
	default:
		if inPlaceIfPossibleUpdater != nil {
			targetUpdater = inPlaceIfPossibleUpdater
		} else {
			targetUpdater = &recreateTargetUpdater{}
		}
	}
	targetUpdater.Setup(r.updateConfig, xset)
	return targetUpdater
}

func (u *GenericTargetUpdater) RecreateTarget(ctx context.Context, targetInfo *TargetUpdateInfo) error {
	if err := u.TargetControl.DeleteTarget(ctx, targetInfo.Object); err != nil {
		return fmt.Errorf("fail to delete Target %s/%s when updating by recreate: %v", targetInfo.GetNamespace(), targetInfo.GetName(), err.Error())
	}

	u.Recorder.Eventf(targetInfo.Object,
		corev1.EventTypeNormal,
		"UpdateTarget",
		"succeed to update Target %s/%s to from revision %s to revision %s by recreate",
		targetInfo.GetNamespace(),
		targetInfo.GetName(),
		targetInfo.CurrentRevision.GetName(),
		targetInfo.UpdateRevision.GetName())

	return nil
}

type recreateTargetUpdater struct {
	GenericTargetUpdater
}

func (u *recreateTargetUpdater) FulfillTargetUpdatedInfo(_ context.Context, _ *appsv1.ControllerRevision, _ *TargetUpdateInfo) error {
	return nil
}

func (u *recreateTargetUpdater) UpgradeTarget(ctx context.Context, targetInfo *TargetUpdateInfo) error {
	return u.GenericTargetUpdater.RecreateTarget(ctx, targetInfo)
}

func (u *recreateTargetUpdater) GetTargetUpdateFinishStatus(_ context.Context, targetInfo *TargetUpdateInfo) (finished bool, msg string, err error) {
	// Recreate policy always treat Target as update not finished
	return targetInfo.IsUpdatedRevision && !targetInfo.DecorationChanged, "", nil
}

type replaceUpdateTargetUpdater struct {
	GenericTargetUpdater
}

func (u *replaceUpdateTargetUpdater) Setup(config *UpdateConfig, xset api.XSetObject) {
	u.GenericTargetUpdater.Setup(config, xset)
}

func (u *replaceUpdateTargetUpdater) BeginUpdateTarget(ctx context.Context, syncContext *SyncContext, targetCh chan *TargetUpdateInfo) (bool, error) {
	succCount, err := controllerutils.SlowStartBatch(len(targetCh), controllerutils.SlowStartInitialBatchSize, false, func(int, error) error {
		targetInfo := <-targetCh
		if targetInfo.ReplacePairNewTargetInfo != nil {
			replacePairNewTarget := targetInfo.ReplacePairNewTargetInfo.Object
			newTargetRevision, exist := replacePairNewTarget.GetLabels()[appsv1.ControllerRevisionHashLabelKey]
			if exist && newTargetRevision == targetInfo.UpdateRevision.GetName() {
				return nil
			}
			if _, exist := u.XsetLabelAnnoMgr.Get(replacePairNewTarget.GetLabels(), api.XDeletionIndicationLabelKey); exist {
				return nil
			}

			u.Recorder.Eventf(targetInfo.Object,
				corev1.EventTypeNormal,
				"ReplaceUpdateTarget",
				"label to-delete on new pair target %s/%s because it is not updated revision, current revision: %s, updated revision: %s",
				replacePairNewTarget.GetNamespace(),
				replacePairNewTarget.GetName(),
				newTargetRevision,
				syncContext.UpdatedRevision.GetName())
			patch := client.RawPatch(types.MergePatchType, []byte(fmt.Sprintf(`{"metadata":{"labels":{%q:"%d"}}}`, u.XsetLabelAnnoMgr.Value(api.XDeletionIndicationLabelKey), time.Now().UnixNano())))
			if patchErr := u.Client.Patch(ctx, targetInfo.ReplacePairNewTargetInfo.Object, patch); patchErr != nil {
				err := fmt.Errorf("failed to delete replace pair new target %s/%s %s",
					targetInfo.ReplacePairNewTargetInfo.GetNamespace(), targetInfo.ReplacePairNewTargetInfo.GetName(), patchErr.Error())
				return err
			}
		}
		return nil
	})

	return succCount > 0, err
}

func (u *replaceUpdateTargetUpdater) FilterAllowOpsTargets(_ context.Context, candidates []*TargetUpdateInfo, _ map[int]*api.ContextDetail, _ *SyncContext, targetCh chan *TargetUpdateInfo) (requeueAfter *time.Duration, err error) {
	activeTargetToUpdate := filterOutPlaceHolderUpdateInfos(candidates)
	for i, targetInfo := range activeTargetToUpdate {
		if targetInfo.IsUpdatedRevision && !targetInfo.PvcTmpHashChanged && !targetInfo.DecorationChanged {
			continue
		}

		targetCh <- activeTargetToUpdate[i]
	}
	return nil, err
}

func (u *replaceUpdateTargetUpdater) FulfillTargetUpdatedInfo(_ context.Context, _ *appsv1.ControllerRevision, _ *TargetUpdateInfo) (err error) {
	return
}

func (u *replaceUpdateTargetUpdater) UpgradeTarget(ctx context.Context, targetInfo *TargetUpdateInfo) error {
	return updateReplaceOriginTarget(ctx, u.Client, u.Recorder, u.XsetLabelAnnoMgr, targetInfo, targetInfo.ReplacePairNewTargetInfo)
}

func (u *replaceUpdateTargetUpdater) GetTargetUpdateFinishStatus(_ context.Context, targetUpdateInfo *TargetUpdateInfo) (finished bool, msg string, err error) {
	replaceNewTargetInfo := targetUpdateInfo.ReplacePairNewTargetInfo
	if replaceNewTargetInfo == nil {
		return
	}

	return u.isTargetUpdatedServiceAvailable(replaceNewTargetInfo)
}

func (u *replaceUpdateTargetUpdater) FinishUpdateTarget(ctx context.Context, targetInfo *TargetUpdateInfo, finishByCancelUpdate bool) error {
	if finishByCancelUpdate {
		// cancel replace update by removing to-replace and replace-by-update label from origin target
		if targetInfo.IsInReplace {
			patch := client.RawPatch(types.MergePatchType, fmt.Appendf(nil, `{"metadata":{"labels":{"%s":null, "%s":null}}}`, u.XsetLabelAnnoMgr.Value(api.XReplaceIndicationLabelKey), u.XsetLabelAnnoMgr.Value(api.XReplaceByReplaceUpdateLabelKey)))
			if err := u.TargetControl.PatchTarget(ctx, targetInfo.Object, patch); err != nil {
				return fmt.Errorf("failed to patch replace pair target %s/%s %w when cancel replace update", targetInfo.GetNamespace(), targetInfo.GetName(), err)
			}
		}
		return nil
	}

	ReplacePairNewTargetInfo := targetInfo.ReplacePairNewTargetInfo
	if ReplacePairNewTargetInfo != nil {
		if _, exist := u.XsetLabelAnnoMgr.Get(targetInfo.GetLabels(), api.XDeletionIndicationLabelKey); !exist {
			patch := client.RawPatch(types.MergePatchType, []byte(fmt.Sprintf(`{"metadata":{"labels":{%q:"%d"}}}`, u.XsetLabelAnnoMgr.Value(api.XDeletionIndicationLabelKey), time.Now().UnixNano())))
			if err := u.TargetControl.PatchTarget(ctx, targetInfo.Object, patch); err != nil {
				return fmt.Errorf("failed to delete replace pair origin target %s/%s %s", targetInfo.GetNamespace(), targetInfo.ReplacePairNewTargetInfo.GetName(), err.Error())
			}
		}
	}
	return nil
}

func (u *GenericTargetUpdater) isTargetUpdatedServiceAvailable(targetInfo *TargetUpdateInfo) (finished bool, msg string, err error) {
	// check decoration changed
	if targetInfo.DecorationChanged {
		return false, "decoration changed", nil
	}

	if targetInfo.GetLabels() == nil {
		return false, "no labels on target", nil
	}
	if targetInfo.IsInReplace && targetInfo.ReplacePairNewTargetInfo != nil {
		return false, "replace origin target", nil
	}

	if u.XsetController.CheckAvailable(targetInfo.Object) {
		return true, "", nil
	}

	return false, "target not service available", nil
}

func int32Min(l, r int32) int32 {
	if l < r {
		return l
	}

	return r
}
