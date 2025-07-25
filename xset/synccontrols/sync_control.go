/*
Copyright 2023-2025 The KusionStack Authors.

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
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/retry"
	appsv1alpha1 "kusionstack.io/kube-api/apps/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"kusionstack.io/kube-utils/controller/expectations"
	"kusionstack.io/kube-utils/controller/mixin"
	controllerutils "kusionstack.io/kube-utils/controller/utils"
	"kusionstack.io/kube-utils/xset/api"
	"kusionstack.io/kube-utils/xset/opslifecycle"
	"kusionstack.io/kube-utils/xset/resourcecontexts"
	"kusionstack.io/kube-utils/xset/xcontrol"
)

type SyncControl interface {
	SyncTargets(ctx context.Context, instance api.XSetObject, syncContext *SyncContext) (bool, error)

	Replace(ctx context.Context, instance api.XSetObject, syncContext *SyncContext) error

	Scale(ctx context.Context, instance api.XSetObject, syncContext *SyncContext) (bool, *time.Duration, error)

	Update(ctx context.Context, instance api.XSetObject, syncContext *SyncContext) (bool, *time.Duration, error)

	CalculateStatus(ctx context.Context, instance api.XSetObject, syncContext *SyncContext) *api.XSetStatus
}

func NewRealSyncControl(reconcileMixIn *mixin.ReconcilerMixin,
	xsetController api.XSetController,
	xControl xcontrol.TargetControl,
	cacheExpectation *expectations.CacheExpectation,
) SyncControl {
	lifeCycleLabelManager := xsetController.GetLifeCycleLabelManager()
	if lifeCycleLabelManager == nil {
		lifeCycleLabelManager = opslifecycle.NewLabelManager(nil)
	}
	scaleInOpsLifecycleAdapter := xsetController.GetScaleInOpsLifecycleAdapter()
	if scaleInOpsLifecycleAdapter == nil {
		scaleInOpsLifecycleAdapter = &opslifecycle.DefaultScaleInLifecycleAdapter{LabelManager: lifeCycleLabelManager}
	}
	updateLifecycleAdapter := xsetController.GetUpdateOpsLifecycleAdapter()
	if updateLifecycleAdapter == nil {
		updateLifecycleAdapter = &opslifecycle.DefaultUpdateLifecycleAdapter{LabelManager: lifeCycleLabelManager}
	}

	xMeta := xsetController.XMeta()
	targetGVK := xMeta.GroupVersionKind()
	xsetMeta := xsetController.XSetMeta()
	xsetGVK := xsetMeta.GroupVersionKind()

	updateConfig := &UpdateConfig{
		xsetController: xsetController,
		client:         reconcileMixIn.Client,
		targetControl:  xControl,
		recorder:       reconcileMixIn.Recorder,

		opsLifecycleMgr:         lifeCycleLabelManager,
		scaleInLifecycleAdapter: scaleInOpsLifecycleAdapter,
		updateLifecycleAdapter:  updateLifecycleAdapter,
		cacheExpectation:        cacheExpectation,
		targetGVK:               targetGVK,
	}
	return &RealSyncControl{
		ReconcilerMixin: *reconcileMixIn,
		xsetController:  xsetController,
		xControl:        xControl,

		updateConfig:     updateConfig,
		cacheExpectation: cacheExpectation,
		xsetGVK:          xsetGVK,
		targetGVK:        targetGVK,

		scaleInLifecycleAdapter: scaleInOpsLifecycleAdapter,
		updateLifecycleAdapter:  updateLifecycleAdapter,
	}
}

var _ SyncControl = &RealSyncControl{}

type RealSyncControl struct {
	mixin.ReconcilerMixin
	xControl       xcontrol.TargetControl
	xsetController api.XSetController

	updateConfig            *UpdateConfig
	scaleInLifecycleAdapter api.LifecycleAdapter
	updateLifecycleAdapter  api.LifecycleAdapter

	cacheExpectation *expectations.CacheExpectation
	xsetGVK          schema.GroupVersionKind
	targetGVK        schema.GroupVersionKind
}

// SyncTargets is used to parse targetWrappers and reclaim Target instance ID
func (r *RealSyncControl) SyncTargets(ctx context.Context, instance api.XSetObject, syncContext *SyncContext) (
	bool, error,
) {
	xspec := r.xsetController.GetXSetSpec(instance)
	if xspec == nil {
		return false, fmt.Errorf("fail to get XSetSpec")
	}

	var err error
	syncContext.FilteredTarget, err = r.xControl.GetFilteredTargets(ctx, xspec.Selector, instance)
	if err != nil {
		return false, fmt.Errorf("fail to get filtered Targets: %w", err)
	}

	toExcludeTargetNames, toIncludeTargetNames, err := r.dealIncludeExcludeTargets(ctx, instance, syncContext.FilteredTarget)
	if err != nil {
		return false, fmt.Errorf("fail to deal with include exclude targets: %s", err.Error())
	}

	// get owned IDs
	var ownedIDs map[int]*appsv1alpha1.ContextDetail
	if err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		ownedIDs, err = resourcecontexts.AllocateID(r.xsetController, r.Client, r.cacheExpectation, instance, syncContext.UpdatedRevision.GetName(), int(RealValue(xspec.Replicas)))
		syncContext.OwnedIds = ownedIDs
		return err
	}); err != nil {
		return false, fmt.Errorf("fail to allocate %d IDs using context when sync Targets: %w", xspec.Replicas, err)
	}

	// stateless case
	var targetWrappers []targetWrapper
	syncContext.CurrentIDs = sets.Int{}
	idToReclaim := sets.Int{}
	toDeleteTargetNames := sets.NewString(xspec.ScaleStrategy.TargetToDelete...)

	for i := range syncContext.FilteredTarget {
		target := syncContext.FilteredTarget[i]
		xName := target.GetName()
		id, _ := GetInstanceID(target)
		toDelete := toDeleteTargetNames.Has(xName)
		toExclude := toExcludeTargetNames.Has(xName)

		// priority: toDelete > toReplace > toExclude
		if toDelete {
			toDeleteTargetNames.Delete(xName)
		}
		if toExclude {
			if targetDuringReplace(target) || toDelete {
				// skip exclude until replace and toDelete done
				toExcludeTargetNames.Delete(xName)
			} else {
				// exclude target and delete its targetContext
				idToReclaim.Insert(id)
			}
		}

		if target.GetDeletionTimestamp() != nil {
			// 1. Reclaim ID from Target which is scaling in and terminating.
			if contextDetail, exist := ownedIDs[id]; exist && contextDetail.Contains(resourcecontexts.ScaleInContextDataKey, "true") {
				idToReclaim.Insert(id)
			}

			_, replaceIndicate := target.GetLabels()[TargetReplaceIndicationLabelKey]
			// 2. filter out Targets which are terminating and not replace indicate
			if !replaceIndicate {
				continue
			}
		}

		targetWrappers = append(targetWrappers, targetWrapper{
			Object:        target,
			ID:            id,
			ContextDetail: ownedIDs[id],
			PlaceHolder:   false,

			ToDelete:  toDelete,
			ToExclude: toExclude,

			IsDuringScaleInOps: opslifecycle.IsDuringOps(r.updateConfig.opsLifecycleMgr, r.scaleInLifecycleAdapter, target),
			IsDuringUpdateOps:  opslifecycle.IsDuringOps(r.updateConfig.opsLifecycleMgr, r.updateLifecycleAdapter, target),
		})

		if id >= 0 {
			syncContext.CurrentIDs.Insert(id)
		}
	}

	// do include exclude targets, and skip doSync() if succeeded
	var inExSucceed bool
	if len(toExcludeTargetNames) > 0 || len(toIncludeTargetNames) > 0 {
		var availableContexts []*appsv1alpha1.ContextDetail
		var getErr error
		availableContexts, ownedIDs, getErr = r.getAvailableTargetIDs(len(toIncludeTargetNames), instance, syncContext)
		if getErr != nil {
			return false, getErr
		}
		if err = r.doIncludeExcludeTargets(ctx, instance, toExcludeTargetNames.List(), toIncludeTargetNames.List(), availableContexts); err != nil {
			r.Recorder.Eventf(instance, corev1.EventTypeWarning, "ExcludeIncludeFailed", "%s syncTargets include exclude with error: %s", r.xsetGVK.Kind, err.Error())
			return false, err
		}
		inExSucceed = true
	}

	// reclaim Target ID which is (1) during ScalingIn, (2) ExcludeTargets
	err = r.reclaimOwnedIDs(false, instance, idToReclaim, ownedIDs, syncContext.CurrentIDs)
	if err != nil {
		r.Recorder.Eventf(instance, corev1.EventTypeWarning, "ReclaimOwnedIDs", "reclaim target contexts with error: %s", err.Error())
		return false, err
	}

	// reclaim scaleStrategy for delete, exclude, include
	err = r.reclaimScaleStrategy(ctx, toDeleteTargetNames, toExcludeTargetNames, toIncludeTargetNames, instance)
	if err != nil {
		r.Recorder.Eventf(instance, corev1.EventTypeWarning, "ReclaimScaleStrategy", "reclaim scaleStrategy with error: %s", err.Error())
		return false, err
	}

	syncContext.TargetWrappers = targetWrappers
	syncContext.OwnedIds = ownedIDs

	syncContext.activeTargets = FilterOutActiveTargetWrappers(syncContext.TargetWrappers)
	syncContext.replacingMap = classifyTargetReplacingMapping(syncContext.activeTargets)

	return inExSucceed, nil
}

// dealIncludeExcludeTargets returns targets which are allowed to exclude and include
func (r *RealSyncControl) dealIncludeExcludeTargets(ctx context.Context, xsetObject api.XSetObject, targets []client.Object) (sets.String, sets.String, error) {
	spec := r.xsetController.GetXSetSpec(xsetObject)
	ownedTargets := sets.String{}
	excludeTargetNames := sets.String{}
	includeTargetNames := sets.String{}

	for _, target := range targets {
		ownedTargets.Insert(target.GetName())
		if _, exist := target.GetLabels()[TargetExcludeIndicationLabelKey]; exist {
			excludeTargetNames.Insert(target.GetName())
		}
	}

	tmpUnOwnedExcludeTargets := sets.String{}
	for _, targetName := range spec.ScaleStrategy.TargetToExclude {
		if ownedTargets.Has(targetName) {
			excludeTargetNames.Insert(targetName)
		} else {
			tmpUnOwnedExcludeTargets.Insert(targetName)
		}
	}

	intersection := sets.String{}
	for _, targetName := range spec.ScaleStrategy.TargetToInclude {
		if excludeTargetNames.Has(targetName) {
			intersection.Insert(targetName)
			excludeTargetNames.Delete(targetName)
		} else if tmpUnOwnedExcludeTargets.Has(targetName) {
			intersection.Insert(targetName)
		} else if !ownedTargets.Has(targetName) {
			includeTargetNames.Insert(targetName)
		}
	}

	if len(intersection) > 0 {
		r.Recorder.Eventf(xsetObject, corev1.EventTypeWarning, "DupExIncludedTarget", "duplicated targets %s in both excluding and including sets", strings.Join(intersection.List(), ", "))
	}

	// seem no need to check allow ResourceExclude, since filterTargets already filter only owned targets
	toExcludeTargets, notAllowedExcludeTargets, exErr := r.allowIncludeExcludeTargets(ctx, xsetObject, excludeTargetNames.List(), AllowResourceExclude)
	toIncludeTargets, notAllowedIncludeTargets, inErr := r.allowIncludeExcludeTargets(ctx, xsetObject, includeTargetNames.List(), AllowResourceInclude)
	if notAllowedExcludeTargets.Len() > 0 {
		r.Recorder.Eventf(xsetObject, corev1.EventTypeWarning, "ExcludeNotAllowed", fmt.Sprintf("targets [%v] are not allowed to exclude, please find out the reason from target's event", notAllowedExcludeTargets.List()))
	}
	if notAllowedIncludeTargets.Len() > 0 {
		r.Recorder.Eventf(xsetObject, corev1.EventTypeWarning, "IncludeNotAllowed", fmt.Sprintf("targets [%v] are not allowed to include, please find out the reason from target's event", notAllowedIncludeTargets.List()))
	}
	return toExcludeTargets, toIncludeTargets, errors.Join(exErr, inErr)
}

// checkAllowFunc refers to AllowResourceExclude and AllowResourceInclude
type checkAllowFunc func(obj metav1.Object, ownerName, ownerKind string) (bool, string)

// allowIncludeExcludeTargets try to classify targetNames to allowedTargets and notAllowedTargets, using checkAllowFunc func
func (r *RealSyncControl) allowIncludeExcludeTargets(ctx context.Context, xset api.XSetObject, targetNames []string, fn checkAllowFunc) (allowTargets, notAllowTargets sets.String, err error) {
	allowTargets = sets.String{}
	notAllowTargets = sets.String{}
	for i := range targetNames {
		target := r.xsetController.EmptyXObject()
		targetName := targetNames[i]
		err = r.Client.Get(ctx, types.NamespacedName{Namespace: xset.GetNamespace(), Name: targetName}, target)
		if apierrors.IsNotFound(err) {
			notAllowTargets.Insert(targetNames[i])
			continue
		} else if err != nil {
			r.Recorder.Eventf(xset, corev1.EventTypeWarning, "ExcludeIncludeFailed", fmt.Sprintf("failed to find target %s: %s", targetNames[i], err.Error()))
			return
		}

		// check allowance for target
		if allowed, reason := fn(target, xset.GetName(), xset.GetObjectKind().GroupVersionKind().Kind); !allowed {
			r.Recorder.Eventf(target, corev1.EventTypeWarning, "ExcludeIncludeNotAllowed",
				fmt.Sprintf("target is not allowed to exclude/include from/to %s %s/%s: %s", r.xsetGVK.Kind, xset.GetNamespace(), xset.GetName(), reason))
			notAllowTargets.Insert(targetName)
			continue
		}
		allowTargets.Insert(targetName)
	}
	return allowTargets, notAllowTargets, nil
}

// Replace is used to replace replace-indicate targets
func (r *RealSyncControl) Replace(ctx context.Context, xsetObject api.XSetObject, syncContext *SyncContext) error {
	var err error
	var needUpdateContext bool
	var idToReclaim sets.Int

	needReplaceOriginTargets, needCleanLabelTargets, targetsNeedCleanLabels, needDeleteTargets := r.dealReplaceTargets(syncContext.FilteredTarget)

	// delete origin targets for replace
	err = BatchDelete(ctx, r.xControl, needDeleteTargets)
	if err != nil {
		r.Recorder.Eventf(xsetObject, corev1.EventTypeWarning, "ReplaceTarget", "delete targets by label with error: %s", err.Error())
		return err
	}

	// clean labels for replace targets
	needUpdateContext, idToReclaim, err = r.cleanReplaceTargetLabels(ctx, needCleanLabelTargets, targetsNeedCleanLabels, syncContext.OwnedIds, syncContext.CurrentIDs)
	if err != nil {
		r.Recorder.Eventf(xsetObject, corev1.EventTypeWarning, "ReplaceTarget", fmt.Sprintf("clean targets replace pair origin name label with error: %s", err.Error()))
		return err
	}

	// create new targets for need replace targets
	if len(needReplaceOriginTargets) > 0 {
		var availableContexts []*appsv1alpha1.ContextDetail
		var getErr error
		availableContexts, syncContext.OwnedIds, getErr = r.getAvailableTargetIDs(len(needReplaceOriginTargets), xsetObject, syncContext)
		if getErr != nil {
			return getErr
		}
		successCount, err := r.replaceOriginTargets(ctx, xsetObject, syncContext, needReplaceOriginTargets, syncContext.OwnedIds, availableContexts)
		needUpdateContext = needUpdateContext || successCount > 0
		if err != nil {
			r.Recorder.Eventf(xsetObject, corev1.EventTypeWarning, "ReplaceTarget", "deal replace targets with error: %s", err.Error())
			return err
		}
	}

	// reclaim Target ID which is ReplaceOriginTarget
	if err := r.reclaimOwnedIDs(needUpdateContext, xsetObject, idToReclaim, syncContext.OwnedIds, syncContext.CurrentIDs); err != nil {
		r.Recorder.Eventf(xsetObject, corev1.EventTypeWarning, "ReclaimOwnedIDs", "reclaim target contexts with error: %s", err.Error())
		return err
	}

	// create targetWrappers for non-exist targets
	for id, contextDetail := range syncContext.OwnedIds {
		if _, inUsed := syncContext.CurrentIDs[id]; inUsed {
			continue
		}
		syncContext.TargetWrappers = append(syncContext.TargetWrappers, targetWrapper{
			ID:            id,
			Object:        nil,
			ContextDetail: contextDetail,
			PlaceHolder:   true,
		})
	}

	return nil
}

func (r *RealSyncControl) Scale(ctx context.Context, xsetObject api.XSetObject, syncContext *SyncContext) (bool, *time.Duration, error) {
	spec := r.xsetController.GetXSetSpec(xsetObject)
	logger := r.Logger.WithValues(r.xsetGVK.Kind, ObjectKeyString(xsetObject))
	var recordedRequeueAfter *time.Duration

	diff := int(RealValue(spec.Replicas)) - len(syncContext.replacingMap)
	scaling := false

	if diff >= 0 {
		// trigger delete targets indicated in ScaleStrategy.TargetToDelete by label
		for _, targetWrapper := range syncContext.activeTargets {
			if targetWrapper.ToDelete {
				err := BatchDelete(ctx, r.xControl, []client.Object{targetWrapper.Object})
				if err != nil {
					return false, recordedRequeueAfter, err
				}
			}
		}

		// scale out targets and return if diff > 0
		if diff > 0 {
			// collect instance ID in used from owned Targets
			targetInstanceIDSet := sets.Int{}
			for _, target := range syncContext.activeTargets {
				targetInstanceIDSet[target.ID] = struct{}{}
			}

			// find IDs and their contexts which have not been used by owned Targets
			availableContext := resourcecontexts.ExtractAvailableContexts(diff, syncContext.OwnedIds, targetInstanceIDSet)
			needUpdateContext := atomic.Bool{}
			succCount, err := controllerutils.SlowStartBatch(len(availableContext), controllerutils.SlowStartInitialBatchSize, false, func(i int, _ error) (err error) {
				availableIDContext := availableContext[i]
				defer func() {
					if decideContextRevision(availableIDContext, syncContext.UpdatedRevision, err == nil) {
						needUpdateContext.Store(true)
					}
				}()
				// use revision recorded in Context
				revision := syncContext.UpdatedRevision
				if revisionName, exist := availableIDContext.Data[resourcecontexts.RevisionContextDataKey]; exist && revisionName != "" {
					for i := range syncContext.Revisions {
						if syncContext.Revisions[i].GetName() == revisionName {
							revision = syncContext.Revisions[i]
							break
						}
					}
				}
				// scale out new Targets with updatedRevision
				// TODO use cache
				target, err := NewTargetFrom(r.xsetController, xsetObject, revision, availableIDContext.ID)
				if err != nil {
					return fmt.Errorf("fail to new Target from revision %s: %w", revision.GetName(), err)
				}

				newTarget := target.DeepCopyObject().(client.Object)
				logger.V(1).Info("try to create Target with revision of "+r.xsetGVK.Kind, "revision", revision.GetName())
				if target, err = r.xControl.CreateTarget(ctx, newTarget); err != nil {
					return err
				}
				// add an expectation for this target creation, before next reconciling
				return r.cacheExpectation.ExpectCreation(r.targetGVK, target.GetNamespace(), target.GetName())
			})
			if needUpdateContext.Load() {
				logger.V(1).Info("try to update ResourceContext for XSet after scaling out")
				if updateContextErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
					return resourcecontexts.UpdateToTargetContext(r.xsetController, r.Client, r.cacheExpectation, xsetObject, syncContext.OwnedIds)
				}); updateContextErr != nil {
					err = errors.Join(updateContextErr, err)
				}
			}
			if err != nil {
				AddOrUpdateCondition(syncContext.NewStatus, api.XSetScale, err, "ScaleOutFailed", err.Error())
				return succCount > 0, recordedRequeueAfter, err
			}
			r.Recorder.Eventf(xsetObject, corev1.EventTypeNormal, "ScaleOut", "scale out %d Target(s)", succCount)
			AddOrUpdateCondition(syncContext.NewStatus, api.XSetScale, nil, "ScaleOut", "")
			return succCount > 0, recordedRequeueAfter, err
		} else {
			AddOrUpdateCondition(syncContext.NewStatus, api.XSetScale, nil, "ScaleOut", "")
			return false, nil, nil
		}
	} else if diff < 0 {
		// chose the targets to scale in
		targetsToScaleIn := r.getTargetsToDelete(syncContext.activeTargets, syncContext.replacingMap, diff*-1)
		// filter out Targets need to trigger TargetOpsLifecycle
		wrapperCh := make(chan *targetWrapper, len(targetsToScaleIn))
		for i := range targetsToScaleIn {
			if targetsToScaleIn[i].IsDuringScaleInOps {
				continue
			}
			wrapperCh <- targetsToScaleIn[i]
		}

		// trigger Targets to enter TargetOpsLifecycle
		succCount, err := controllerutils.SlowStartBatch(len(wrapperCh), controllerutils.SlowStartInitialBatchSize, false, func(_ int, err error) error {
			wrapper := <-wrapperCh
			object := wrapper.Object

			// trigger TargetOpsLifecycle with scaleIn OperationType
			logger.V(1).Info("try to begin TargetOpsLifecycle for scaling in Target in XSet", "wrapper", ObjectKeyString(object))
			// todo switch to x opslifecycle
			if updated, err := opslifecycle.Begin(r.updateConfig.opsLifecycleMgr, r.Client, r.scaleInLifecycleAdapter, object); err != nil {
				return fmt.Errorf("fail to begin TargetOpsLifecycle for Scaling in Target %s/%s: %w", object.GetNamespace(), object.GetName(), err)
			} else if updated {
				r.Recorder.Eventf(object, corev1.EventTypeNormal, "BeginScaleInLifecycle", "succeed to begin TargetOpsLifecycle for scaling in")
				// add an expectation for this wrapper creation, before next reconciling
				if err := r.cacheExpectation.ExpectUpdation(r.targetGVK, object.GetNamespace(), object.GetName(), object.GetResourceVersion()); err != nil {
					return err
				}
			}

			return nil
		})
		scaling = succCount != 0

		if err != nil {
			AddOrUpdateCondition(syncContext.NewStatus, api.XSetScale, err, "ScaleInFailed", err.Error())
			return scaling, recordedRequeueAfter, err
		} else {
			AddOrUpdateCondition(syncContext.NewStatus, api.XSetScale, nil, "ScaleIn", "")
		}

		needUpdateContext := false
		for i, targetWrapper := range targetsToScaleIn {
			requeueAfter, allowed := opslifecycle.AllowOps(r.updateConfig.opsLifecycleMgr, r.scaleInLifecycleAdapter, RealValue(spec.ScaleStrategy.OperationDelaySeconds), targetWrapper.Object)
			if !allowed && targetWrapper.Object.GetDeletionTimestamp() == nil {
				r.Recorder.Eventf(targetWrapper.Object, corev1.EventTypeNormal, "TargetScaleInLifecycle", "Target is not allowed to scale in")
				continue
			}

			if requeueAfter != nil {
				r.Recorder.Eventf(targetWrapper.Object, corev1.EventTypeNormal, "TargetScaleInLifecycle", "delay Target scale in for %d seconds", requeueAfter.Seconds())
				if recordedRequeueAfter == nil || *requeueAfter < *recordedRequeueAfter {
					recordedRequeueAfter = requeueAfter
				}

				continue
			}

			// if Target is allowed to operate or Target has already been deleted, promte to delete Target
			if contextDetail, exist := syncContext.OwnedIds[targetWrapper.ID]; exist && !contextDetail.Contains(resourcecontexts.ScaleInContextDataKey, "true") {
				needUpdateContext = true
				contextDetail.Put(resourcecontexts.ScaleInContextDataKey, "true")
			}

			if targetWrapper.GetDeletionTimestamp() != nil {
				continue
			}

			wrapperCh <- targetsToScaleIn[i]
		}

		// mark these Targets to scalingIn
		if needUpdateContext {
			logger.V(1).Info("try to update ResourceContext for XSet when scaling in Target")
			if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				return resourcecontexts.UpdateToTargetContext(r.xsetController, r.Client, r.cacheExpectation, xsetObject, syncContext.OwnedIds)
			}); err != nil {
				AddOrUpdateCondition(syncContext.NewStatus, api.XSetScale, err, "ScaleInFailed", fmt.Sprintf("failed to update Context for scaling in: %s", err))
				return scaling, recordedRequeueAfter, err
			} else {
				AddOrUpdateCondition(syncContext.NewStatus, api.XSetScale, nil, "ScaleIn", "")
			}
		}

		// do delete Target resource
		succCount, err = controllerutils.SlowStartBatch(len(wrapperCh), controllerutils.SlowStartInitialBatchSize, false, func(i int, _ error) error {
			target := <-wrapperCh
			logger.V(1).Info("try to scale in Target", "target", ObjectKeyString(target))
			if err := r.xControl.DeleteTarget(ctx, target.Object); err != nil {
				return fmt.Errorf("fail to delete Target %s/%s when scaling in: %w", target.GetNamespace(), target.GetName(), err)
			}

			r.Recorder.Eventf(xsetObject, corev1.EventTypeNormal, "TargetDeleted", "succeed to scale in Target %s/%s", target.GetNamespace(), target.GetName())
			return nil
		})
		scaling = scaling || succCount > 0

		if succCount > 0 {
			r.Recorder.Eventf(xsetObject, corev1.EventTypeNormal, "ScaleIn", "scale in %d Target(s)", succCount)
		}
		if err != nil {
			AddOrUpdateCondition(syncContext.NewStatus, api.XSetScale, err, "ScaleInFailed", fmt.Sprintf("fail to delete Target for scaling in: %s", err))
			return scaling, recordedRequeueAfter, err
		} else {
			AddOrUpdateCondition(syncContext.NewStatus, api.XSetScale, nil, "ScaleIn", "")
		}

		return scaling, recordedRequeueAfter, err
	}

	// reset ContextDetail.ScalingIn, if there are Targets had its TargetOpsLifecycle reverted
	needUpdateTargetContext := false
	for _, targetWrapper := range syncContext.activeTargets {
		if contextDetail, exist := syncContext.OwnedIds[targetWrapper.ID]; exist && contextDetail.Contains(resourcecontexts.ScaleInContextDataKey, "true") &&
			!opslifecycle.IsDuringOps(r.updateConfig.opsLifecycleMgr, r.scaleInLifecycleAdapter, targetWrapper) {
			needUpdateTargetContext = true
			contextDetail.Remove(resourcecontexts.ScaleInContextDataKey)
		}
	}

	if needUpdateTargetContext {
		logger.V(1).Info("try to update ResourceContext for XSet after scaling")
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			return resourcecontexts.UpdateToTargetContext(r.xsetController, r.Client, r.cacheExpectation, xsetObject, syncContext.OwnedIds)
		}); err != nil {
			return scaling, recordedRequeueAfter, fmt.Errorf("fail to reset ResourceContext: %w", err)
		}
	}

	return scaling, recordedRequeueAfter, nil
}

func (r *RealSyncControl) Update(ctx context.Context, xsetObject api.XSetObject, syncContext *SyncContext) (bool, *time.Duration, error) {
	logger := r.Logger.WithValues("xset", ObjectKeyString(xsetObject))
	var err error
	var recordedRequeueAfter *time.Duration
	// 1. scan and analysis targets update info for active targets and PlaceHolder targets
	targetUpdateInfos := r.attachTargetUpdateInfo(xsetObject, syncContext)

	// 2. decide Target update candidates
	candidates := decideTargetToUpdate(r.xsetController, xsetObject, targetUpdateInfos)
	targetToUpdate := filterOutPlaceHolderUpdateInfos(candidates)
	targetCh := make(chan *targetUpdateInfo, len(targetToUpdate))
	updater := r.newTargetUpdater(xsetObject)
	updating := false

	// 3. filter already updated revision,
	for i, targetInfo := range targetToUpdate {
		if targetInfo.IsUpdatedRevision {
			continue
		}

		// 3.1 fulfillTargetUpdateInfo to all not updatedRevision target
		if targetInfo.CurrentRevision.GetName() != UnknownRevision {
			if err = updater.FulfillTargetUpdatedInfo(ctx, syncContext.UpdatedRevision, targetInfo); err != nil {
				logger.Error(err, fmt.Sprintf("fail to analyze target %s/%s in-place update support", targetInfo.GetNamespace(), targetInfo.GetName()))
				continue
			}
		}

		if targetInfo.GetDeletionTimestamp() != nil {
			continue
		}

		if opslifecycle.IsDuringOps(r.updateConfig.opsLifecycleMgr, r.updateLifecycleAdapter, targetInfo) {
			continue
		}

		targetCh <- targetToUpdate[i]
	}

	// 4. begin target update lifecycle
	updating, err = updater.BeginUpdateTarget(ctx, syncContext, targetCh)
	if err != nil {
		return updating, recordedRequeueAfter, err
	}

	// 5. (1) filter out  targets not allow to ops now, such as OperationDelaySeconds strategy; (2) update PlaceHolder Targets resourceContext revision
	recordedRequeueAfter, err = updater.FilterAllowOpsTargets(ctx, candidates, syncContext.OwnedIds, syncContext, targetCh)
	if err != nil {
		AddOrUpdateCondition(syncContext.NewStatus,
			api.XSetUpdate, err, "UpdateFailed",
			fmt.Sprintf("fail to update Context for updating: %s", err))
		return updating, recordedRequeueAfter, err
	} else {
		AddOrUpdateCondition(syncContext.NewStatus,
			api.XSetUpdate, nil, "Updated", "")
	}

	// 6. update Target
	succCount, err := controllerutils.SlowStartBatch(len(targetCh), controllerutils.SlowStartInitialBatchSize, false, func(_ int, _ error) error {
		targetInfo := <-targetCh
		logger.V(1).Info("before target update operation",
			"target", ObjectKeyString(targetInfo.Object),
			"revision.from", targetInfo.CurrentRevision.GetName(),
			"revision.to", syncContext.UpdatedRevision.GetName(),
			"inPlaceUpdate", targetInfo.InPlaceUpdateSupport,
			"onlyMetadataChanged", targetInfo.OnlyMetadataChanged,
		)

		spec := r.xsetController.GetXSetSpec(xsetObject)
		isReplaceUpdate := spec.UpdateStrategy.UpdatePolicy == api.XSetReplaceTargetUpdateStrategyType
		if targetInfo.IsInReplacing && !isReplaceUpdate {
			// a replacing target should be replaced by an updated revision target when encountering upgrade
			if err := updateReplaceOriginTarget(ctx, r.Client, r.Recorder, targetInfo, targetInfo.ReplacePairNewTargetInfo); err != nil {
				return err
			}
		} else {
			if err := updater.UpgradeTarget(ctx, targetInfo); err != nil {
				return err
			}
		}

		return nil
	})

	updating = updating || succCount > 0
	if err != nil {
		AddOrUpdateCondition(syncContext.NewStatus, api.XSetUpdate, err, "UpdateFailed", err.Error())
		return updating, recordedRequeueAfter, err
	} else {
		AddOrUpdateCondition(syncContext.NewStatus, api.XSetUpdate, nil, "Updated", "")
	}

	targetToUpdateSet := sets.String{}
	for i := range targetToUpdate {
		targetToUpdateSet.Insert(targetToUpdate[i].GetName())
	}
	// 7. try to finish all Targets'TargetOpsLifecycle if its update is finished.
	succCount, err = controllerutils.SlowStartBatch(len(targetUpdateInfos), controllerutils.SlowStartInitialBatchSize, false, func(i int, _ error) error {
		targetInfo := targetUpdateInfos[i]

		if !targetInfo.IsDuringOps || targetInfo.PlaceHolder || targetInfo.GetDeletionTimestamp() != nil {
			return nil
		}

		// check Target is during updating, and it is finished or not
		finished, msg, err := updater.GetTargetUpdateFinishStatus(ctx, targetInfo)
		if err != nil {
			return fmt.Errorf("failed to get target %s/%s update finished: %w", targetInfo.GetNamespace(), targetInfo.GetName(), err)
		}

		if finished {
			if err := updater.FinishUpdateTarget(ctx, targetInfo); err != nil {
				return err
			}
			r.Recorder.Eventf(targetInfo.Object,
				corev1.EventTypeNormal,
				"UpdateTargetFinished",
				"target %s/%s is finished for upgrade to revision %s",
				targetInfo.GetNamespace(), targetInfo.GetName(), targetInfo.UpdateRevision.GetName())
		} else {
			r.Recorder.Eventf(targetInfo.Object,
				corev1.EventTypeNormal,
				"WaitingUpdateReady",
				"waiting for target %s/%s to update finished: %s",
				targetInfo.GetNamespace(), targetInfo.GetName(), msg)
		}

		return nil
	})

	return updating || succCount > 0, recordedRequeueAfter, err
}

func (r *RealSyncControl) CalculateStatus(ctx context.Context, instance api.XSetObject, syncContext *SyncContext) *api.XSetStatus {
	newStatus := syncContext.NewStatus
	newStatus.ObservedGeneration = instance.GetGeneration()

	var readyReplicas, scheduledReplicas, replicas, updatedReplicas, operatingReplicas, updatedReadyReplicas, availableReplicas, updatedAvailableReplicas int32

	activeTargets := FilterOutActiveTargetWrappers(syncContext.TargetWrappers)
	for _, targetWrapper := range activeTargets {
		if targetWrapper.GetDeletionTimestamp() != nil {
			continue
		}

		replicas++

		isUpdated := false
		if isUpdated = IsTargetUpdatedRevision(targetWrapper.Object, syncContext.UpdatedRevision.Name); isUpdated {
			updatedReplicas++
		}

		if opslifecycle.IsDuringOps(r.updateConfig.opsLifecycleMgr, r.scaleInLifecycleAdapter, targetWrapper) ||
			opslifecycle.IsDuringOps(r.updateConfig.opsLifecycleMgr, r.updateLifecycleAdapter, targetWrapper) {
			operatingReplicas++
		}

		if r.xsetController.CheckReady(targetWrapper.Object) {
			readyReplicas++
			if isUpdated {
				updatedReadyReplicas++
			}
		}

		if r.xsetController.CheckAvailable(targetWrapper.Object) {
			availableReplicas++
			if isUpdated {
				updatedAvailableReplicas++
			}
		}

		if r.xsetController.CheckScheduled(targetWrapper.Object) {
			scheduledReplicas++
		}
	}

	newStatus.ReadyReplicas = readyReplicas
	newStatus.Replicas = replicas
	newStatus.UpdatedReplicas = updatedReplicas
	newStatus.OperatingReplicas = operatingReplicas
	newStatus.UpdatedReadyReplicas = updatedReadyReplicas
	newStatus.ScheduledReplicas = scheduledReplicas
	newStatus.AvailableReplicas = availableReplicas
	newStatus.UpdatedAvailableReplicas = updatedAvailableReplicas

	spec := r.xsetController.GetXSetSpec(instance)
	if (spec.Replicas == nil && newStatus.UpdatedReadyReplicas >= 0) ||
		newStatus.UpdatedReadyReplicas >= *spec.Replicas {
		newStatus.CurrentRevision = syncContext.UpdatedRevision.Name
	}

	return newStatus
}

// getAvailableTargetIDs try to extract and re-allocate want available IDs.
func (r *RealSyncControl) getAvailableTargetIDs(
	want int,
	instance api.XSetObject,
	syncContext *SyncContext,
) ([]*appsv1alpha1.ContextDetail, map[int]*appsv1alpha1.ContextDetail, error) {
	ownedIDs := syncContext.OwnedIds
	currentIDs := syncContext.CurrentIDs

	availableContexts := resourcecontexts.ExtractAvailableContexts(want, ownedIDs, currentIDs)
	if len(availableContexts) >= want {
		return availableContexts, ownedIDs, nil
	}

	diff := want - len(availableContexts)

	var newOwnedIDs map[int]*appsv1alpha1.ContextDetail
	var err error
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		newOwnedIDs, err = resourcecontexts.AllocateID(r.xsetController, r.Client, r.cacheExpectation, instance, syncContext.UpdatedRevision.GetName(), len(ownedIDs)+diff)
		return err
	}); err != nil {
		return nil, ownedIDs, fmt.Errorf("fail to allocate IDs using context when include Targets: %w", err)
	}

	return resourcecontexts.ExtractAvailableContexts(want, newOwnedIDs, currentIDs), newOwnedIDs, nil
}

// reclaimOwnedIDs delete and reclaim unused IDs
func (r *RealSyncControl) reclaimOwnedIDs(
	needUpdateContext bool,
	xset api.XSetObject,
	idToReclaim sets.Int,
	ownedIDs map[int]*appsv1alpha1.ContextDetail,
	currentIDs sets.Int,
) error {
	// TODO stateful case
	// 1) only reclaim non-existing Targets' ID. Do not reclaim terminating Targets' ID until these Targets and PVC have been deleted from ETCD
	// 2) do not filter out these terminating Targets
	for id, contextDetail := range ownedIDs {
		if _, exist := currentIDs[id]; exist {
			continue
		}
		if contextDetail.Contains(resourcecontexts.ScaleInContextDataKey, "true") {
			idToReclaim.Insert(id)
		}
	}

	for _, id := range idToReclaim.List() {
		needUpdateContext = true
		delete(ownedIDs, id)
	}

	// TODO clean replace-pair-keys or dirty targetContext
	// 1) replace pair target are not exists
	// 2) target exists but is not replaceIndicated

	if needUpdateContext {
		logger := r.Logger.WithValues(r.xsetGVK.Kind, ObjectKeyString(xset))
		logger.V(1).Info("try to update ResourceContext for XSet when sync")
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			return resourcecontexts.UpdateToTargetContext(r.xsetController, r.Client, r.cacheExpectation, xset, ownedIDs)
		}); err != nil {
			return fmt.Errorf("fail to update ResourceContext when reclaiming IDs: %w", err)
		}
	}
	return nil
}

// FilterOutActiveTargetWrappers filter out non placeholder targets
func FilterOutActiveTargetWrappers(targets []targetWrapper) []*targetWrapper {
	var filteredTargetWrappers []*targetWrapper
	for i, target := range targets {
		if target.PlaceHolder {
			continue
		}
		filteredTargetWrappers = append(filteredTargetWrappers, &targets[i])
	}
	return filteredTargetWrappers
}

func targetDuringReplace(target client.Object) bool {
	labels := target.GetLabels()
	if labels == nil {
		return false
	}
	_, replaceIndicate := labels[TargetReplaceIndicationLabelKey]
	_, replaceOriginTarget := labels[TargetReplacePairNewId]
	_, replaceNewTarget := labels[TargetReplacePairOriginName]
	return replaceIndicate || replaceOriginTarget || replaceNewTarget
}

// BatchDelete try to trigger target deletion by to-delete label
func BatchDelete(ctx context.Context, targetControl xcontrol.TargetControl, needDeleteTargets []client.Object) error {
	_, err := controllerutils.SlowStartBatch(len(needDeleteTargets), controllerutils.SlowStartInitialBatchSize, false, func(i int, _ error) error {
		return targetControl.DeleteTarget(ctx, needDeleteTargets[i])
	})
	return err
}

// decideContextRevision decides revision for 3 target create types: (1) just create, (2) upgrade by recreate, (3) delete and recreate
func decideContextRevision(contextDetail *appsv1alpha1.ContextDetail, updatedRevision *appsv1.ControllerRevision, createSucceeded bool) bool {
	needUpdateContext := false
	if !createSucceeded {
		if contextDetail.Contains(resourcecontexts.JustCreateContextDataKey, "true") {
			// TODO choose just create targets' revision according to scaleStrategy
			contextDetail.Put(resourcecontexts.RevisionContextDataKey, updatedRevision.GetName())
			delete(contextDetail.Data, resourcecontexts.TargetDecorationRevisionKey)
			needUpdateContext = true
		} else if contextDetail.Contains(resourcecontexts.RecreateUpdateContextDataKey, "true") {
			contextDetail.Put(resourcecontexts.RevisionContextDataKey, updatedRevision.GetName())
			delete(contextDetail.Data, resourcecontexts.TargetDecorationRevisionKey)
			needUpdateContext = true
		}
		// if target is delete and recreate, never change revisionKey
	} else {
		// TODO delete ID if create succeeded
		contextDetail.Remove(resourcecontexts.JustCreateContextDataKey)
		contextDetail.Remove(resourcecontexts.RecreateUpdateContextDataKey)
		needUpdateContext = true
	}
	return needUpdateContext
}
