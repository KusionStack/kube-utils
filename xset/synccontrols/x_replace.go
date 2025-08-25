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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clientutil "kusionstack.io/kube-utils/client"
	controllerutils "kusionstack.io/kube-utils/controller/utils"
	"kusionstack.io/kube-utils/xset/api"
	"kusionstack.io/kube-utils/xset/opslifecycle"
)

const (
	ReplaceNewTargetIDContextDataKey    = "ReplaceNewTargetID"
	ReplaceOriginTargetIDContextDataKey = "ReplaceOriginTargetID"
)

func (r *RealSyncControl) cleanReplaceTargetLabels(ctx context.Context, needCleanLabelTargets []client.Object, targetsNeedCleanLabels [][]string, ownedIDs map[int]*api.ContextDetail, currentIDs sets.Int) (bool, sets.Int, error) {
	needUpdateContext := false
	needDeleteTargetsIDs := sets.Int{}
	mapOriginToNewTargetContext := mapReplaceOriginToNewTargetContext(ownedIDs)
	mapNewToOriginTargetContext := mapReplaceNewToOriginTargetContext(ownedIDs)
	_, err := controllerutils.SlowStartBatch(len(needCleanLabelTargets), controllerutils.SlowStartInitialBatchSize, false, func(i int, _ error) error {
		target := needCleanLabelTargets[i]
		needCleanLabels := targetsNeedCleanLabels[i]
		var deletePatch []map[string]string
		for _, labelKey := range needCleanLabels {
			patchOperation := map[string]string{
				"op":   "remove",
				"path": fmt.Sprintf("/metadata/labels/%s", strings.ReplaceAll(labelKey, "/", "~1")),
			}
			deletePatch = append(deletePatch, patchOperation)
			// replace finished, (1) remove ReplaceNewTargetID, ReplaceOriginTargetID key from IDs, (2) try to delete origin Target's ID
			if labelKey == TargetReplacePairOriginName {
				needUpdateContext = true
				newTargetId, _ := GetInstanceID(target)
				if originTargetContext, exist := mapOriginToNewTargetContext[newTargetId]; exist && originTargetContext != nil {
					originTargetContext.Remove(ReplaceNewTargetIDContextDataKey)
					if _, exist := currentIDs[originTargetContext.ID]; !exist {
						needDeleteTargetsIDs.Insert(originTargetContext.ID)
					}
				}
				if contextDetail, exist := ownedIDs[newTargetId]; exist {
					contextDetail.Remove(ReplaceOriginTargetIDContextDataKey)
				}
			}
			// replace canceled, (1) remove ReplaceNewTargetID, ReplaceOriginTargetID key from IDs, (2) try to delete new Target's ID
			_, replaceIndicate := target.GetLabels()[TargetReplaceIndicationLabelKey]
			if !replaceIndicate && labelKey == TargetReplacePairNewId {
				needUpdateContext = true
				originTargetId, _ := GetInstanceID(target)
				if newTargetContext, exist := mapNewToOriginTargetContext[originTargetId]; exist && newTargetContext != nil {
					newTargetContext.Remove(ReplaceOriginTargetIDContextDataKey)
					if _, exist := currentIDs[newTargetContext.ID]; !exist {
						needDeleteTargetsIDs.Insert(newTargetContext.ID)
					}
				}
				if contextDetail, exist := ownedIDs[originTargetId]; exist {
					contextDetail.Remove(ReplaceNewTargetIDContextDataKey)
				}
			}
		}
		// patch to bytes
		patchBytes, err := json.Marshal(deletePatch)
		if err != nil {
			return err
		}
		if err = r.xControl.PatchTarget(ctx, target, client.RawPatch(types.JSONPatchType, patchBytes)); err != nil {
			return fmt.Errorf("failed to remove replace pair label %s/%s: %w", target.GetNamespace(), target.GetName(), err)
		}
		return nil
	})

	return needUpdateContext, needDeleteTargetsIDs, err
}

func (r *RealSyncControl) replaceOriginTargets(
	ctx context.Context,
	instance api.XSetObject,
	syncContext *SyncContext,
	needReplaceOriginTargets []client.Object,
	ownedIDs map[int]*api.ContextDetail,
	availableContexts []*api.ContextDetail,
) (int, error) {
	mapNewToOriginTargetContext := mapReplaceNewToOriginTargetContext(ownedIDs)
	successCount, err := controllerutils.SlowStartBatch(len(needReplaceOriginTargets), controllerutils.SlowStartInitialBatchSize, false, func(i int, _ error) error {
		originTarget := needReplaceOriginTargets[i]
		originTargetId, _ := GetInstanceID(originTarget)

		replaceRevision := getReplaceRevision(originTarget, syncContext)

		// create target using update revision if replaced by update, otherwise using current revision
		newTarget, err := NewTargetFrom(r.xsetController, instance, replaceRevision, originTargetId)
		if err != nil {
			return err
		}
		// add instance id and replace pair label
		var instanceId string
		var newTargetContext *api.ContextDetail
		if contextDetail, exist := mapNewToOriginTargetContext[originTargetId]; exist && contextDetail != nil {
			newTargetContext = contextDetail
			// reuse targetContext ID if pair-relation exists
			instanceId = fmt.Sprintf("%d", newTargetContext.ID)
			newTarget.GetLabels()[TargetInstanceIDLabelKey] = instanceId
		} else {
			if availableContexts[i] == nil {
				return fmt.Errorf("cannot found available context for replace new target when replacing origin target %s/%s", originTarget.GetNamespace(), originTarget.GetName())
			}
			newTargetContext = availableContexts[i]
			// add replace pair-relation to targetContexts for originTarget and newTarget
			instanceId = fmt.Sprintf("%d", newTargetContext.ID)
			newTarget.GetLabels()[TargetInstanceIDLabelKey] = instanceId
			ownedIDs[originTargetId].Put(ReplaceNewTargetIDContextDataKey, instanceId)
			ownedIDs[newTargetContext.ID].Put(ReplaceOriginTargetIDContextDataKey, strconv.Itoa(originTargetId))
			r.resourceContextControl.Remove(ownedIDs[newTargetContext.ID], api.EnumJustCreateContextDataKey)
		}
		newTarget.GetLabels()[TargetReplacePairOriginName] = originTarget.GetName()
		r.resourceContextControl.Put(newTargetContext, api.EnumRevisionContextDataKey, replaceRevision.GetName())

		if newCreatedTarget, err := r.xControl.CreateTarget(ctx, newTarget); err == nil {
			r.Recorder.Eventf(originTarget,
				corev1.EventTypeNormal,
				"CreatePairTarget",
				"succeed to create replace pair Target %s/%s with revision %s by replace",
				originTarget.GetNamespace(),
				originTarget.GetName(),
				replaceRevision.GetName())

			patch := client.RawPatch(types.MergePatchType, []byte(fmt.Sprintf(`{"metadata":{"labels":{%q:%q}}}`, TargetReplacePairNewId, instanceId)))
			if err = r.xControl.PatchTarget(ctx, originTarget, patch); err != nil {
				return fmt.Errorf("fail to update origin target %s/%s pair label %s when updating by replaceUpdate: %s", originTarget.GetNamespace(), originTarget.GetName(), newCreatedTarget.GetName(), err.Error())
			}
			return r.cacheExpectations.ExpectCreation(clientutil.ObjectKeyString(instance), r.targetGVK, newTarget.GetNamespace(), newTarget.GetName())
		} else {
			r.Recorder.Eventf(originTarget,
				corev1.EventTypeNormal,
				"ReplaceTarget",
				"failed to create replace pair Target %s/%s from revision %s by replace update: %s",
				originTarget.GetNamespace(),
				originTarget.GetName(),
				replaceRevision.GetName(),
				err.Error())
			return err
		}
	})

	return successCount, err
}

func (r *RealSyncControl) dealReplaceTargets(targets []client.Object, logger logr.Logger) (
	needReplaceTargets, needCleanLabelTargets []client.Object, targetNeedCleanLabels [][]string, needDeleteTargets []client.Object,
) {
	targetInstanceIdMap := make(map[string]client.Object)
	targetNameMap := make(map[string]client.Object)

	for _, target := range targets {
		targetLabels := target.GetLabels()

		if instanceId, ok := targetLabels[TargetInstanceIDLabelKey]; ok {
			targetInstanceIdMap[instanceId] = target
		}
		targetNameMap[target.GetName()] = target
	}

	// deal need replace targets
	for _, target := range targets {
		targetLabels := target.GetLabels()

		// no replace indication label
		if _, exist := targetLabels[TargetReplaceIndicationLabelKey]; !exist {
			continue
		}

		// origin target is about to scaleIn, skip replace
		if opslifecycle.IsDuringOps(r.updateConfig.opsLifecycleLabelMgr, r.scaleInLifecycleAdapter, target) {
			logger.Info("dealReplaceTargets", "target is during scaleIn ops lifecycle, skip replacing", target.GetName())
			continue
		}

		// target is replace new created target, skip replace
		if originTargetName, exist := targetLabels[TargetReplacePairOriginName]; exist {
			if _, exist := targetNameMap[originTargetName]; exist {
				continue
			}
		}

		// target already has a new created target for replacement
		if newPairTargetId, exist := targetLabels[TargetReplacePairNewId]; exist {
			if _, exist := targetInstanceIdMap[newPairTargetId]; exist {
				continue
			}
		}

		needReplaceTargets = append(needReplaceTargets, target)
	}

	for _, target := range targets {
		targetLabels := target.GetLabels()
		_, replaceByUpdate := targetLabels[TargetReplaceByReplaceUpdateLabelKey]
		var needCleanLabels []string

		// target is replace new created target, skip replace
		if originTargetName, exist := targetLabels[TargetReplacePairOriginName]; exist {
			// replace pair origin target is not exist, clean label.
			if originTarget, exist := targetNameMap[originTargetName]; !exist {
				needCleanLabels = append(needCleanLabels, TargetReplacePairOriginName)
			} else if originTarget.GetLabels()[TargetReplaceIndicationLabelKey] == "" {
				// replace canceled, delete replace new target if new target is not service available
				if serviceAvailable := opslifecycle.IsServiceAvailable(r.updateConfig.opsLifecycleLabelMgr, target); !serviceAvailable {
					needDeleteTargets = append(needDeleteTargets, target)
				}
			} else if !replaceByUpdate {
				// not replace update, delete origin target when new created target is service available
				if serviceAvailable := opslifecycle.IsServiceAvailable(r.updateConfig.opsLifecycleLabelMgr, target); serviceAvailable {
					needDeleteTargets = append(needDeleteTargets, originTarget)
				}
			}
		}

		if newPairTargetId, exist := targetLabels[TargetReplacePairNewId]; exist {
			if _, exist := targetInstanceIdMap[newPairTargetId]; !exist {
				needCleanLabels = append(needCleanLabels, TargetReplacePairNewId)
			}
		}

		if len(needCleanLabels) > 0 {
			needCleanLabelTargets = append(needCleanLabelTargets, target)
			targetNeedCleanLabels = append(targetNeedCleanLabels, needCleanLabels)
		}
	}
	return needReplaceTargets, needCleanLabelTargets, targetNeedCleanLabels, needDeleteTargets
}

func updateReplaceOriginTarget(
	ctx context.Context,
	c client.Client,
	recorder record.EventRecorder,
	originTargetUpdateInfo, newTargetUpdateInfo *targetUpdateInfo,
) error {
	originTarget := originTargetUpdateInfo.Object

	// 1. delete the new target if not updated
	if newTargetUpdateInfo != nil {
		newTarget := newTargetUpdateInfo.Object
		_, deletionIndicate := newTarget.GetLabels()[TargetDeletionIndicationLabelKey]
		currentRevision, exist := newTarget.GetLabels()[appsv1.ControllerRevisionHashLabelKey]
		if exist && currentRevision != originTargetUpdateInfo.UpdateRevision.GetName() && !deletionIndicate {
			patch := client.RawPatch(types.MergePatchType, []byte(fmt.Sprintf(`{"metadata":{"labels":{%q:"%d"}}}`, TargetDeletionIndicationLabelKey, time.Now().UnixNano())))
			if patchErr := c.Patch(ctx, newTarget, patch); patchErr != nil {
				err := fmt.Errorf("failed to delete replace pair new target %s/%s %s",
					newTarget.GetNamespace(), newTarget.GetName(), patchErr.Error())
				return err
			}
			recorder.Eventf(originTarget,
				corev1.EventTypeNormal,
				"DeleteOldNewTarget",
				"succeed to delete replace new Target %s/%s by label to-replace",
				originTarget.GetNamespace(),
				originTarget.GetName(),
			)
		}
	}

	// 2. replace the origin target with updated target
	_, replaceIndicate := originTarget.GetLabels()[TargetReplaceIndicationLabelKey]
	_, replaceByUpdate := originTarget.GetLabels()[TargetReplaceByReplaceUpdateLabelKey]
	if !replaceIndicate || !replaceByUpdate {
		now := time.Now().UnixNano()
		patch := client.RawPatch(types.MergePatchType, []byte(fmt.Sprintf(`{"metadata":{"labels":{%q:"%v", %q: "%v"}}}`, TargetReplaceIndicationLabelKey, now, TargetReplaceByReplaceUpdateLabelKey, originTargetUpdateInfo.UpdateRevision.GetName())))
		if err := c.Patch(ctx, originTarget, patch); err != nil {
			return fmt.Errorf("fail to label origin target %s/%s with replace indicate label by replaceUpdate: %s", originTarget.GetNamespace(), originTarget.GetName(), err.Error())
		}
		recorder.Eventf(originTarget,
			corev1.EventTypeNormal,
			"UpdateOriginTarget",
			"succeed to update Target %s/%s by label to-replace",
			originTarget.GetNamespace(),
			originTarget.GetName(),
		)
	}

	return nil
}

// getReplaceRevision finds replaceNewTarget's revision from originTarget
func getReplaceRevision(originTarget client.Object, syncContext *SyncContext) *appsv1.ControllerRevision {
	// replace update, first find revision from label, if revision not found, just replace with updated revision
	if updateRevisionName, exist := originTarget.GetLabels()[TargetReplaceByReplaceUpdateLabelKey]; exist {
		for _, rv := range syncContext.Revisions {
			if updateRevisionName == rv.GetName() {
				return rv
			}
		}
		return syncContext.UpdatedRevision
	}

	// replace by to-replace label, just replace with current revision
	targetCurrentRevisionName, exist := originTarget.GetLabels()[appsv1.ControllerRevisionHashLabelKey]
	if !exist {
		return syncContext.CurrentRevision
	}

	for _, revision := range syncContext.Revisions {
		if revision.GetName() == targetCurrentRevisionName {
			return revision
		}
	}

	return syncContext.CurrentRevision
}

// classify the pair relationship for Target replacement.
func classifyTargetReplacingMapping(targetWrappers []*targetWrapper) map[string]*targetWrapper {
	targetNameMap := make(map[string]*targetWrapper)
	targetIdMap := make(map[string]*targetWrapper)
	for _, targetWrapper := range targetWrappers {
		targetNameMap[targetWrapper.GetName()] = targetWrapper
		targetIdMap[strconv.Itoa(targetWrapper.ID)] = targetWrapper
	}

	// old target name => new target wrapper
	replaceTargetMapping := make(map[string]*targetWrapper)
	for _, targetWrapper := range targetWrappers {
		if targetWrapper.Object == nil {
			continue
		}
		name := targetWrapper.GetName()
		if replacePairNewIdStr, exist := targetWrapper.GetLabels()[TargetReplacePairNewId]; exist {
			if pairNewTarget, exist := targetIdMap[replacePairNewIdStr]; exist {
				replaceTargetMapping[name] = pairNewTarget
				// if one of pair targets is to Exclude, both targets should not scaleIn
				targetWrapper.ToExclude = targetWrapper.ToExclude || pairNewTarget.ToExclude
				continue
			}
		} else if replaceOriginStr, exist := targetWrapper.GetLabels()[TargetReplacePairOriginName]; exist {
			if originTarget, exist := targetNameMap[replaceOriginStr]; exist {
				if originTarget.GetLabels()[TargetReplacePairNewId] == strconv.Itoa(targetWrapper.ID) {
					continue
				}
			}
		}

		// non paired target, just put it in the map
		replaceTargetMapping[name] = nil
	}
	return replaceTargetMapping
}

func mapReplaceNewToOriginTargetContext(ownedIDs map[int]*api.ContextDetail) map[int]*api.ContextDetail {
	mapNewToOriginTargetContext := make(map[int]*api.ContextDetail)
	for id, contextDetail := range ownedIDs {
		if val, exist := contextDetail.Data[ReplaceNewTargetIDContextDataKey]; exist {
			newTargetId, _ := strconv.ParseInt(val, 10, 32)
			newTargetContextDetail, exist := ownedIDs[int(newTargetId)]
			if exist && newTargetContextDetail.Data[ReplaceOriginTargetIDContextDataKey] == strconv.Itoa(id) {
				mapNewToOriginTargetContext[id] = newTargetContextDetail
			} else {
				mapNewToOriginTargetContext[id] = nil
			}
		}
	}
	return mapNewToOriginTargetContext
}

func mapReplaceOriginToNewTargetContext(ownedIDs map[int]*api.ContextDetail) map[int]*api.ContextDetail {
	mapOriginToNewTargetContext := make(map[int]*api.ContextDetail)
	for id, contextDetail := range ownedIDs {
		if val, exist := contextDetail.Data[ReplaceOriginTargetIDContextDataKey]; exist {
			originTargetId, _ := strconv.ParseInt(val, 10, 32)
			originTargetContextDetail, exist := ownedIDs[int(originTargetId)]
			if exist && originTargetContextDetail.Data[ReplaceNewTargetIDContextDataKey] == strconv.Itoa(id) {
				mapOriginToNewTargetContext[id] = originTargetContextDetail
			} else {
				mapOriginToNewTargetContext[id] = nil
			}
		}
	}
	return mapOriginToNewTargetContext
}
