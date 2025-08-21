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
	"errors"
	"sort"
	"strconv"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/retry"

	clientutil "kusionstack.io/kube-utils/client"
	controllerutils "kusionstack.io/kube-utils/controller/utils"
	"kusionstack.io/kube-utils/xset/api"
	"kusionstack.io/kube-utils/xset/opslifecycle"
)

// getTargetsToDelete finds number of diff targets from filteredTargets to do scaleIn
func (r *RealSyncControl) getTargetsToDelete(filteredTargets []*targetWrapper, replaceMapping map[string]*targetWrapper, diff int) []*targetWrapper {
	var countedTargets []*targetWrapper
	for _, target := range filteredTargets {
		if _, exist := replaceMapping[target.GetName()]; exist {
			countedTargets = append(countedTargets, target)
		}
	}

	// 1. select targets to delete in first round according to diff
	sort.Sort(newActiveTargetsForDeletion(countedTargets))
	if diff > len(countedTargets) {
		diff = len(countedTargets)
	}

	// 2. select targets to delete in second round according to replace, delete, exclude
	var needDeleteTargets []*targetWrapper
	for _, target := range countedTargets[:diff] {
		//  don't scaleIn exclude target and its newTarget (if exist)
		if target.ToExclude {
			continue
		}

		if replacePairTarget, exist := replaceMapping[target.GetName()]; exist && replacePairTarget != nil {
			// don't selective scaleIn newTarget (and its originTarget) until replace finished
			if replacePairTarget.ToDelete && !target.ToDelete {
				continue
			}
			// when scaleIn origin Target, newTarget should be deleted if not service available
			if serviceAvailable := opslifecycle.IsServiceAvailable(r.updateConfig.opsLifecycleMgr, target); !serviceAvailable {
				needDeleteTargets = append(needDeleteTargets, replacePairTarget)
			}
		}
		needDeleteTargets = append(needDeleteTargets, target)
	}

	return needDeleteTargets
}

type ActiveTargetsForDeletion struct {
	targets []*targetWrapper
}

func newActiveTargetsForDeletion(targets []*targetWrapper) *ActiveTargetsForDeletion {
	return &ActiveTargetsForDeletion{
		targets: targets,
	}
}

func (s *ActiveTargetsForDeletion) Len() int { return len(s.targets) }
func (s *ActiveTargetsForDeletion) Swap(i, j int) {
	s.targets[i], s.targets[j] = s.targets[j], s.targets[i]
}

// Less sort deletion order by: targetToDelete > targetToExclude > duringScaleIn > others
func (s *ActiveTargetsForDeletion) Less(i, j int) bool {
	l, r := s.targets[i], s.targets[j]

	if l.ToDelete != r.ToDelete {
		return l.ToDelete
	}

	if l.ToExclude != r.ToExclude {
		return l.ToExclude
	}

	// targets which are during scaleInOps should be deleted before those not during
	if l.IsDuringScaleInOps != r.IsDuringScaleInOps {
		return l.IsDuringScaleInOps
	}

	// TODO consider service available timestamps

	lCreationTime := l.Object.GetCreationTimestamp().Time
	rCreationTime := r.Object.GetCreationTimestamp().Time
	return lCreationTime.Before(rCreationTime)
}

// doIncludeExcludeTargets do real include and exclude for targets which are allowed to in/exclude
func (r *RealSyncControl) doIncludeExcludeTargets(ctx context.Context, xset api.XSetObject, excludeTargets, includeTargets []string, availableContexts []*api.ContextDetail) error {
	var excludeErrs, includeErrs []error
	_, _ = controllerutils.SlowStartBatch(len(excludeTargets), controllerutils.SlowStartInitialBatchSize, false, func(idx int, _ error) (err error) {
		defer func() { excludeErrs = append(excludeErrs, err) }()
		return r.excludeTarget(ctx, xset, excludeTargets[idx])
	})
	_, _ = controllerutils.SlowStartBatch(len(includeTargets), controllerutils.SlowStartInitialBatchSize, false, func(idx int, _ error) (err error) {
		defer func() { includeErrs = append(includeErrs, err) }()
		return r.includeTarget(ctx, xset, includeTargets[idx], strconv.Itoa(availableContexts[idx].ID))
	})
	return errors.Join(append(includeErrs, excludeErrs...)...)
}

// excludeTarget try to exclude a target from xset
func (r *RealSyncControl) excludeTarget(ctx context.Context, xsetObject api.XSetObject, targetName string) error {
	target := r.xsetController.NewXObject()
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: xsetObject.GetNamespace(), Name: targetName}, target); err != nil {
		return err
	}

	target.GetLabels()[TargetOrphanedIndicateLabelKey] = "true"
	return r.xControl.OrphanTarget(xsetObject, target)
}

// includeTarget try to include a target into xset
func (r *RealSyncControl) includeTarget(ctx context.Context, xsetObject api.XSetObject, targetName, instanceId string) error {
	target := r.xsetController.NewXObject()
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: xsetObject.GetNamespace(), Name: targetName}, target); err != nil {
		return err
	}

	target.GetLabels()[TargetInstanceIDLabelKey] = instanceId
	delete(target.GetLabels(), TargetOrphanedIndicateLabelKey)
	return r.xControl.AdoptTarget(xsetObject, target)
}

// reclaimScaleStrategy updates targetToDelete, targetToExclude, targetToInclude in scaleStrategy
func (r *RealSyncControl) reclaimScaleStrategy(ctx context.Context, deletedTargets, excludedTargets, includedTargets sets.String, xsetObject api.XSetObject) error {
	xspec := r.xsetController.GetXSetSpec(xsetObject)
	// reclaim TargetToDelete
	toDeleteTargets := sets.NewString(xspec.ScaleStrategy.TargetToDelete...)
	notDeletedTargets := toDeleteTargets.Delete(deletedTargets.List()...)
	xspec.ScaleStrategy.TargetToDelete = notDeletedTargets.List()
	// reclaim TargetToExclude
	toExcludeTargets := sets.NewString(xspec.ScaleStrategy.TargetToExclude...)
	notExcludeTargets := toExcludeTargets.Delete(excludedTargets.List()...)
	xspec.ScaleStrategy.TargetToExclude = notExcludeTargets.List()
	// reclaim TargetToInclude
	toIncludeTargetNames := sets.NewString(xspec.ScaleStrategy.TargetToInclude...)
	notIncludeTargets := toIncludeTargetNames.Delete(includedTargets.List()...)
	xspec.ScaleStrategy.TargetToInclude = notIncludeTargets.List()
	if err := r.xsetController.UpdateScaleStrategy(xsetObject, &xspec.ScaleStrategy); err != nil {
		return err
	}
	// update xsetObject.spec.scaleStrategy
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		return r.Client.Update(ctx, xsetObject)
	}); err != nil {
		return err
	}
	return r.cacheExpectations.ExpectUpdation(clientutil.ObjectKeyString(xsetObject), r.xsetGVK, xsetObject.GetNamespace(), xsetObject.GetName(), xsetObject.GetResourceVersion())
}
