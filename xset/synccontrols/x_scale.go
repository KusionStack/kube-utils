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
	"fmt"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clientutil "kusionstack.io/kube-utils/client"
	controllerutils "kusionstack.io/kube-utils/controller/utils"
	"kusionstack.io/kube-utils/xset/api"
	"kusionstack.io/kube-utils/xset/opslifecycle"
	"kusionstack.io/kube-utils/xset/subresources"
)

// getTargetsToDelete
// 1. finds number of diff targets from filteredPods to do scaleIn
// 2. finds targets allowed to scale in out of diff
func (r *RealSyncControl) getTargetsToDelete(xsetObject api.XSetObject, filteredTargets []*targetWrapper, replaceMapping map[string]*targetWrapper, diff int) []*targetWrapper {
	var countedTargets []*targetWrapper
	for _, target := range filteredTargets {
		if _, exist := replaceMapping[target.GetName()]; exist {
			countedTargets = append(countedTargets, target)
		}
	}

	// 1. select targets to delete in first round according to diff
	sort.Sort(newActiveTargetsForDeletion(countedTargets, r.xsetController.CheckReadyTime))
	if diff > len(countedTargets) {
		diff = len(countedTargets)
	}

	// 2. select targets to delete in second round according to replace, delete, exclude
	var needDeleteTargets []*targetWrapper
	for i, target := range countedTargets {
		// find pods to be scaleIn out of diff, is allowed to ops
		spec := r.xsetController.GetXSetSpec(xsetObject)
		_, allowed := opslifecycle.AllowOps(r.updateConfig.xsetLabelAnnoMgr, r.scaleInLifecycleAdapter, ptr.Deref(spec.ScaleStrategy.OperationDelaySeconds, 0), target)
		if i >= diff && !allowed {
			continue
		}

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
			if !r.xsetController.CheckAvailable(target.Object) {
				needDeleteTargets = append(needDeleteTargets, replacePairTarget)
			}
		}
		needDeleteTargets = append(needDeleteTargets, target)
	}

	return needDeleteTargets
}

type ActiveTargetsForDeletion struct {
	targets        []*targetWrapper
	checkReadyFunc func(object client.Object) (bool, *metav1.Time)
}

func newActiveTargetsForDeletion(
	targets []*targetWrapper,
	checkReadyFunc func(object client.Object) (bool, *metav1.Time),
) *ActiveTargetsForDeletion {
	return &ActiveTargetsForDeletion{
		targets:        targets,
		checkReadyFunc: checkReadyFunc,
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

	lReady, _ := s.checkReadyFunc(l.Object)
	rReady, _ := s.checkReadyFunc(r.Object)
	if lReady != rReady {
		return lReady
	}

	if l.OpsPriority != nil && r.OpsPriority != nil {
		if l.OpsPriority.PriorityClass != r.OpsPriority.PriorityClass {
			return l.OpsPriority.PriorityClass < r.OpsPriority.PriorityClass
		}
		if l.OpsPriority.DeletionCost != r.OpsPriority.DeletionCost {
			return l.OpsPriority.DeletionCost < r.OpsPriority.DeletionCost
		}
	}

	// TODO consider service available timestamps
	return CompareTarget(l.Object, r.Object, s.checkReadyFunc)
}

// dealIncludeExcludeTargets returns targets which are allowed to exclude and include
func (r *RealSyncControl) dealIncludeExcludeTargets(ctx context.Context, xsetObject api.XSetObject, targets []client.Object) (sets.String, sets.String, error) {
	spec := r.xsetController.GetXSetSpec(xsetObject)
	ownedTargets := sets.String{}
	excludeTargetNames := sets.String{}
	includeTargetNames := sets.String{}

	for _, target := range targets {
		ownedTargets.Insert(target.GetName())
		if _, exist := r.xsetLabelAnnoMgr.Get(target.GetLabels(), api.XExcludeIndicationLabelKey); exist {
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

	toExcludeTargets, notAllowedExcludeTargets, exErr := r.allowIncludeExcludeTargets(ctx, xsetObject, excludeTargetNames.List(), AllowResourceExclude, r.xsetLabelAnnoMgr)
	toIncludeTargets, notAllowedIncludeTargets, inErr := r.allowIncludeExcludeTargets(ctx, xsetObject, includeTargetNames.List(), AllowResourceInclude, r.xsetLabelAnnoMgr)
	if notAllowedExcludeTargets.Len() > 0 {
		r.Recorder.Eventf(xsetObject, corev1.EventTypeWarning, "ExcludeNotAllowed", fmt.Sprintf("targets [%v] are not allowed to exclude, please find out the reason from target's event", notAllowedExcludeTargets.List()))
	}
	if notAllowedIncludeTargets.Len() > 0 {
		r.Recorder.Eventf(xsetObject, corev1.EventTypeWarning, "IncludeNotAllowed", fmt.Sprintf("targets [%v] are not allowed to include, please find out the reason from target's event", notAllowedIncludeTargets.List()))
	}
	return toExcludeTargets, toIncludeTargets, errors.Join(exErr, inErr)
}

// checkAllowFunc refers to AllowResourceExclude and AllowResourceInclude
type checkAllowFunc func(obj metav1.Object, ownerName, ownerKind string, labelMgr api.XSetLabelAnnotationManager) (bool, string)

// allowIncludeExcludeTargets try to classify targetNames to allowedTargets and notAllowedTargets, using checkAllowFunc func
func (r *RealSyncControl) allowIncludeExcludeTargets(ctx context.Context, xset api.XSetObject, targetNames []string, fn checkAllowFunc, labelMgr api.XSetLabelAnnotationManager) (allowTargets, notAllowTargets sets.String, err error) {
	allowTargets = sets.String{}
	notAllowTargets = sets.String{}
	for i := range targetNames {
		target := r.xsetController.NewXObject()
		targetName := targetNames[i]
		err = r.Client.Get(ctx, types.NamespacedName{Namespace: xset.GetNamespace(), Name: targetName}, target)
		if apierrors.IsNotFound(err) {
			notAllowTargets.Insert(targetNames[i])
			continue
		} else if err != nil {
			r.Recorder.Eventf(xset, corev1.EventTypeWarning, "ExcludeIncludeFailed", fmt.Sprintf("failed to find target %s: %s", targetNames[i], err.Error()))
			return allowTargets, notAllowTargets, err
		}

		// check allowance for target
		if allowed, reason := fn(target, xset.GetName(), xset.GetObjectKind().GroupVersionKind().Kind, labelMgr); !allowed {
			r.Recorder.Eventf(target, corev1.EventTypeWarning, "ExcludeIncludeNotAllowed",
				fmt.Sprintf("target is not allowed to exclude/include from/to %s %s/%s: %s", r.xsetGVK.Kind, xset.GetNamespace(), xset.GetName(), reason))
			notAllowTargets.Insert(targetName)
			continue
		}

		// check allowance for subresource
		pvcsAllowed := true
		if subresources.SubresourcePvcEnabled(r.pvcControl) {
			adapter, _ := r.xsetController.(api.SubResourcePvcAdapter)
			volumes := adapter.GetXSpecVolumes(target)
			for i := range volumes {
				volume := volumes[i]
				if volume.PersistentVolumeClaim == nil {
					continue
				}
				pvc := &corev1.PersistentVolumeClaim{}
				err = r.Client.Get(ctx, types.NamespacedName{Namespace: target.GetNamespace(), Name: volume.PersistentVolumeClaim.ClaimName}, pvc)
				// if pvc not found, ignore it. In case of pvc is filtered by controller-mesh
				if apierrors.IsNotFound(err) {
					continue
				} else if err != nil {
					r.Recorder.Eventf(target, corev1.EventTypeWarning, "ExcludeIncludeNotAllowed", fmt.Sprintf("failed to check allowed to exclude/include from/to xset %s/%s: %s", xset.GetNamespace(), xset.GetName(), err.Error()))
					pvcsAllowed = false
				}
				if allowed, reason := fn(pvc, xset.GetName(), xset.GetObjectKind().GroupVersionKind().Kind, labelMgr); !allowed {
					r.Recorder.Eventf(target, corev1.EventTypeWarning, "ExcludeIncludeNotAllowed", fmt.Sprintf("failed to check allowed to exclude/include from/to xset %s/%s: %s", xset.GetNamespace(), xset.GetName(), reason))
					pvcsAllowed = false
				}
			}
		}
		if pvcsAllowed {
			allowTargets.Insert(targetName)
		} else {
			notAllowTargets.Insert(targetName)
		}
	}
	return allowTargets, notAllowTargets, nil
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

	// exclude subresource
	if subresources.SubresourcePvcEnabled(r.pvcControl) {
		adapter, _ := r.xsetController.(api.SubResourcePvcAdapter)
		volumes := adapter.GetXSpecVolumes(target)
		for i := range volumes {
			volume := volumes[i]
			if volume.PersistentVolumeClaim == nil {
				continue
			}
			pvc := &corev1.PersistentVolumeClaim{}
			err := r.Client.Get(ctx, types.NamespacedName{Namespace: target.GetNamespace(), Name: volume.PersistentVolumeClaim.ClaimName}, pvc)
			// If pvc not found, ignore it. In case of pvc is filtered out by controller-mesh
			if apierrors.IsNotFound(err) {
				continue
			} else if err != nil {
				return err
			}

			r.xsetLabelAnnoMgr.Set(pvc, api.XOrphanedIndicationLabelKey, "true")
			if err := r.pvcControl.OrphanPvc(ctx, xsetObject, pvc); err != nil {
				return err
			}
		}
	}

	r.xsetLabelAnnoMgr.Set(target, api.XOrphanedIndicationLabelKey, "true")
	if err := r.xControl.OrphanTarget(xsetObject, target); err != nil {
		return err
	}
	return r.cacheExpectations.ExpectUpdation(clientutil.ObjectKeyString(xsetObject), r.targetGVK, target.GetNamespace(), target.GetName(), target.GetResourceVersion())
}

// includeTarget try to include a target into xset
func (r *RealSyncControl) includeTarget(ctx context.Context, xsetObject api.XSetObject, targetName, instanceId string) error {
	target := r.xsetController.NewXObject()
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: xsetObject.GetNamespace(), Name: targetName}, target); err != nil {
		return err
	}

	// exclude subresource
	if subresources.SubresourcePvcEnabled(r.pvcControl) {
		adapter, _ := r.xsetController.(api.SubResourcePvcAdapter)
		volumes := adapter.GetXSpecVolumes(target)
		for i := range volumes {
			volume := volumes[i]
			if volume.PersistentVolumeClaim == nil {
				continue
			}
			pvc := &corev1.PersistentVolumeClaim{}
			err := r.Client.Get(ctx, types.NamespacedName{Namespace: target.GetNamespace(), Name: volume.PersistentVolumeClaim.ClaimName}, pvc)
			// If pvc not found, ignore it. In case of pvc is filtered out by controller-mesh
			if apierrors.IsNotFound(err) {
				continue
			} else if err != nil {
				return err
			}

			r.xsetLabelAnnoMgr.Set(pvc, api.XInstanceIdLabelKey, instanceId)
			r.xsetLabelAnnoMgr.Delete(pvc.GetLabels(), api.XOrphanedIndicationLabelKey)
			if err := r.pvcControl.AdoptPvc(ctx, xsetObject, pvc); err != nil {
				return err
			}
		}
	}

	r.xsetLabelAnnoMgr.Set(target, api.XInstanceIdLabelKey, instanceId)
	r.xsetLabelAnnoMgr.Delete(target.GetLabels(), api.XOrphanedIndicationLabelKey)
	if err := r.xControl.AdoptTarget(xsetObject, target); err != nil {
		return err
	}
	return r.cacheExpectations.ExpectUpdation(clientutil.ObjectKeyString(xsetObject), r.targetGVK, target.GetNamespace(), target.GetName(), target.GetResourceVersion())
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
	if err := r.xsetController.UpdateScaleStrategy(ctx, r.Client, xsetObject, &xspec.ScaleStrategy); err != nil {
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
