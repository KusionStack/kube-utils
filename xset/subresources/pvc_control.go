/*
 * Copyright 2024-2025 KusionStack Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package subresources

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/sets"
	appsv1alpha1 "kusionstack.io/kube-api/apps/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kubeutilclient "kusionstack.io/kube-utils/client"
	"kusionstack.io/kube-utils/controller/expectations"
	"kusionstack.io/kube-utils/controller/mixin"
	refmanagerutil "kusionstack.io/kube-utils/controller/refmanager"
	"kusionstack.io/kube-utils/xset/api"
)

const (
	FieldIndexOwnerRefUID = "ownerRefUID"
)

var PVCGvk = corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim")

type PvcControl interface {
	GetFilteredPvcs(context.Context, api.XSetObject) ([]*corev1.PersistentVolumeClaim, error)
	CreateTargetPvcs(context.Context, api.XSetObject, client.Object, []*corev1.PersistentVolumeClaim) error
	DeleteTargetPvcs(context.Context, api.XSetObject, client.Object, []*corev1.PersistentVolumeClaim) error
	DeleteTargetUnusedPvcs(context.Context, api.XSetObject, client.Object, []*corev1.PersistentVolumeClaim) error
	OrphanPvc(context.Context, api.XSetObject, *corev1.PersistentVolumeClaim) error
	AdoptPvc(context.Context, api.XSetObject, *corev1.PersistentVolumeClaim) error
	AdoptPvcsLeftByRetainPolicy(context.Context, api.XSetObject) ([]*corev1.PersistentVolumeClaim, error)
	IsTargetPvcTmpChanged(api.XSetObject, client.Object, []*corev1.PersistentVolumeClaim) (bool, error)
	RetainPvcWhenXSetDeleted(xset api.XSetObject) bool
	RetainPvcWhenXSetScaled(xset api.XSetObject) bool
}

type RealPvcControl struct {
	client           client.Client
	scheme           *runtime.Scheme
	pvcAdapter       api.SubResourcePvcAdapter
	expectations     *expectations.CacheExpectations
	xsetLabelAnnoMgr api.XSetLabelAnnotationManager
	xsetController   api.XSetController
}

func NewRealPvcControl(mixin *mixin.ReconcilerMixin, expectations *expectations.CacheExpectations, xsetLabelAnnoMgr api.XSetLabelAnnotationManager, xsetController api.XSetController) (PvcControl, error) {
	// requires x kind is Pod
	xmeta := xsetController.XMeta()
	if xmeta.Kind != "Pod" {
		return nil, nil
	}
	// requires implementation of SubResourcePvcAdapter
	pvcAdapter, ok := xsetController.(api.SubResourcePvcAdapter)
	if !ok {
		return nil, nil
	}
	// here we go, set up cache and return real pvc control
	if err := setUpCache(mixin.Cache, xsetController); err != nil {
		return nil, err
	}
	return &RealPvcControl{
		client:           mixin.Client,
		scheme:           mixin.Scheme,
		pvcAdapter:       pvcAdapter,
		expectations:     expectations,
		xsetLabelAnnoMgr: xsetLabelAnnoMgr,
		xsetController:   xsetController,
	}, nil
}

func (pc *RealPvcControl) GetFilteredPvcs(ctx context.Context, xset api.XSetObject) ([]*corev1.PersistentVolumeClaim, error) {
	// list pvcs using ownerReference
	var filteredPvcs []*corev1.PersistentVolumeClaim
	ownedPvcList := &corev1.PersistentVolumeClaimList{}
	if err := pc.client.List(ctx, ownedPvcList, &client.ListOptions{
		Namespace:     xset.GetNamespace(),
		FieldSelector: fields.OneTermEqualSelector(FieldIndexOwnerRefUID, string(xset.GetUID())),
	}); err != nil {
		return nil, err
	}

	for i := range ownedPvcList.Items {
		pvc := &ownedPvcList.Items[i]
		if pvc.DeletionTimestamp == nil {
			filteredPvcs = append(filteredPvcs, pvc)
		}
	}
	return filteredPvcs, nil
}

func (pc *RealPvcControl) CreateTargetPvcs(ctx context.Context, xset api.XSetObject, x client.Object, existingPvcs []*corev1.PersistentVolumeClaim) error {
	id, exist := pc.xsetLabelAnnoMgr.Get(x.GetLabels(), api.XInstanceIdLabelKey)
	if !exist {
		return nil
	}

	// provision pvcs related to pod using pvc template, and reuse
	// pvcs if "instance-id" and "pvc-template-hash" label matched
	pvcsMap, err := pc.provisionUpdatedPvc(ctx, id, xset, existingPvcs)
	if err != nil {
		return err
	}

	newVolumes := make([]corev1.Volume, 0, len(pvcsMap))
	// mount updated pvcs to target
	for name, pvc := range pvcsMap {
		volume := corev1.Volume{
			Name: name,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvc.Name,
					ReadOnly:  false,
				},
			},
		}
		newVolumes = append(newVolumes, volume)
	}

	// append legacy pvcs
	currentVolumes := pc.pvcAdapter.GetXSpecVolumes(x)
	for i := range currentVolumes {
		currentVolume := currentVolumes[i]
		if _, ok := pvcsMap[currentVolume.Name]; !ok {
			newVolumes = append(newVolumes, currentVolume)
		}
	}
	pc.pvcAdapter.SetXSpecVolumes(x, newVolumes)
	return nil
}

func (pc *RealPvcControl) provisionUpdatedPvc(ctx context.Context, id string, xset api.XSetObject, existingPvcs []*corev1.PersistentVolumeClaim) (map[string]*corev1.PersistentVolumeClaim, error) {
	updatedPvcs, _, err := pc.classifyTargetPvcs(id, xset, existingPvcs)
	if err != nil {
		return nil, err
	}

	templates := pc.pvcAdapter.GetXSetPvcTemplate(xset)
	for i := range templates {
		pvcTmp := templates[i]
		// reuse pvc
		if _, exist := updatedPvcs[pvcTmp.Name]; exist {
			continue
		}
		// create new pvc
		claim, err := pc.buildPvcWithHash(id, xset, &pvcTmp)
		if err != nil {
			return nil, err
		}

		if err := pc.client.Create(ctx, claim); err != nil {
			return nil, fmt.Errorf("fail to create pvc for id %s: %w", id, err)
		}

		if err := pc.expectations.ExpectCreation(
			kubeutilclient.ObjectKeyString(xset),
			PVCGvk,
			claim.Namespace,
			claim.Name,
		); err != nil {
			return nil, err
		}

		updatedPvcs[pvcTmp.Name] = claim
	}
	return updatedPvcs, nil
}

func (pc *RealPvcControl) DeleteTargetPvcs(ctx context.Context, xset api.XSetObject, x client.Object, pvcs []*corev1.PersistentVolumeClaim) error {
	for _, pvc := range pvcs {
		if pvc.Labels == nil || x.GetLabels() == nil {
			continue
		}

		// only delete pvcs used by target
		pvcId, _ := pc.xsetLabelAnnoMgr.Get(pvc.Labels, api.XInstanceIdLabelKey)
		targetId, _ := pc.xsetLabelAnnoMgr.Get(x.GetLabels(), api.XInstanceIdLabelKey)
		if pvcId != targetId {
			continue
		}

		if err := deletePvcWithExpectations(ctx, pc.client, xset, pc.expectations, pvc); err != nil {
			return err
		}
	}
	return nil
}

func (pc *RealPvcControl) DeleteTargetUnusedPvcs(ctx context.Context, xset api.XSetObject, x client.Object, existingPvcs []*corev1.PersistentVolumeClaim) error {
	if x.GetLabels() == nil {
		return nil
	}
	id, exist := pc.xsetLabelAnnoMgr.Get(x.GetLabels(), api.XInstanceIdLabelKey)
	if !exist {
		return nil
	}

	newPvcs, oldPvcs, err := pc.classifyTargetPvcs(id, xset, existingPvcs)
	if err != nil {
		return err
	}

	volumeMounts := pc.pvcAdapter.GetXVolumeMounts(x)
	mountedVolumeTmps := sets.String{}
	for i := range volumeMounts {
		mountedVolumeTmps.Insert(volumeMounts[i].Name)
	}

	// delete pvc which is not claimed in templates
	if err := pc.deleteUnclaimedPvcs(ctx, xset, oldPvcs, mountedVolumeTmps); err != nil {
		return err
	}
	// delete old pvc if new pvc is provisioned and not RetainPVCWhenXSetScaled
	if pc.pvcAdapter.RetainPvcWhenXSetScaled(xset) {
		return pc.deleteOldPvcs(ctx, xset, newPvcs, oldPvcs)
	}
	return nil
}

func (pc *RealPvcControl) OrphanPvc(ctx context.Context, xset api.XSetObject, pvc *corev1.PersistentVolumeClaim) error {
	xsetSpec := pc.xsetController.GetXSetSpec(xset)
	if xsetSpec.Selector.MatchLabels == nil {
		return nil
	}
	refWriter := refmanagerutil.NewOwnerRefWriter(pc.client)
	matcher, err := refmanagerutil.LabelSelectorAsMatch(xsetSpec.Selector)
	if err != nil {
		return fmt.Errorf("fail to create labelSelector matcher: %s", err.Error())
	}
	refManager := refmanagerutil.NewObjectControllerRefManager(refWriter, xset, xset.GetObjectKind().GroupVersionKind(), matcher)

	if _, err := refManager.Claim(ctx, pvc); err != nil {
		return fmt.Errorf("failed to adopt pvc: %s", err.Error())
	}
	return nil
}

func (pc *RealPvcControl) AdoptPvc(ctx context.Context, xset api.XSetObject, pvc *corev1.PersistentVolumeClaim) error {
	xsetSpec := pc.xsetController.GetXSetSpec(xset)
	if xsetSpec.Selector.MatchLabels == nil {
		return nil
	}
	if pvc.Labels == nil || pvc.Annotations == nil {
		return nil
	}

	refWriter := refmanagerutil.NewOwnerRefWriter(pc.client)
	if err := refWriter.Release(ctx, xset, pvc); err != nil {
		return fmt.Errorf("failed to orphan target: %s", err.Error())
	}
	return nil
}

func (pc *RealPvcControl) AdoptPvcsLeftByRetainPolicy(ctx context.Context, xset api.XSetObject) ([]*corev1.PersistentVolumeClaim, error) {
	xsetSpec := pc.xsetController.GetXSetSpec(xset)
	ownerSelector := xsetSpec.Selector.DeepCopy()
	if ownerSelector.MatchLabels == nil {
		ownerSelector.MatchLabels = map[string]string{}
	}
	ownerSelector.MatchLabels[pc.xsetLabelAnnoMgr.Value(api.ControlledByXSetLabel)] = "true"
	ownerSelector.MatchExpressions = append(ownerSelector.MatchExpressions, metav1.LabelSelectorRequirement{ // nolint
		Key:      pc.xsetLabelAnnoMgr.Value(api.XOrphanedIndicationLabelKey), // should not be excluded pvcs
		Operator: metav1.LabelSelectorOpDoesNotExist,
	})
	ownerSelector.MatchExpressions = append(ownerSelector.MatchExpressions, metav1.LabelSelectorRequirement{
		Key:      pc.xsetLabelAnnoMgr.Value(api.XInstanceIdLabelKey), // instance-id label should exist
		Operator: metav1.LabelSelectorOpExists,
	})
	ownerSelector.MatchExpressions = append(ownerSelector.MatchExpressions, metav1.LabelSelectorRequirement{
		Key:      pc.xsetLabelAnnoMgr.Value(api.SubResourcePvcTemplateHashLabelKey), // pvc-hash label should exist
		Operator: metav1.LabelSelectorOpExists,
	})

	selector, err := metav1.LabelSelectorAsSelector(ownerSelector)
	if err != nil {
		return nil, err
	}

	orphanedPvcList := &corev1.PersistentVolumeClaimList{}
	if err := pc.client.List(ctx, orphanedPvcList, &client.ListOptions{Namespace: xset.GetNamespace(), LabelSelector: selector}); err != nil {
		return nil, err
	}

	// adopt orphaned pvcs
	var claims []*corev1.PersistentVolumeClaim
	for i := range orphanedPvcList.Items {
		pvc := orphanedPvcList.Items[i]
		if pvc.OwnerReferences != nil && len(pvc.OwnerReferences) > 0 {
			continue
		}
		if pvc.Labels == nil {
			pvc.Labels = make(map[string]string)
		}
		if pvc.Annotations == nil {
			pvc.Annotations = make(map[string]string)
		}

		claims = append(claims, &pvc)
	}
	for i := range claims {
		if err := pc.AdoptPvc(ctx, xset, claims[i]); err != nil {
			return nil, err
		}
	}
	return claims, nil
}

func (pc *RealPvcControl) IsTargetPvcTmpChanged(xset api.XSetObject, x client.Object, existingPvcs []*corev1.PersistentVolumeClaim) (bool, error) {
	pvcTemplates := pc.pvcAdapter.GetXSetPvcTemplate(xset)
	xSpecVolumes := pc.pvcAdapter.GetXSpecVolumes(x)
	// get pvc template hash values
	newHashMapping, err := PvcTmpHashMapping(pvcTemplates)
	if err != nil {
		return false, err
	}

	// get existing x pvcs hash values
	existingPvcHash := map[string]string{}
	for _, pvc := range existingPvcs {
		if pvc.Labels == nil || x.GetLabels() == nil {
			continue
		}
		pvcId, _ := pc.xsetLabelAnnoMgr.Get(pvc.Labels, api.XInstanceIdLabelKey)
		targetId, _ := pc.xsetLabelAnnoMgr.Get(x.GetLabels(), api.XInstanceIdLabelKey)
		if pvcId != targetId {
			continue
		}
		if _, exist := pc.xsetLabelAnnoMgr.Get(pvc.Labels, api.SubResourcePvcTemplateHashLabelKey); !exist {
			continue
		}
		existingPvcHash[pvc.Name] = pvc.Labels[appsv1alpha1.PvcTemplateHashLabelKey]
	}

	// check mounted pvcs changed
	for i := range xSpecVolumes {
		volume := xSpecVolumes[i]
		if volume.PersistentVolumeClaim == nil || volume.PersistentVolumeClaim.ClaimName == "" {
			continue
		}
		pvcName := volume.PersistentVolumeClaim.ClaimName
		TmpName := volume.Name
		if newHashMapping[TmpName] != existingPvcHash[pvcName] {
			return true, nil
		}
	}
	return false, nil
}

func (pc *RealPvcControl) RetainPvcWhenXSetDeleted(xset api.XSetObject) bool {
	return pc.pvcAdapter.RetainPvcWhenXSetDeleted(xset)
}

func (pc *RealPvcControl) RetainPvcWhenXSetScaled(xset api.XSetObject) bool {
	return pc.pvcAdapter.RetainPvcWhenXSetScaled(xset)
}

func (pc *RealPvcControl) deleteUnclaimedPvcs(ctx context.Context, xset api.XSetObject, oldPvcs map[string]*corev1.PersistentVolumeClaim, mountedPvcNames sets.String) error {
	inUsedPvcNames := sets.String{}
	templates := pc.pvcAdapter.GetXSetPvcTemplate(xset)
	for i := range templates {
		inUsedPvcNames.Insert(templates[i].Name)
	}
	for pvcTmpName, pvc := range oldPvcs {
		// if pvc is still mounted on target, keep it
		if mountedPvcNames.Has(pvcTmpName) {
			continue
		}

		// is pvc is claimed in pvc templates, keep it
		if inUsedPvcNames.Has(pvcTmpName) {
			continue
		}

		if err := deletePvcWithExpectations(ctx, pc.client, xset, pc.expectations, pvc); err != nil {
			return err
		}
	}
	return nil
}

func (pc *RealPvcControl) deleteOldPvcs(ctx context.Context, xset api.XSetObject, newPvcs, oldPvcs map[string]*corev1.PersistentVolumeClaim) error {
	for pvcTmpName, pvc := range oldPvcs {
		if newPvcExist := newPvcs[pvcTmpName]; newPvcExist != nil {
			continue
		}
		if err := deletePvcWithExpectations(ctx, pc.client, xset, pc.expectations, pvc); err != nil {
			return err
		}
	}
	return nil
}

func (pc *RealPvcControl) buildPvcWithHash(id string, xset api.XSetObject, pvcTmp *corev1.PersistentVolumeClaim) (*corev1.PersistentVolumeClaim, error) {
	claim := pvcTmp.DeepCopy()
	claim.Name = ""
	claim.GenerateName = fmt.Sprintf("%s-%s-", xset.GetName(), pvcTmp.Name)
	claim.Namespace = xset.GetNamespace()
	xsetMeta := pc.xsetController.XSetMeta()
	xsetGvk := xsetMeta.GroupVersionKind()
	claim.OwnerReferences = append(claim.OwnerReferences,
		*metav1.NewControllerRef(xset, xsetGvk))

	if claim.Labels == nil {
		claim.Labels = map[string]string{}
	}
	xsetSpec := pc.xsetController.GetXSetSpec(xset)
	for k, v := range xsetSpec.Selector.MatchLabels {
		claim.Labels[k] = v
	}
	pc.xsetLabelAnnoMgr.Set(claim, api.ControlledByXSetLabel, "true")

	hash, err := PvcTmpHash(pvcTmp)
	if err != nil {
		return nil, err
	}
	pc.xsetLabelAnnoMgr.Set(claim, api.SubResourcePvcTemplateHashLabelKey, hash)
	pc.xsetLabelAnnoMgr.Set(claim, api.XInstanceIdLabelKey, id)
	pc.xsetLabelAnnoMgr.Set(claim, api.SubResourcePvcTemplateLabelKey, pvcTmp.Name)
	return claim, nil
}

// classify pvcs into old and new ones
func (pc *RealPvcControl) classifyTargetPvcs(id string, xset api.XSetObject, existingPvcs []*corev1.PersistentVolumeClaim) (map[string]*corev1.PersistentVolumeClaim, map[string]*corev1.PersistentVolumeClaim, error) {
	newPvcs := map[string]*corev1.PersistentVolumeClaim{}
	oldPvcs := map[string]*corev1.PersistentVolumeClaim{}

	newPvcTemplates := pc.pvcAdapter.GetXSetPvcTemplate(xset)
	newTmpHash, err := PvcTmpHashMapping(newPvcTemplates)
	if err != nil {
		return newPvcs, oldPvcs, err
	}

	for _, pvc := range existingPvcs {
		if pvc.DeletionTimestamp != nil {
			continue
		}

		if pvc.Labels == nil {
			continue
		}

		if val, exist := pc.xsetLabelAnnoMgr.Get(pvc.Labels, api.XInstanceIdLabelKey); !exist {
			continue
		} else if val != id {
			continue
		}

		if _, exist := pc.xsetLabelAnnoMgr.Get(pvc.Labels, api.SubResourcePvcTemplateHashLabelKey); !exist {
			continue
		}
		hash, _ := pc.xsetLabelAnnoMgr.Get(pvc.Labels, api.SubResourcePvcTemplateHashLabelKey)
		pvcTmpName, err := pc.extractPvcTmpName(xset, pvc)
		if err != nil {
			return nil, nil, err
		}

		// classify into updated and old pvcs
		if newTmpHash[pvcTmpName] == hash {
			newPvcs[pvcTmpName] = pvc
		} else {
			oldPvcs[pvcTmpName] = pvc
		}
	}

	return newPvcs, oldPvcs, nil
}

func (pc *RealPvcControl) extractPvcTmpName(xset api.XSetObject, pvc *corev1.PersistentVolumeClaim) (string, error) {
	if pvcTmpName, exist := pc.xsetLabelAnnoMgr.Get(pvc.Labels, api.SubResourcePvcTemplateLabelKey); exist {
		return pvcTmpName, nil
	}
	lastDashIndex := strings.LastIndex(pvc.Name, "-")
	if lastDashIndex == -1 {
		return "", fmt.Errorf("pvc %s has no postfix", pvc.Name)
	}

	rest := pvc.Name[:lastDashIndex]
	if !strings.HasPrefix(rest, xset.GetName()+"-") {
		return "", fmt.Errorf("malformed pvc name %s, expected a part of CollaSet name %s", pvc.Name, xset.GetName())
	}

	return strings.TrimPrefix(rest, xset.GetName()+"-"), nil
}

func PvcTmpHash(pvc *corev1.PersistentVolumeClaim) (string, error) {
	bytes, err := json.Marshal(pvc)
	if err != nil {
		return "", fmt.Errorf("fail to marshal pvc template: %w", err)
	}

	hf := fnv.New32()
	if _, err = hf.Write(bytes); err != nil {
		return "", fmt.Errorf("fail to calculate pvc template hash: %w", err)
	}

	return rand.SafeEncodeString(fmt.Sprint(hf.Sum32())), nil
}

func PvcTmpHashMapping(pvcTmps []corev1.PersistentVolumeClaim) (map[string]string, error) {
	pvcHashMapping := map[string]string{}
	for i := range pvcTmps {
		pvcTmp := pvcTmps[i]
		hash, err := PvcTmpHash(&pvcTmp)
		if err != nil {
			return nil, err
		}
		pvcHashMapping[pvcTmp.Name] = hash
	}
	return pvcHashMapping, nil
}

func deletePvcWithExpectations(ctx context.Context, client client.Client, xset api.XSetObject, expectations *expectations.CacheExpectations, pvc *corev1.PersistentVolumeClaim) error {
	if err := client.Delete(ctx, pvc); err != nil {
		return err
	}

	// expect deletion
	if err := expectations.ExpectDeletion(kubeutilclient.ObjectKeyString(xset), PVCGvk, pvc.GetNamespace(), pvc.GetName()); err != nil {
		return err
	}
	return nil
}

func setUpCache(cache cache.Cache, controller api.XSetController) error {
	if err := cache.IndexField(context.TODO(), &corev1.PersistentVolumeClaim{}, FieldIndexOwnerRefUID, func(object client.Object) []string {
		ownerRef := metav1.GetControllerOf(object)
		if ownerRef == nil || ownerRef.Kind != controller.XSetMeta().Kind {
			return nil
		}
		return []string{string(ownerRef.UID)}
	}); err != nil {
		return fmt.Errorf("failed to index by field for pvc->xset %s: %s", FieldIndexOwnerRefUID, err.Error())
	}
	return nil
}
