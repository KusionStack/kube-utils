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

package revisionowner

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"

	"kusionstack.io/kube-utils/controller/history"
	"kusionstack.io/kube-utils/xset/api"
	"kusionstack.io/kube-utils/xset/xcontrol"
)

var _ history.RevisionOwner = &revisionOwner{}

type revisionOwner struct {
	api.XSetController

	xcontrol.TargetControl
}

func NewRevisionOwner(xsetController api.XSetController, xcontrol xcontrol.TargetControl) *revisionOwner {
	return &revisionOwner{
		XSetController: xsetController,
		TargetControl:  xcontrol,
	}
}

func (r *revisionOwner) GetGroupVersionKind() schema.GroupVersionKind {
	meta := r.XSetController.XSetMeta()
	return meta.GroupVersionKind()
}

func (r *revisionOwner) GetMatchLabels(parent metav1.Object) map[string]string {
	obj := parent.(api.XSetObject)
	xsetSpec := r.XSetController.GetXSetSpec(obj)
	// that's ok to return nil, manager will use nothingSelector instead
	return xsetSpec.Selector.MatchLabels
}

func (r *revisionOwner) GetInUsedRevisions(parent metav1.Object) (sets.String, error) {
	xSetObject, _ := parent.(api.XSetObject)
	spec := r.XSetController.GetXSetSpec(xSetObject)
	status := r.XSetController.GetXSetStatus(xSetObject)

	res := sets.String{}

	res.Insert(status.UpdatedRevision)
	res.Insert(status.CurrentRevision)

	targets, _, err := r.TargetControl.GetFilteredTargets(context.TODO(), spec.Selector, xSetObject)
	if err != nil {
		return nil, err
	}
	for _, target := range targets {
		if target.GetLabels() != nil {
			currentRevisionName, exist := target.GetLabels()[appsv1.ControllerRevisionHashLabelKey]
			if exist {
				res.Insert(currentRevisionName)
			}
		}
	}
	return res, nil
}

func (r *revisionOwner) GetCollisionCount(obj metav1.Object) *int32 {
	xset := obj.(api.XSetObject)
	return r.XSetController.GetXSetStatus(xset).CollisionCount
}

func (r *revisionOwner) GetHistoryLimit(obj metav1.Object) int32 {
	xset := obj.(api.XSetObject)
	return r.XSetController.GetXSetSpec(xset).HistoryLimit
}

func (r *revisionOwner) GetPatch(obj metav1.Object) ([]byte, error) {
	return r.getXSetPatch(obj)
}

func (r *revisionOwner) GetCurrentRevision(obj metav1.Object) string {
	xset := obj.(api.XSetObject)
	return r.XSetController.GetXSetStatus(xset).CurrentRevision
}

func (r *revisionOwner) getXSetPatch(obj metav1.Object) ([]byte, error) {
	return r.XSetController.GetXSetPatch(obj)
}
