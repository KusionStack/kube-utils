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

package revisionadapter

import (
	"context"
	"encoding/json"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"kusionstack.io/kube-utils/xset/api"
	"kusionstack.io/kube-utils/xset/xcontrol"
)

type RevisionOwnerAdapter struct {
	api.XSetController

	xcontrol.TargetControl
}

func NewRevisionOwnerAdapter(xsetController api.XSetController, xcontrol xcontrol.TargetControl) *RevisionOwnerAdapter {
	return &RevisionOwnerAdapter{
		XSetController: xsetController,
		TargetControl:  xcontrol,
	}
}

func (r *RevisionOwnerAdapter) GetSelector(obj metav1.Object) *metav1.LabelSelector {
	xset := obj.(api.XSetObject)
	return r.XSetController.GetXSetSpec(xset).Selector
}

func (r *RevisionOwnerAdapter) GetCollisionCount(obj metav1.Object) *int32 {
	xset := obj.(api.XSetObject)
	return r.XSetController.GetXSetStatus(xset).CollisionCount
}

func (r *RevisionOwnerAdapter) GetHistoryLimit(obj metav1.Object) int32 {
	xset := obj.(api.XSetObject)
	return r.XSetController.GetXSetSpec(xset).HistoryLimit
}

func (r *RevisionOwnerAdapter) GetPatch(obj metav1.Object) ([]byte, error) {
	return r.getXSetPatch(obj)
}

func (r *RevisionOwnerAdapter) GetCurrentRevision(obj metav1.Object) string {
	xset := obj.(api.XSetObject)
	return r.XSetController.GetXSetStatus(xset).CurrentRevision
}

func (r *RevisionOwnerAdapter) IsInUsed(obj metav1.Object, revision string) bool {
	xSetObject, _ := obj.(api.XSetObject)
	spec := r.XSetController.GetXSetSpec(xSetObject)
	status := r.XSetController.GetXSetStatus(xSetObject)

	if status.UpdatedRevision == revision || status.CurrentRevision == revision {
		return true
	}

	targets, _ := r.TargetControl.GetFilteredTargets(context.TODO(), spec.Selector, xSetObject)
	for _, target := range targets {
		if target.GetLabels() != nil {
			currentRevisionName, exist := target.GetLabels()[appsv1.ControllerRevisionHashLabelKey]
			if exist && currentRevisionName == revision {
				return true
			}
		}
	}

	return false
}

func (r *RevisionOwnerAdapter) getXSetPatch(obj metav1.Object) ([]byte, error) {
	xset := obj.(api.XSetObject)
	xTpl := r.XSetController.GetXTemplate(xset)
	tplBytes, err := json.Marshal(xTpl)
	if err != nil {
		return nil, err
	}
	tplMap := make(map[string]interface{})
	err = json.Unmarshal(tplBytes, &tplMap)
	if err != nil {
		return nil, err
	}

	tplMap["$patch"] = "replace"
	specMap := make(map[string]interface{})
	specMap["template"] = tplMap
	objMap := make(map[string]interface{})
	objMap["spec"] = specMap

	patch, err := json.Marshal(objMap)
	return patch, err
}
