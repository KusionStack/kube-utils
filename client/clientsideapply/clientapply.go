// Copyright 2025 The KusionStack Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package clientsideapply

import (
	"encoding/json"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/endpoints/handlers/fieldmanager"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ClientSideApplyManagedFieldsAnnotationKey = "meta.k8s.io/client-side-apply-managed-fields"
)

type FieldManager interface {
	Apply(liveObj, appliedObj client.Object, fieldManager string, force bool) (client.Object, error)
}

type clientApplyFieldManager struct {
	fieldmanager *fieldmanager.FieldManager
	gvk          schema.GroupVersionKind
}

var _ FieldManager = &clientApplyFieldManager{}

func (f *clientApplyFieldManager) Apply(liveObj, appliedObj client.Object, fieldManager string, force bool) (client.Object, error) {
	if liveObj.GetObjectKind().GroupVersionKind().Empty() {
		liveObj.GetObjectKind().SetGroupVersionKind(f.gvk)
	}
	if appliedObj.GetObjectKind().GroupVersionKind().Empty() {
		appliedObj.GetObjectKind().SetGroupVersionKind(f.gvk)
	}

	// we need to store server-side managed fields and clean managedFields in liveObj to let client-side apply work
	originManagedFields := liveObj.GetManagedFields()
	if len(originManagedFields) > 0 {
		liveObj.SetManagedFields(nil)
	}

	f.decodeManagedFieldsFromAnnotation(liveObj)
	obj, err := f.fieldmanager.Apply(liveObj, appliedObj, fieldManager, force)
	if err != nil {
		return nil, err
	}
	clientObj := obj.(client.Object)
	f.encodeManagedFieldsFromAnnotation(clientObj)

	// restore server-side managed fields
	if len(originManagedFields) > 0 {
		clientObj.SetManagedFields(originManagedFields)
	}
	return clientObj, nil
}

func (f *clientApplyFieldManager) decodeManagedFieldsFromAnnotation(obj client.Object) {
	// get managed fields from annotation
	annotations := obj.GetAnnotations()
	if len(annotations) == 0 {
		return
	}
	mf := annotations[ClientSideApplyManagedFieldsAnnotationKey]
	if len(mf) == 0 {
		return
	}

	fields := decodeManagedFields(mf)
	if len(fields) == 0 {
		return
	}
	obj.SetManagedFields(fields)
}

func (f *clientApplyFieldManager) encodeManagedFieldsFromAnnotation(obj client.Object) {
	managedFields := obj.GetManagedFields()
	if len(managedFields) == 0 {
		// managedFields is empty, skip
		return
	}

	// Note: Time should always be empty if Operation is 'Apply'
	for i, fields := range managedFields {
		if fields.Operation == metav1.ManagedFieldsOperationApply {
			managedFields[i].Time = nil
		}
	}

	fieldsValue := encodeManagedFields(managedFields)
	// get managed fields from annotation
	annotations := obj.GetAnnotations()
	if len(annotations) == 0 {
		annotations = make(map[string]string)
	}
	annotations[ClientSideApplyManagedFieldsAnnotationKey] = fieldsValue
	obj.SetAnnotations(annotations)
	obj.SetManagedFields(nil)
}

func encodeManagedFields(managedFields []metav1.ManagedFieldsEntry) string {
	meta := &struct {
		ManagedFields []metav1.ManagedFieldsEntry `json:"managedFields"`
	}{
		ManagedFields: managedFields,
	}
	mfStr, _ := json.Marshal(meta)
	return string(mfStr)
}

func decodeManagedFields(value string) []metav1.ManagedFieldsEntry {
	meta := &struct {
		ManagedFields []metav1.ManagedFieldsEntry `json:"managedFields"`
	}{}
	err := json.Unmarshal([]byte(value), meta)
	if err != nil {
		return nil
	}
	return meta.ManagedFields
}
