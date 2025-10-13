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

	f.decodeManagedFieldsFromAnnotation(liveObj)
	obj, err := f.fieldmanager.Apply(liveObj, appliedObj, fieldManager, force)
	if err != nil {
		return nil, err
	}
	clientObj := obj.(client.Object)
	f.encodeManagedFieldsFromAnnotation(clientObj)
	return clientObj, nil
}

func (f *clientApplyFieldManager) decodeManagedFieldsFromAnnotation(obj client.Object) {
	if len(obj.GetManagedFields()) > 0 {
		// managedFields already exist, skip
		return
	}
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
		// managedFields already exist, skip
		return
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
