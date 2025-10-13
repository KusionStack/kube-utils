package clientsideapply

import (
	"github.com/stretchr/testify/suite"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/endpoints/handlers/fieldmanager"
	"sigs.k8s.io/yaml"
)

type clientApplyFieldManagerTestSuite struct {
	suite.Suite

	fieldManager *clientApplyFieldManager
}

func (s *clientApplyFieldManagerTestSuite) SetupSuite() {
	fm := newTestFieldManager(corev1.SchemeGroupVersion.WithKind("Pod"), "", func(m fieldmanager.Manager) fieldmanager.Manager {
		return NewAdoptNonAppliedManager(
			m,
			&unstructuredCreater{},
			corev1.SchemeGroupVersion.WithKind("Pod"),
		)
	})
	s.fieldManager = &clientApplyFieldManager{
		fieldmanager: fm,
		gvk:          corev1.SchemeGroupVersion.WithKind("Pod"),
	}
}

func newEmptyUnstructured(gvk schema.GroupVersionKind) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{
		Object: map[string]any{},
	}
	obj.SetGroupVersionKind(gvk)
	return obj
}

func (s *clientApplyFieldManagerTestSuite) TestApply() {
	// create pod
	liveObj := &unstructured.Unstructured{}

	appliedObj := &unstructured.Unstructured{Object: map[string]any{}}
	err := yaml.Unmarshal([]byte(`{
		"metadata": {
			"name": "pod",
			"labels": {"app": "nginx"}
		},
		"spec": {
			"containers": [{
				"name":  "nginx",
				"image": "nginx:latest"
			}]
        }
	}`), &appliedObj.Object)
	s.Require().NoError(err)

	out, err := s.fieldManager.Apply(liveObj, appliedObj, "clientapply_fieldmanager_test", false)
	s.Require().NoError(err)
	s.IsType(&unstructured.Unstructured{}, out)
	s.Len(out.GetManagedFields(), 0)
	s.Len(out.GetAnnotations(), 1)
	s.Contains(out.GetAnnotations(), ClientSideApplyManagedFieldsAnnotationKey)

	liveObj = out.(*unstructured.Unstructured)
	appliedObj = &unstructured.Unstructured{Object: map[string]any{}}
	err = yaml.Unmarshal([]byte(`{
		"metadata": {
			"name": "pod",
			"labels": {"app": "nginx", "test": "test"}
		},
		"spec": {
			"containers": [{
				"name":  "nginx",
				"image": "nginx:test"
			}]
        }
	}`), &appliedObj.Object)
	s.Require().NoError(err)

	out, err = s.fieldManager.Apply(liveObj, appliedObj, "clientapply_fieldmanager_test", false)
	s.Require().NoError(err)
	s.IsType(&unstructured.Unstructured{}, out)
	s.Len(out.GetManagedFields(), 0)
	s.Len(out.GetAnnotations(), 1)
	s.Contains(out.GetAnnotations(), ClientSideApplyManagedFieldsAnnotationKey)
}
