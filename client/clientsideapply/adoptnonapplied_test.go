package clientsideapply

import (
	"github.com/stretchr/testify/suite"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/endpoints/handlers/fieldmanager"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

type fakeObjectDefaulter struct{}

func (d *fakeObjectDefaulter) Default(in runtime.Object) {}

func newTestFieldManager(gvk schema.GroupVersionKind, subresource string, chainFieldManager func(fieldmanager.Manager) fieldmanager.Manager) *fieldmanager.FieldManager {
	typeConverter := fieldmanager.DeducedTypeConverter{}
	objectConverter := &unstructuredConvertor{}
	f, err := fieldmanager.NewStructuredMergeManager(
		typeConverter,
		objectConverter,
		&fakeObjectDefaulter{},
		gvk.GroupVersion(),
		gvk.GroupVersion(),
		nil,
	)
	if err != nil {
		panic(err)
	}

	live := &unstructured.Unstructured{}
	live.SetGroupVersionKind(gvk)

	f = fieldmanager.NewCapManagersManager(
		fieldmanager.NewBuildManagerInfoManager(
			fieldmanager.NewManagedFieldsUpdater(
				fieldmanager.NewStripMetaManager(f),
			), gvk.GroupVersion(), subresource,
		), fieldmanager.DefaultMaxUpdateManagers,
	)

	if chainFieldManager != nil {
		f = chainFieldManager(f)
	}

	return fieldmanager.NewFieldManager(f, subresource)
}

type adoptNonAppliedManagerTestSuite struct {
	suite.Suite

	fieldManager *fieldmanager.FieldManager
}

func (s *adoptNonAppliedManagerTestSuite) SetupSuite() {
	s.fieldManager = newTestFieldManager(podGVK, "", func(m fieldmanager.Manager) fieldmanager.Manager {
		return NewAdoptNonAppliedManager(
			m,
			&unstructuredCreater{},
			podGVK,
		)
	})
}

func (s *adoptNonAppliedManagerTestSuite) TestNoUpdateBeforeFirstApply() {
	liveObj := newEmptyUnstructured(podGVK)

	appliedObj := &unstructured.Unstructured{Object: map[string]any{}}
	err := yaml.Unmarshal([]byte(`{
		"apiVersion": "v1",
		"kind": "Pod",
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

	out, err := s.fieldManager.Apply(liveObj, appliedObj, "fieldmanager_test_apply", false)
	s.Require().NoError(err)
	s.Require().Implements((*client.Object)(nil), out)
	obj := out.(client.Object)
	managedFields := obj.GetManagedFields()
	if s.Len(managedFields, 1) {
		s.Equal("fieldmanager_test_apply", managedFields[0].Manager)
	}
}

func (s *adoptNonAppliedManagerTestSuite) TestUpdateBeforeFirstApply() {
	liveObj := newEmptyUnstructured(podGVK)
	liveObj.SetLabels(map[string]string{"app": "nginx"})

	appliedObj := &unstructured.Unstructured{Object: map[string]any{}}
	err := yaml.Unmarshal([]byte(`{
		"apiVersion": "v1",
		"kind": "Pod",
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

	out, err := s.fieldManager.Apply(liveObj, appliedObj, "fieldmanager_test_apply", false)
	s.Require().NoError(err)
	s.Require().Implements((*client.Object)(nil), out)
	obj := out.(client.Object)
	managedFields := obj.GetManagedFields()
	if s.Len(managedFields, 1) {
		s.Equal("fieldmanager_test_apply", managedFields[0].Manager)
	}
}
