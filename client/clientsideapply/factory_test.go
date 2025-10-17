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
	"fmt"

	"github.com/stretchr/testify/suite"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/endpoints/handlers/fieldmanager"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/kube-openapi/pkg/util/proto"
	prototesting "k8s.io/kube-openapi/pkg/util/proto/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

var podGVK = corev1.SchemeGroupVersion.WithKind("Pod")

var kubernetesSwaggerSchema = prototesting.Fake{
	Path: "./testdata/swagger.json",
}

func NewFakeOpenAPIModels() proto.Models {
	d, err := kubernetesSwaggerSchema.OpenAPISchema()
	if err != nil {
		panic(err)
	}
	m, err := proto.NewOpenAPIData(d)
	if err != nil {
		panic(err)
	}
	return m
}

func NewFakeTypeConverter(m proto.Models) fieldmanager.TypeConverter {
	tc, err := fieldmanager.NewTypeConverter(m, false)
	if err != nil {
		panic(fmt.Sprintf("Failed to build TypeConverter: %v", err))
	}
	return tc
}

type fieldManagerFactoryTestSuite struct {
	suite.Suite

	skipNonAppliedFactory  *FieldManagerFactory
	adoptNonAppliedFactory *FieldManagerFactory

	emptyPod, livePod, appliedPod, updatePod *corev1.Pod
}

func (s *fieldManagerFactoryTestSuite) SetupTest() {
	m := NewFakeOpenAPIModels()
	typeConverter := NewFakeTypeConverter(m)
	s.skipNonAppliedFactory = NewFieldManagerFactory(WithTypeConverter(typeConverter))
	s.adoptNonAppliedFactory = NewFieldManagerFactory(
		WithTypeConverter(typeConverter),
		WithAdoptNonApplied(),
		WithScheme(clientgoscheme.Scheme),
	)

	s.emptyPod = &corev1.Pod{}
	s.livePod = &corev1.Pod{}
	s.appliedPod = &corev1.Pod{}
	s.updatePod = &corev1.Pod{}

	s.decodeObject(`
apiVersion: v1
kind: Pod
metadata:
  name: test
  namespace: default
  labels:
    env: live
  annotations:
    key: live
spec:
  schedulerName: default-scheduler
  containers:
  - name: nginx
    image: nginx:live
    env:
    - name: env
      value: live
`, s.livePod)

	s.decodeObject(`
apiVersion: v1
kind: Pod
metadata:
  name: test
  namespace: default
  labels:
    app: nginx
    env: first-apply
spec:
  containers:
  - name: nginx
    image: nginx:first-apply
    env:
    - name: env
      value: first-apply
    - name: app
      value: nginx
`, s.appliedPod)

	s.decodeObject(`
apiVersion: v1
kind: Pod
metadata:
  name: test
  namespace: default
  labels:
    env: update
spec:
  containers:
  - name: nginx
    image: nginx:update
    env:
    - name: env
      value: update
    - name: app
      value: nginx
`, s.updatePod)
}

func (s *fieldManagerFactoryTestSuite) decodeObject(yamlStr string, out any) {
	err := yaml.Unmarshal([]byte(yamlStr), out)
	s.Require().NoError(err)
}

func (s *fieldManagerFactoryTestSuite) getManagedFields(obj metav1.Object) []metav1.ManagedFieldsEntry {
	value := obj.GetAnnotations()[ClientSideApplyManagedFieldsAnnotationKey]
	if len(value) == 0 {
		return nil
	}
	return decodeManagedFields(value)
}

func (s *fieldManagerFactoryTestSuite) deleteManagedFields(obj metav1.Object) {
	annotations := obj.GetAnnotations()
	_, ok := annotations[ClientSideApplyManagedFieldsAnnotationKey]
	if ok {
		delete(annotations, ClientSideApplyManagedFieldsAnnotationKey)
		if len(annotations) == 0 {
			annotations = nil
		}
		obj.SetAnnotations(annotations)
	}
}

func (s *fieldManagerFactoryTestSuite) objectEqualsIgnoreManagedFields(expected, actual client.Object) {
	s.deleteManagedFields(expected)
	s.deleteManagedFields(actual)
	s.Equal(expected, actual)
}

func (s *fieldManagerFactoryTestSuite) TestFirstApply_SkipNonApplied() {
	fm, err := s.skipNonAppliedFactory.NewFieldManager(podGVK, "")
	s.Require().NoError(err)
	// load from cache
	cacheFm, err := s.skipNonAppliedFactory.NewFieldManager(podGVK, "")
	s.Require().NoError(err)
	s.Equal(fm, cacheFm)

	managerName := "skip_non_applied_field_manager"

	// simulate creating new pod
	obj, err := fm.Apply(s.emptyPod, s.livePod, managerName, false)
	s.Require().NoError(err)
	fields := s.getManagedFields(obj)
	if s.Len(fields, 1) {
		s.Equal(managerName, fields[0].Manager)
	}
	s.objectEqualsIgnoreManagedFields(s.livePod, obj)

	// simulate appling to existing pod, force = false
	_, err = fm.Apply(s.livePod, s.appliedPod, managerName, false)
	s.Require().Error(err)
	s.True(apierrors.IsConflict(err), "should be conflict")
	if s.Implements((*apierrors.APIStatus)(nil), err) {
		apiStatus, _ := err.(apierrors.APIStatus)
		if s.Len(apiStatus.Status().Details.Causes, 3) {
			s.Equal(`.metadata.labels.env`, apiStatus.Status().Details.Causes[0].Field)
			s.Equal(`.spec.containers[name="nginx"].image`, apiStatus.Status().Details.Causes[1].Field)
			s.Equal(`.spec.containers[name="nginx"].env[name="env"].value`, apiStatus.Status().Details.Causes[2].Field)
		}
	}

	// simulate appling to existing pod, force = true
	liveObj, err := fm.Apply(s.livePod, s.appliedPod, managerName, true)
	s.Require().NoError(err)
	fields = s.getManagedFields(liveObj)
	if s.Len(fields, 2) {
		s.Equal(managerName, fields[0].Manager)
		s.EqualValues("Apply", fields[0].Operation)
		s.Equal("before-first-apply", fields[1].Manager)
		s.EqualValues("Update", fields[1].Operation)
	}

	expectedObj := &corev1.Pod{}
	s.decodeObject(`
apiVersion: v1
kind: Pod
metadata:
  name: test
  namespace: default
  labels:
    app: nginx
    env: first-apply
  annotations:
    key: live
spec:
  schedulerName: default-scheduler
  containers:
  - name: nginx
    image: nginx:first-apply
    env:
    - name: env
      value: first-apply
    - name: app
      value: nginx
`, expectedObj)

	s.objectEqualsIgnoreManagedFields(expectedObj, liveObj.DeepCopyObject().(client.Object))
}

func (s *fieldManagerFactoryTestSuite) TestFirstApply_AdoptNonApplied() {
	fm, err := s.adoptNonAppliedFactory.NewFieldManager(podGVK, "")
	s.Require().NoError(err)

	managerName := "adopt_non_applied_field_manager"

	// simulate creating new pod
	obj, err := fm.Apply(s.emptyPod, s.livePod, managerName, false)
	s.Require().NoError(err)
	fields := s.getManagedFields(obj)
	if s.Len(fields, 1) {
		s.Equal(managerName, fields[0].Manager)
	}
	s.objectEqualsIgnoreManagedFields(s.livePod, obj)

	// simulate appling to existing pod
	obj, err = fm.Apply(s.livePod, s.appliedPod, managerName, false)
	s.Require().NoError(err)
	fields = s.getManagedFields(obj)
	if s.Len(fields, 1) {
		s.Equal(managerName, fields[0].Manager)
	}
	s.objectEqualsIgnoreManagedFields(s.appliedPod, obj)
}

func (s *fieldManagerFactoryTestSuite) TestUnstructured_SkipNonApplied() {
	fmf := NewFieldManagerFactory()
	gvk := schema.FromAPIVersionAndKind("client-side-apply/v1", "TestKind")

	managerName := "test_field_manager"

	fm, err := fmf.NewFieldManager(gvk, "")
	s.Require().NoError(err)

	emptyObj := newEmptyUnstructured(gvk)

	liveObj := newEmptyUnstructured(gvk)
	s.decodeObject(`
apiVersion: client-side-apply/v1
kind: TestKind
metadata:
  name: test
  namespace: default
  labels:
    env: live
  annotations:
    key: live
spec:
  schedulerName: default-scheduler
  containers:
  - name: nginx
    image: nginx:live
    env:
    - name: env
      value: live
`, &liveObj.Object)

	obj, err := fm.Apply(emptyObj, liveObj, managerName, false)
	s.Require().NoError(err)
	fields := s.getManagedFields(obj)
	if s.Len(fields, 1) {
		s.Equal(managerName, fields[0].Manager)
	}
	s.objectEqualsIgnoreManagedFields(liveObj, obj)

	appliedObj := newEmptyUnstructured(gvk)
	s.decodeObject(`
apiVersion: client-side-apply/v1
kind: TestKind
metadata:
  name: test
  namespace: default
  labels:
    app: nginx
    env: first-apply
spec:
  containers:
  - name: nginx
    image: nginx:applied
    env:
    - name: env
      value: first-apply
    - name: app
      value: nginx
`, &appliedObj.Object)

	obj, err = fm.Apply(liveObj, appliedObj, managerName, true)
	s.Require().NoError(err)

	fields = s.getManagedFields(obj)
	if s.Len(fields, 2) {
		s.Equal(managerName, fields[0].Manager)
		s.EqualValues("Apply", fields[0].Operation)
		s.Equal("before-first-apply", fields[1].Manager)
		s.EqualValues("Update", fields[1].Operation)
	}

	expectedObj := newEmptyUnstructured(gvk)
	s.decodeObject(`
apiVersion: client-side-apply/v1
kind: TestKind
metadata:
  name: test
  namespace: default
  labels:
    app: nginx
    env: first-apply
  annotations:
    key: live
spec:
  schedulerName: default-scheduler
  containers:
  - name: nginx
    image: nginx:applied
    env:
    - name: env
      value: first-apply
    - name: app
      value: nginx
`, &expectedObj.Object)

	s.objectEqualsIgnoreManagedFields(expectedObj, obj)
}
