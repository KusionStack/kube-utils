/*
Copyright 2025 The KusionStack Authors.

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

package client

import (
	"context"
	"encoding/json"
	"fmt"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/stretchr/testify/suite"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func newTestPod() *corev1.Pod {
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			Labels: map[string]string{
				"version": "v1",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "test-container",
					Image: "nginx:latest",
				},
			},
		},
	}
}

func newTestRandomPod() *corev1.Pod {
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("test-pod-%s", rand.String(5)),
			Namespace: "default",
			Labels: map[string]string{
				"version": "v1",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "test-container",
					Image: "nginx:latest",
				},
			},
		},
	}
}

type fakeClient struct {
	client.Client
}

func (c *fakeClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	existingObj := obj.DeepCopyObject().(client.Object)
	err := c.Client.Get(ctx, client.ObjectKeyFromObject(existingObj), existingObj)
	if err != nil {
		return err
	}

	existingJSON, err := json.Marshal(existingObj)
	if err != nil {
		return err
	}

	modifiedObj := existingObj.DeepCopyObject().(client.Object)

	patchData, err := patch.Data(obj)
	if err != nil {
		return err
	}
	switch patch.Type() {
	case types.JSONPatchType:
		patch, err := jsonpatch.DecodePatch(patchData)
		if err != nil {
			return err
		}
		modified, err := patch.Apply(existingJSON)
		if err != nil {
			return err
		}

		if err := json.Unmarshal(modified, modifiedObj); err != nil {
			return err
		}
	case types.MergePatchType:
		modified, err := jsonpatch.MergePatch(existingJSON, patchData)
		if err != nil {
			return err
		}

		if err := json.Unmarshal(modified, modifiedObj); err != nil {
			return err
		}
	case types.StrategicMergePatchType:
		mergedByte, err := strategicpatch.StrategicMergePatch(existingJSON, patchData, obj)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(mergedByte, modifiedObj); err != nil {
			return err
		}
	default:
		return fmt.Errorf("PatchType is not supported")
	}

	if existingObj.GetResourceVersion() != modifiedObj.GetResourceVersion() {
		return errors.NewConflict(schema.GroupResource{}, obj.GetName(), fmt.Errorf("resource version conflict"))
	}

	return c.Client.Patch(ctx, obj, patch, opts...)
}

type clientTestSuite struct {
	suite.Suite

	cli client.Client
}

func (s *clientTestSuite) SetupSuite() {
	s.cli = &fakeClient{Client: fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()}
}

func (s *clientTestSuite) TearDownTest() {
	s.cli.DeleteAllOf(context.Background(), &corev1.Pod{})
}

func (s *clientTestSuite) TestConflict() {
	pod := newTestPod()
	err := s.cli.Create(context.Background(), pod)
	s.Require().NoError(err)

	oldPod := pod.DeepCopy()

	pod.Labels["version"] = "v2"
	err = s.cli.Update(context.Background(), pod)
	s.Require().NoError(err, "bump up resource version")

	modified := oldPod.DeepCopy()
	modified.Labels["version"] = "v3"
	err = s.cli.Update(context.Background(), modified)
	s.True(errors.IsConflict(err), "update should fail on conflict")

	modified = oldPod.DeepCopy()
	modified.Labels["version"] = "v4"
	mergePatch := client.MergeFromWithOptions(oldPod, client.MergeFromWithOptimisticLock{})
	err = s.cli.Patch(context.Background(), modified, mergePatch)
	s.True(errors.IsConflict(err), "patch should fail on conflict")
}

func (s *clientTestSuite) TestUpdateOnConflict() {
	type testCase struct {
		name           string
		setupFunc      func() *corev1.Pod
		modifyFunc     func(obj *corev1.Pod) error
		expectedLabels map[string]string
		expectedChange bool
		expectedError  bool
	}

	testCases := []testCase{
		{
			name: "successful update on conflict",
			setupFunc: func() *corev1.Pod {
				pod := newTestRandomPod()
				err := s.cli.Create(context.Background(), pod)
				s.Require().NoError(err)

				oldPod := pod.DeepCopy()
				// update pod to increase resource version
				pod.Labels["version"] = "v2"
				err = s.cli.Update(context.Background(), pod)
				s.Require().NoError(err)

				// return old pod to trigger update on conflict
				return oldPod
			},
			modifyFunc: func(obj *corev1.Pod) error {
				obj.Labels["version"] = "v3"
				return nil
			},
			expectedLabels: map[string]string{"version": "v3"},
			expectedChange: true,
			expectedError:  false,
		},
		{
			name: "no change",
			setupFunc: func() *corev1.Pod {
				pod := newTestRandomPod()
				err := s.cli.Create(context.Background(), pod)
				s.Require().NoError(err)
				return pod
			},
			modifyFunc: func(obj *corev1.Pod) error {
				return nil
			},
			expectedLabels: map[string]string{"version": "v1"},
			expectedChange: false,
			expectedError:  false,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			modified := tc.setupFunc()
			changed, err := UpdateOnConflict(context.Background(), s.cli, s.cli, modified, tc.modifyFunc)

			if tc.expectedError {
				s.Require().Error(err)
			} else {
				s.Require().NoError(err)
			}

			s.Equal(tc.expectedChange, changed)

			for key, expectedValue := range tc.expectedLabels {
				s.Equal(expectedValue, modified.Labels[key])
			}

			if !tc.expectedError {
				existing := &corev1.Pod{}
				err = s.cli.Get(context.Background(), client.ObjectKeyFromObject(modified), existing)
				s.Require().NoError(err)
				s.Equal(existing, modified)
			}
		})
	}
}

func (s *clientTestSuite) TestCreateOrUpdateOnConflict() {
	type testCase struct {
		name           string
		setupFunc      func() *corev1.Pod
		modifyFunc     func(obj *corev1.Pod) error
		expectedResult controllerutil.OperationResult
		expectedLabels map[string]string
		expectedError  bool
	}

	testCases := []testCase{
		{
			name: "create new pod",
			setupFunc: func() *corev1.Pod {
				return newTestRandomPod()
			},
			modifyFunc: func(obj *corev1.Pod) error {
				obj.Labels["version"] = "v1"
				return nil
			},
			expectedResult: controllerutil.OperationResultCreated,
			expectedLabels: map[string]string{"version": "v1"},
			expectedError:  false,
		},
		{
			name: "no change on existing pod",
			setupFunc: func() *corev1.Pod {
				pod := newTestRandomPod()
				err := s.cli.Create(context.Background(), pod)
				s.Require().NoError(err)
				return pod
			},
			modifyFunc: func(obj *corev1.Pod) error {
				return nil
			},
			expectedResult: controllerutil.OperationResultNone,
			expectedLabels: map[string]string{"version": "v1"},
			expectedError:  false,
		},
		{
			name: "update existing pod",
			setupFunc: func() *corev1.Pod {
				pod := newTestRandomPod()
				err := s.cli.Create(context.Background(), pod)
				s.Require().NoError(err)

				oldPod := pod.DeepCopy()
				// update pod to increase resource version
				pod.Labels["version"] = "v2"
				err = s.cli.Update(context.Background(), pod)
				s.Require().NoError(err)

				// return old pod to trigger update on conflict
				return oldPod
			},
			modifyFunc: func(obj *corev1.Pod) error {
				obj.Labels["version"] = "v3"
				return nil
			},
			expectedResult: controllerutil.OperationResultUpdated,
			expectedLabels: map[string]string{"version": "v3"},
			expectedError:  false,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			modified := tc.setupFunc()
			result, err := CreateOrUpdateOnConflict(context.Background(), s.cli, s.cli, modified, tc.modifyFunc)

			if tc.expectedError {
				s.Require().Error(err)
			} else {
				s.Require().NoError(err)
			}

			s.Equal(tc.expectedResult, result)

			for key, expectedValue := range tc.expectedLabels {
				s.Equal(expectedValue, modified.Labels[key])
			}

			if !tc.expectedError {
				existing := &corev1.Pod{}
				err = s.cli.Get(context.Background(), client.ObjectKeyFromObject(modified), existing)
				s.Require().NoError(err)
				s.Equal(existing, modified)
			}
		})
	}
}

func (s *clientTestSuite) TestPatch() {
	type testCase struct {
		name           string
		setupFunc      func() *corev1.Pod
		modifyFunc     func(obj *corev1.Pod) error
		expectedLabels map[string]string
		expectedChange bool
		expectedError  bool
	}

	pod := newTestPod()
	err := s.cli.Create(context.Background(), pod)
	s.Require().NoError(err)

	testCases := []testCase{
		{
			name: "patch on current pod",
			setupFunc: func() *corev1.Pod {
				pod := newTestRandomPod()
				err := s.cli.Create(context.Background(), pod)
				s.Require().NoError(err)
				return pod
			},
			modifyFunc: func(obj *corev1.Pod) error {
				obj.Labels["version"] = "v2"
				return nil
			},
			expectedLabels: map[string]string{"version": "v2"},
			expectedChange: true,
			expectedError:  false,
		},
		{
			name: "patch on old pod",
			setupFunc: func() *corev1.Pod {
				pod := newTestRandomPod()
				err := s.cli.Create(context.Background(), pod)
				s.Require().NoError(err)

				oldPod := pod.DeepCopy()
				// update pod to increase resource version
				pod.Labels["version"] = "v2"
				err = s.cli.Update(context.Background(), pod)
				s.Require().NoError(err)

				// return old pod to trigger update on conflict
				return oldPod
			},
			modifyFunc: func(obj *corev1.Pod) error {
				obj.Labels["version"] = "v3"
				return nil
			},
			expectedLabels: map[string]string{"version": "v3"},
			expectedChange: true,
			expectedError:  false,
		},
		{
			name: "patch with no changes",
			setupFunc: func() *corev1.Pod {
				pod := newTestRandomPod()
				err := s.cli.Create(context.Background(), pod)
				s.Require().NoError(err)
				return pod
			},
			modifyFunc: func(obj *corev1.Pod) error {
				return nil
			},
			expectedLabels: map[string]string{"version": "v1"},
			expectedChange: false,
			expectedError:  false,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			modified := tc.setupFunc()
			changed, err := Patch(context.Background(), s.cli, modified, tc.modifyFunc)

			if tc.expectedError {
				s.Require().Error(err)
			} else {
				s.Require().NoError(err)
			}

			s.Equal(tc.expectedChange, changed)

			for key, expectedValue := range tc.expectedLabels {
				s.Equal(expectedValue, modified.Labels[key])
			}

			if !tc.expectedError {
				existing := &corev1.Pod{}
				err = s.cli.Get(context.Background(), client.ObjectKeyFromObject(modified), existing)
				s.Require().NoError(err)
				s.Equal(existing, modified)
			}
		})
	}
}

func (s *clientTestSuite) TestPatchOnConflict() {
	type testCase struct {
		name           string
		setupFunc      func() *corev1.Pod
		modifyFunc     func(obj *corev1.Pod) error
		expectedLabels map[string]string
		expectedChange bool
		expectedError  bool
	}

	testCases := []testCase{
		{
			name: "patch on current pod",
			setupFunc: func() *corev1.Pod {
				pod := newTestRandomPod()
				err := s.cli.Create(context.Background(), pod)
				s.Require().NoError(err)
				return pod
			},
			modifyFunc: func(obj *corev1.Pod) error {
				obj.Labels["version"] = "v2"
				return nil
			},
			expectedLabels: map[string]string{"version": "v2"},
			expectedChange: true,
			expectedError:  false,
		},
		{
			name: "patch on old pod",
			setupFunc: func() *corev1.Pod {
				pod := newTestRandomPod()
				err := s.cli.Create(context.Background(), pod)
				s.Require().NoError(err)

				oldPod := pod.DeepCopy()
				// update pod to increase resource version
				pod.Labels["version"] = "v2"
				err = s.cli.Update(context.Background(), pod)
				s.Require().NoError(err)

				// return old pod to trigger update on conflict
				return oldPod
			},
			modifyFunc: func(obj *corev1.Pod) error {
				obj.Labels["version"] = "v3"
				return nil
			},
			expectedLabels: map[string]string{"version": "v3"},
			expectedChange: true,
			expectedError:  false,
		},
		{
			name: "patch with no changes",
			setupFunc: func() *corev1.Pod {
				pod := newTestRandomPod()
				err := s.cli.Create(context.Background(), pod)
				s.Require().NoError(err)
				return pod
			},
			modifyFunc: func(obj *corev1.Pod) error {
				return nil
			},
			expectedLabels: map[string]string{"version": "v1"},
			expectedChange: false,
			expectedError:  false,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			modified := tc.setupFunc()
			changed, err := PatchOnConflict(context.Background(), s.cli, s.cli, modified, tc.modifyFunc)

			if tc.expectedError {
				s.Require().Error(err)
			} else {
				s.Require().NoError(err)
			}

			s.Equal(tc.expectedChange, changed)

			for key, expectedValue := range tc.expectedLabels {
				s.Equal(expectedValue, modified.Labels[key])
			}

			if !tc.expectedError {
				existing := &corev1.Pod{}
				err = s.cli.Get(context.Background(), client.ObjectKeyFromObject(modified), existing)
				s.Require().NoError(err)
				s.Equal(existing, modified)
			}
		})
	}
}
