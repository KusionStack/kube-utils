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
	"github.com/stretchr/testify/suite"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type mapsTestSuite struct {
	suite.Suite
}

func (s *mapsTestSuite) TestMutateLabelsAndAnnotations() {
	type testCase struct {
		name                string
		setupFunc           func() client.Object
		mutateLabels        func(labels map[string]string)
		expectedLabels      map[string]string
		mutateAnnotations   func(annotations map[string]string)
		expectedAnnotations map[string]string
	}

	testCases := []testCase{
		{
			name: "mutate pod labels and annotations",
			setupFunc: func() client.Object {
				return newTestRandomPod()
			},
			mutateLabels: func(labels map[string]string) {
				labels["mutate-labels"] = "test"
			},
			expectedLabels: map[string]string{
				"version":       "v1",
				"mutate-labels": "test",
			},
			mutateAnnotations: func(annotations map[string]string) {
				annotations["mutate-annotations"] = "test"
			},
			expectedAnnotations: map[string]string{
				"mutate-annotations": "test",
			},
		},
		{
			name: "mutate unstructured labels and annotations",
			setupFunc: func() client.Object {
				return &unstructured.Unstructured{Object: map[string]any{}}
			},
			mutateLabels: func(labels map[string]string) {
				labels["mutate-labels"] = "test"
			},
			expectedLabels: map[string]string{
				"mutate-labels": "test",
			},
			mutateAnnotations: func(annotations map[string]string) {
				annotations["mutate-annotations"] = "test"
			},
			expectedAnnotations: map[string]string{
				"mutate-annotations": "test",
			},
		},
		{
			name: "keep nil annotation",
			setupFunc: func() client.Object {
				return newTestRandomPod()
			},
			mutateAnnotations: func(annotations map[string]string) {
			},
			expectedAnnotations: nil,
		},
		{
			name: "keep empty labels",
			setupFunc: func() client.Object {
				return newTestRandomPod()
			},
			mutateLabels: func(labels map[string]string) {
				delete(labels, "version")
			},
			expectedLabels: map[string]string{},
		},
	}
	for i := range testCases {
		tc := testCases[i]
		s.Run(tc.name, func() {
			obj := tc.setupFunc()

			if tc.mutateLabels != nil {
				MutateLabels(obj, tc.mutateLabels)
				s.Equal(tc.expectedLabels, obj.GetLabels())
			}

			if tc.mutateAnnotations != nil {
				MutateAnnotations(obj, tc.mutateAnnotations)
				s.Equal(tc.expectedAnnotations, obj.GetAnnotations())
			}
		})
	}
}

func (s *mapsTestSuite) TestGetMapValueByDefault() {
	type testCase struct {
		name     string
		m        map[string]string
		key      string
		defValue string
		expected string
	}

	testCases := []testCase{
		{
			name:     "get value by key",
			m:        map[string]string{"key": "value"},
			key:      "key",
			expected: "value",
		},
		{
			name:     "get value by default",
			m:        map[string]string{},
			key:      "key",
			defValue: "default",
			expected: "default",
		},
		{
			name:     "get value from nil map",
			m:        nil,
			key:      "key",
			defValue: "default",
			expected: "default",
		},
	}
	for i := range testCases {
		tc := testCases[i]
		s.Run(tc.name, func() {
			s.Equal(tc.expected, GetMapValueByDefault(tc.m, tc.key, tc.defValue))
		})
	}
}
