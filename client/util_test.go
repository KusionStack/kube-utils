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
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func newTestPod() *corev1.Pod {
	return &corev1.Pod{
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

func TestUpdateOnConflict(t *testing.T) {
	client := fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()

	pod := newTestPod()
	err := client.Create(context.Background(), pod)
	assert.NoError(t, err)

	oldPod := pod.DeepCopy()

	pod.Labels["version"] = "v2"
	err = client.Update(context.Background(), pod)
	assert.NoError(t, err)

	oldPod.Labels["version"] = "v3"
	err = client.Update(context.Background(), oldPod)
	assert.True(t, errors.IsConflict(err), "update should fail on conflict")

	// reset old pod and update it again
	oldPod.Labels["version"] = "v1"
	pod = oldPod.DeepCopy()
	changed, err := UpdateOnConflict(context.Background(), client, client, pod, func(obj *corev1.Pod) error {
		obj.Labels["version"] = "v3"
		return nil
	})
	assert.NoError(t, err, "update should succeed on conflict")
	assert.True(t, changed, "pod should be changed")
	assert.Equal(t, "v3", pod.Labels["version"])

	changed, err = UpdateOnConflict(context.Background(), client, client, pod, func(obj *corev1.Pod) error {
		obj.Labels["version"] = "v3"
		return nil
	})
	assert.NoError(t, err, "update should succeed")
	assert.False(t, changed, "pod should not be changed")
	assert.Equal(t, "v3", pod.Labels["version"])
}

func TestCreateOrUpdateOnConflict(t *testing.T) {
	pod := newTestPod()
	client := fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()

	result, err := CreateOrUpdateOnConflict(context.Background(), client, client, pod, func(obj *corev1.Pod) error {
		obj.Labels["version"] = "v2"
		return nil
	})
	assert.NoError(t, err, "create should succeed")
	assert.EqualValues(t, controllerutil.OperationResultCreated, result, "pod created")
	assert.Equal(t, "v2", pod.Labels["version"])

	result, err = CreateOrUpdateOnConflict(context.Background(), client, client, pod, func(obj *corev1.Pod) error {
		obj.Labels["version"] = "v2"
		return nil
	})
	assert.NoError(t, err)
	assert.EqualValues(t, controllerutil.OperationResultNone, result, "no change")

	result, err = CreateOrUpdateOnConflict(context.Background(), client, client, pod, func(obj *corev1.Pod) error {
		obj.Labels["version"] = "v3"
		return nil
	})
	assert.NoError(t, err)
	assert.EqualValues(t, controllerutil.OperationResultUpdated, result)
	assert.Equal(t, "v3", pod.Labels["version"])
}
