/**
 * Copyright 2025 The KusionStack Authors
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

package client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestAddFinalizerAndUpdate(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()

	pod := newTestPod()
	err := c.Create(context.Background(), pod)
	require.NoError(t, err)

	oldPod := pod.DeepCopy()

	// latest pod has finalizers: test/v1
	err = AddFinalizerAndUpdate(context.Background(), c, pod, "test/v1")
	require.NoError(t, err)

	// oldPod is behind latest pod's resourceVersion
	// oldPod fails to add finalizer test/v2
	oldPod.Finalizers = []string{"test/v2"}
	err = c.Update(context.Background(), oldPod)
	assert.True(t, errors.IsConflict(err), "update should fail on conflict")

	// add finalizer test/v2 on oldPod with retryOnConflict
	// latest pod has finalizers: test/v1, test/v2
	oldPod.Finalizers = []string{}
	pod = oldPod.DeepCopy()
	err = AddFinalizerAndUpdate(context.Background(), c, pod, "test/v2")
	if assert.NoError(t, err) {
		assert.Len(t, pod.Finalizers, 2)
		assert.Equal(t, "test/v1", pod.Finalizers[0])
		assert.Equal(t, "test/v2", pod.Finalizers[1])
	}
}

func TestRemoveFinalizerAndUpdate(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()

	pod := newTestPod()
	err := c.Create(context.Background(), pod)
	require.NoError(t, err)

	// latest pod has finalizers: test/v1
	err = AddFinalizerAndUpdate(context.Background(), c, pod, "test/v1")
	require.NoError(t, err)

	oldPod := pod.DeepCopy()

	// latest pod has finalizers: test/v1 test/v2
	err = AddFinalizerAndUpdate(context.Background(), c, pod, "test/v2")
	require.NoError(t, err)

	// oldPod is behind latest pod's resourceVersion
	// oldPod fails to remove finalizer test/v1
	oldPod.Finalizers = []string{}
	err = c.Update(context.Background(), oldPod)
	assert.True(t, errors.IsConflict(err), "update should fail on conflict")

	// remove finalizer test/v1 on oldPod with retryOnConflict
	// latest pod has finalizers: test/v2
	oldPod.Finalizers = []string{"test/v1"}
	pod = oldPod.DeepCopy()
	err = RemoveFinalizerAndUpdate(context.Background(), c, pod, "test/v1")
	require.NoError(t, err)
	assert.Len(t, pod.Finalizers, 1)
	assert.Equal(t, "test/v2", pod.Finalizers[0])
}

func TestRemoveFinalizerAndDelete(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()

	pod := newTestPod()
	err := c.Create(context.Background(), pod)
	require.NoError(t, err)

	err = AddFinalizerAndUpdate(context.Background(), c, pod, "test/v1")
	require.NoError(t, err)

	// delete pod with non-exist finalizer "test/v2"
	// pod will not be deleted
	err = RemoveFinalizerAndDelete(context.Background(), c, pod, "test/v2")
	require.NoError(t, err)
	err = c.Get(context.Background(), client.ObjectKeyFromObject(pod), pod)
	require.NoError(t, err)
	assert.NotNil(t, pod.DeletionTimestamp)

	// delete pod with exist finalizer "test/v1"
	// pod will be deleted
	err = RemoveFinalizerAndDelete(context.Background(), c, pod, "test/v1")
	require.NoError(t, err)
	err = c.Get(context.Background(), client.ObjectKeyFromObject(pod), pod)
	assert.True(t, errors.IsNotFound(err))
}
