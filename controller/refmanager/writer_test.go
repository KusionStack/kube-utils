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

package refmanager

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (s *ownerReferenceTestSute) TestAdopt() {
	writer := NewOwnerRefWriter(s.client)

	testcase := []struct {
		name    string
		pod     *corev1.Pod
		wantErr bool
	}{
		{
			name: "adopt orpha pod",
			pod:  newFakePod(nil),
		},
		{
			name: "adopt owned pod",
			pod:  newFakePod(s.owner),
		},
		{
			name:    "adopt pod with conflict",
			pod:     newFakePod(s.deferentOwner),
			wantErr: true,
		},
	}

	for i := range testcase {
		test := testcase[i]
		s.Run(test.name, func() {
			// create object
			err := s.client.Create(context.TODO(), test.pod)
			s.NoError(err)

			err = writer.Adopt(context.TODO(), s.owner, s.ownerGVK, test.pod)
			if test.wantErr {
				s.Error(err)
			} else {
				s.NoError(err)
				// get pod from client
				pod := &corev1.Pod{}
				err = s.client.Get(context.TODO(), client.ObjectKeyFromObject(test.pod), pod)
				s.Require().NoError(err)
				owner := metav1.GetControllerOfNoCopy(pod)
				s.Require().NotNil(owner)
				s.True(ReferSameObject(s.ownerRef, *owner), "the controller owner reference must be the same")
			}
		})
	}
}

func (s *ownerReferenceTestSute) TestRelease() {
	writer := NewOwnerRefWriter(s.client)

	testcase := []struct {
		name string
		pod  client.Object
	}{
		{
			name: "release orphan pod, nothing changed",
			pod:  newFakePod(nil),
		},
		{
			name: "release owned pod",
			pod:  newFakePod(s.owner),
		},
		{
			name: "release pod controlled by other object",
			pod:  newFakePod(s.deferentOwner),
		},
	}

	for i := range testcase {
		test := testcase[i]
		s.Run(test.name, func() {
			// create object
			err := s.client.Create(context.TODO(), test.pod)
			s.NoError(err)

			writer.Release(context.TODO(), s.owner, test.pod) // nolint

			// get pod from client
			pod := &corev1.Pod{}
			err = s.client.Get(context.TODO(), client.ObjectKeyFromObject(test.pod), pod)
			s.Require().NoError(err)
			owner := metav1.GetControllerOfNoCopy(pod)
			if owner != nil {
				s.False(ReferSameObject(s.ownerRef, *owner), "the controller owner reference must not be the same")
			}
		})
	}
}
