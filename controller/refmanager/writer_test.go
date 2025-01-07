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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (s *ownerReferenceTestSute) TestAdopt() {
	writer := NewOwnerRefWriter(s.client)

	testcase := []struct {
		name    string
		object  client.Object
		wantErr bool
	}{
		{
			name:   "adopt orpha pod",
			object: newFakePod(nil),
		},
		{
			name:   "adopt owned pod",
			object: newFakePod(s.owner),
		},
		{
			name:    "adopt pod with conflict",
			object:  newFakePod(s.deferentOwner),
			wantErr: true,
		},
	}

	for i := range testcase {
		test := testcase[i]
		s.Run(test.name, func() {
			// create object
			err := s.client.Create(context.TODO(), test.object)
			s.NoError(err)

			err = writer.Adopt(context.TODO(), s.owner, s.ownerGVK, test.object)
			if test.wantErr {
				s.Error(err)
			} else {
				s.NoError(err)
				owner := metav1.GetControllerOfNoCopy(test.object)
				s.True(ReferSameObject(s.ownerRef, *owner), "the controller owner reference must be the same")
			}
		})
	}
}

func (s *ownerReferenceTestSute) TestRelease() {
	writer := NewOwnerRefWriter(s.client)

	testcase := []struct {
		name   string
		object client.Object
	}{
		{
			name:   "release orphan pod, nothing changed",
			object: newFakePod(nil),
		},
		{
			name:   "release owned pod",
			object: newFakePod(s.owner),
		},
		{
			name:   "release pod controlled by other object",
			object: newFakePod(s.deferentOwner),
		},
	}

	for i := range testcase {
		test := testcase[i]
		s.Run(test.name, func() {
			// create object
			err := s.client.Create(context.TODO(), test.object)
			s.NoError(err)

			writer.Release(context.TODO(), s.owner, test.object) // nolint
			owner := metav1.GetControllerOfNoCopy(test.object)

			if owner != nil {
				s.False(ReferSameObject(s.ownerRef, *owner), "the controller owner reference must not be the same")
			}
		})
	}
}
