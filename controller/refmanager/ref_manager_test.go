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
	"fmt"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (s *ownerReferenceTestSute) TestClaim() {
	writer := NewOwnerRefWriter(s.client)
	match, err := LabelSelectorAsMatch(s.owner.Spec.Selector)
	s.NoError(err)
	m := NewObjectControllerRefManager(writer, s.owner, s.ownerGVK, match)

	testcase := []struct {
		name             string
		object           client.Object
		originalOwnerRef *metav1.OwnerReference
		wantErr          bool
		wantAdopted      bool
		wantRelease      bool
	}{
		{
			name:             "adopt orpha pod",
			object:           newFakePod(nil),
			originalOwnerRef: nil,
			wantErr:          false,
			wantAdopted:      true,
			wantRelease:      false,
		},
		{
			name: "do nothing on orphan pod without match",
			object: func() client.Object {
				pod := newFakePod(nil)
				// delete labels to mismatch
				pod.Labels = nil
				return pod
			}(),
			originalOwnerRef: nil,
			wantErr:          false,
			wantAdopted:      false,
			wantRelease:      false,
		},
		{
			name:             "adopt owned pod",
			object:           newFakePod(s.owner),
			originalOwnerRef: &s.ownerRef,
			wantErr:          false,
			wantAdopted:      true,
			wantRelease:      false,
		},
		{
			name: "release owned pod if mismatch",
			object: func() client.Object {
				pod := newFakePod(s.owner)
				// delete labels to mismatch
				pod.Labels = nil
				return pod
			}(),
			originalOwnerRef: &s.ownerRef,
			wantErr:          false,
			wantAdopted:      false,
			wantRelease:      true,
		},
		{
			name:             "do nothing on non-owned pod",
			object:           newFakePod(s.deferentOwner),
			originalOwnerRef: &s.deferentOwnerRef,
			wantErr:          false,
			wantAdopted:      false,
			wantRelease:      false,
		},
		{
			name: "do nothing on deleting pod",
			object: func() client.Object {
				pod := newFakePod(nil)
				pod.DeletionTimestamp = ptr.To(metav1.Now())
				return pod
			}(),
			originalOwnerRef: nil,
			wantErr:          false,
			wantAdopted:      false,
			wantRelease:      false,
		},
	}

	for i := range testcase {
		test := testcase[i]
		s.Run(test.name, func() {
			// create object
			err := s.client.Create(context.TODO(), test.object)
			s.NoError(err)

			claimed, err := m.Claim(context.Background(), test.object)
			if test.wantErr {
				s.Error(err)
				return
			}

			s.NoError(err)
			if s.Equal(test.wantAdopted, claimed) {
				owner := metav1.GetControllerOf(test.object)
				if test.wantAdopted {
					s.True(ReferSameObject(*owner, s.ownerRef), "object should be adopted")
				}
				if test.wantRelease {
					if owner != nil {
						s.False(ReferSameObject(*owner, *test.originalOwnerRef), "owner should be release")
					}
				}
				if !test.wantAdopted && !test.wantRelease && test.originalOwnerRef != nil {
					s.True(ReferSameObject(*owner, *test.originalOwnerRef), "ownerRef should not be changed ")
				}
			}
		})
	}
}

func (s *ownerReferenceTestSute) TestClaimWhenOwnerDeleting() {
	writer := NewOwnerRefWriter(s.client)
	match, err := LabelSelectorAsMatch(s.owner.Spec.Selector)
	s.NoError(err)
	s.owner.DeletionTimestamp = ptr.To(metav1.Now())
	m := NewObjectControllerRefManager(writer, s.owner, s.ownerGVK, match)

	testcase := []struct {
		name             string
		object           client.Object
		originalOwnerRef *metav1.OwnerReference
		wantErr          bool
		wantClaimed      bool
	}{
		{
			name:             "orpha pod",
			object:           newFakePod(nil),
			originalOwnerRef: nil,
			wantErr:          false,
			wantClaimed:      false,
		},
		{
			name: "orphan pod without match",
			object: func() client.Object {
				pod := newFakePod(nil)
				// delete labels to mismatch
				pod.Labels = nil
				return pod
			}(),
			originalOwnerRef: nil,
			wantErr:          false,
			wantClaimed:      false,
		},
		{
			name:             "owned pod",
			object:           newFakePod(s.owner),
			originalOwnerRef: &s.ownerRef,
			wantErr:          false,
			wantClaimed:      true,
		},
		{
			name: "owned pod without match",
			object: func() client.Object {
				pod := newFakePod(s.owner)
				// delete labels to mismatch
				pod.Labels = nil
				return pod
			}(),
			originalOwnerRef: &s.ownerRef,
			wantErr:          false,
			wantClaimed:      false,
		},
		{
			name:             "non-owned pod",
			object:           newFakePod(s.deferentOwner),
			originalOwnerRef: &s.deferentOwnerRef,
			wantErr:          false,
			wantClaimed:      false,
		},
		{
			name: "pod with DeletionTimestamp",
			object: func() client.Object {
				pod := newFakePod(nil)
				pod.DeletionTimestamp = ptr.To(metav1.Now())
				return pod
			}(),
			originalOwnerRef: nil,
			wantErr:          false,
			wantClaimed:      false,
		},
	}

	for i := range testcase {
		test := testcase[i]
		s.Run(test.name, func() {
			// create object
			err := s.client.Create(context.TODO(), test.object)
			s.NoError(err)

			claimed, err := m.Claim(context.Background(), test.object)
			if test.wantErr {
				s.Error(err)
				return
			}

			s.Equal(test.wantClaimed, claimed)
			owner := metav1.GetControllerOf(test.object)
			if test.originalOwnerRef != nil {
				s.True(ReferSameObject(*owner, *test.originalOwnerRef), "ownerRef should not be changed")
			}
		})
	}
}

func (s *ownerReferenceTestSute) TestClaimWhenCanAdoptFailed() {
	writer := NewOwnerRefWriter(s.client)
	match, err := LabelSelectorAsMatch(s.owner.Spec.Selector)
	s.NoError(err)
	m := NewObjectControllerRefManager(writer, s.owner, s.ownerGVK, match)
	rm := m.(*objectControllerRefManager)
	rm.canAdoptFunc = func(o metav1.Object) error {
		return fmt.Errorf("object is deleted")
	}
	// claim will failed when calling adopt
	object := newFakePod(nil)
	claimed, err := m.Claim(context.Background(), object)

	s.Error(err)
	s.False(claimed)
}

func (s *ownerReferenceTestSute) TestClaimAllOf() {
	writer := NewOwnerRefWriter(s.client)
	match, err := LabelSelectorAsMatch(s.owner.Spec.Selector)
	s.Require().NoError(err)
	m := NewObjectControllerRefManager(writer, s.owner, s.ownerGVK, match)

	adopted := []metav1.Object{
		newFakePod(nil),
		newFakePod(s.owner),
	}
	released := []metav1.Object{
		func() metav1.Object {
			pod := newFakePod(s.owner)
			// delete labels to mismatch
			pod.Labels = nil
			return pod
		}(),
	}
	noChanged := []metav1.Object{
		newFakePod(s.deferentOwner),
	}

	allObjects := append([]metav1.Object{}, adopted...)
	allObjects = append(allObjects, released...)
	allObjects = append(allObjects, noChanged...)

	for _, obj := range allObjects {
		err := s.client.Create(context.TODO(), obj.(client.Object))
		s.Require().NoError(err)
	}

	result, err := m.ClaimAllOf(context.Background(), allObjects)
	s.Require().NoError(err)
	// check adopted objects
	s.Len(result, len(adopted))
	resultNames := lo.Map(result, func(o metav1.Object, _ int) string {
		return o.GetName()
	})
	adoptedNames := lo.Map(adopted, func(o metav1.Object, _ int) string {
		return o.GetName()
	})
	s.EqualValues(adoptedNames, resultNames)

	forEarch := func(objs []metav1.Object, check func(ownerRef *metav1.OwnerReference)) {
		for _, obj := range objs {
			pod := &corev1.Pod{}
			err := s.client.Get(context.Background(), client.ObjectKeyFromObject(obj.(client.Object)), pod)
			s.Require().NoError(err)
			ownerRef := metav1.GetControllerOf(pod)
			check(ownerRef)
		}
	}

	// check adopted object owners
	forEarch(adopted, func(ownerRef *metav1.OwnerReference) {
		s.True(ReferSameObject(*ownerRef, s.ownerRef), "object should be adopted")
	})
	// check released objects
	forEarch(released, func(ownerRef *metav1.OwnerReference) {
		s.Require().Nil(ownerRef)
	})
	// check no changed objects
	forEarch(noChanged, func(ownerRef *metav1.OwnerReference) {
		s.True(ReferSameObject(*ownerRef, s.deferentOwnerRef), "object should not be changed")
	})
}
