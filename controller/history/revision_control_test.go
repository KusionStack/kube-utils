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

package history

import (
	"context"

	"github.com/stretchr/testify/suite"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type revisionControlTestSuite struct {
	suite.Suite
	client         client.Client
	ownerAdapter   RevisionOwner
	revisionContrl RevisionControlInterface
	testObj        *appsv1.StatefulSet
	testRevision   *appsv1.ControllerRevision
}

func (s *revisionControlTestSuite) SetupSuite() {
	s.client = fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()
	s.revisionContrl = NewRevisionControl(s.client, s.client)
	s.ownerAdapter = &revisionOwnerStatefulSet{}
}

func (s *revisionControlTestSuite) SetupTest() {
	s.testObj = newTestStatefulSet()
	err := s.client.Create(context.TODO(), s.testObj)
	s.Require().NoError(err)

	collisionCount := int32(0)
	// create revision for test object
	revision := newStatefulSetControllerRevision(s.testObj, 0, &collisionCount)
	err = s.client.Create(context.TODO(), revision)
	s.Require().NoError(err)
	s.testRevision = revision
}

func (s *revisionControlTestSuite) TearDownTest() {
	err := s.client.Delete(context.TODO(), s.testObj)
	s.NoError(err)
	err = s.client.DeleteAllOf(context.TODO(), &appsv1.ControllerRevision{}, client.InNamespace(testNamespace))
	s.NoError(err)
}

func (s *revisionControlTestSuite) TestCreateRevisions() {
	sequence := []struct {
		name               string
		obj                *appsv1.StatefulSet
		wantCollisionCount int32
		revisionsCount     int
	}{
		{
			name:               "conflict but return existing one",
			obj:                s.testObj,
			wantCollisionCount: 0,
			revisionsCount:     1,
		},
		{
			name: "template-update",
			obj: func() *appsv1.StatefulSet {
				obj := s.testObj.DeepCopy()
				// change template
				obj.Spec.Template.Labels["version"] = "v2"
				return obj
			}(),
			wantCollisionCount: 0,
			revisionsCount:     2,
		},
	}

	collisionCount := int32(0)
	for i := range sequence {
		test := sequence[i]
		s.Run(test.name, func() {
			revision := newStatefulSetControllerRevision(test.obj, 0, &collisionCount)
			created, err := s.revisionContrl.CreateControllerRevision(context.Background(), s.testObj, revision, &collisionCount)
			if s.NoError(err) {
				s.Equal(test.wantCollisionCount, collisionCount)
				err := s.client.Get(context.Background(), client.ObjectKeyFromObject(created), &appsv1.ControllerRevision{})
				s.NoError(err)

				list := &appsv1.ControllerRevisionList{}
				err = s.client.List(context.Background(), list, client.InNamespace(test.obj.Namespace))
				s.NoError(err)
				s.Len(list.Items, test.revisionsCount)
			}
		})
	}
}

func (s *revisionControlTestSuite) TestCreateRevisionsConflict1() {
	collisionCount := int32(0)
	// change parent object's UID to rretend it's been recreated.
	obj := s.testObj
	obj.UID = "mock-uid"
	revision := newStatefulSetControllerRevision(obj, 0, &collisionCount)

	// this revision will has the same name as the first revision, but it's parent object has different UID.
	created, err := s.revisionContrl.CreateControllerRevision(context.Background(), obj, revision, &collisionCount)
	s.Require().NoError(err)
	s.EqualValues(1, collisionCount, "create revision conflict, collisionCount should be 1")
	s.NotEqual(revision.Name, created.Name, "create revision conflict, revision name should be changed")
	s.Equal(revision.Labels[ControllerRevisionHashLabel], created.Labels[ControllerRevisionHashLabel], "the hash label should be the same")

	list := &appsv1.ControllerRevisionList{}
	err = s.client.List(context.Background(), list, client.InNamespace(s.testObj.Namespace))
	s.Require().NoError(err)
	s.Len(list.Items, 2)
}

func (s *revisionControlTestSuite) TestCreateRevisionsConflict2() {
	collisionCount := int32(0)
	// change existing revision data in store
	revision := s.testRevision.DeepCopy()
	revision.Data.Raw = []byte(`{"data":"changed"}`)
	err := s.client.Update(context.Background(), revision)
	s.Require().NoError(err)

	// create revision again
	revision = newStatefulSetControllerRevision(s.testObj, 0, &collisionCount)
	created, err := s.revisionContrl.CreateControllerRevision(context.Background(), s.testObj, revision, &collisionCount)
	s.Require().NoError(err)
	s.EqualValues(1, collisionCount, "create revision conflict, collisionCount should be 1")
	s.NotEqual(revision.Name, created.Name)
	s.Equal(revision.Labels[ControllerRevisionHashLabel], created.Labels[ControllerRevisionHashLabel], "the hash label should be the same")

	list := &appsv1.ControllerRevisionList{}
	err = s.client.List(context.Background(), list, client.InNamespace(s.testObj.Namespace))
	s.Require().NoError(err)
	s.Len(list.Items, 2)
}

func (s *revisionControlTestSuite) TestListRevisions() {
	// create orphan revision
	revision := newStatefulSetControllerRevision(s.testObj, 0, ptr.To(int32(0)))
	revision.OwnerReferences = nil
	s.revisionContrl.CreateControllerRevision(context.Background(), s.testObj, revision, ptr.To(int32(1)))

	// create 3 revisions
	for i := 2; i < 5; i++ {
		revision := newStatefulSetControllerRevision(s.testObj, 0, ptr.To(int32(0)))
		s.revisionContrl.CreateControllerRevision(context.Background(), s.testObj, revision, ptr.To(int32(i)))
	}

	selector, _ := metav1.LabelSelectorAsSelector(s.testObj.Spec.Selector)
	result, err := s.revisionContrl.ListControllerRevisions(context.Background(), s.testObj, selector)
	s.Require().NoError(err)
	s.Len(result, 5)
}

func (s *revisionControlTestSuite) TestUpdateRevision() {
	newRevison := 100
	s.revisionContrl.UpdateControllerRevision(context.Background(), s.testRevision, int64(newRevison))

	revision := &appsv1.ControllerRevision{}
	err := s.client.Get(context.Background(), client.ObjectKeyFromObject(s.testRevision), revision)
	s.Require().NoError(err)
	s.EqualValues(newRevison, revision.Revision)
}

func (s *revisionControlTestSuite) TestDeleteRevision() {
	key := client.ObjectKeyFromObject(s.testRevision)
	err := s.revisionContrl.DeleteControllerRevision(context.Background(), s.testRevision)
	s.Require().NoError(err)
	err = s.client.Get(context.Background(), key, &appsv1.ControllerRevision{})
	s.True(errors.IsNotFound(err), "revision should be deleted")

	// delete it again
	err = s.revisionContrl.DeleteControllerRevision(context.Background(), s.testRevision)
	s.Require().NoError(err)
	err = s.client.Get(context.Background(), key, &appsv1.ControllerRevision{})
	s.True(errors.IsNotFound(err), "revision should be deleted")
}
