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
	"encoding/json"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	testNamespace = "revision-test"
	testName      = "test-revision"
)

var statefulSetGVK = appsv1.SchemeGroupVersion.WithKind("StatefulSet")

func newTestStatefulSet() *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testName,
		},
		Spec: appsv1.StatefulSetSpec{
			RevisionHistoryLimit: ptr.To[int32](10),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"test": "revision",
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"test":    "revision",
						"version": "v1",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "test",
							Image: "nginx:v1",
						},
					},
				},
			},
		},
	}
}

type historyManagerTestSuite struct {
	suite.Suite
	client       client.Client
	history      RevisionControlInterface
	testObj      *appsv1.StatefulSet
	ownerAdapter RevisionOwner
	manager      HistoryManager
}

func (s *historyManagerTestSuite) SetupSuite() {
	s.client = fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()
	s.history = NewRevisionControl(s.client, s.client)
	s.ownerAdapter = &revisionOwnerStatefulSet{}
	s.manager = NewHistoryManager(s.history, s.ownerAdapter)
}

func (s *historyManagerTestSuite) SetupTest() {
	s.testObj = newTestStatefulSet()
	err := s.client.Create(context.TODO(), s.testObj)
	s.NoError(err)
}

func (s *historyManagerTestSuite) TearDownTest() {
	err := s.client.Delete(context.TODO(), s.testObj)
	s.NoError(err)
	err = s.client.DeleteAllOf(context.TODO(), &appsv1.ControllerRevision{}, client.InNamespace(testNamespace))
	s.NoError(err)
}

func (s *historyManagerTestSuite) TestRevisionConstruction() {
	// first loop to construct first revision
	currentRevison, updatedRevision, revisions, collisionCount, created, err := s.manager.ConstructRevisions(context.Background(), s.testObj)
	if s.NoError(err) {
		s.EqualValues(0, collisionCount, "collision count should be 0")
		s.True(created, "first loop should create new revision")
		s.Len(revisions, 1)
		s.NotEmpty(currentRevison.Name)
		s.NotEmpty(updatedRevision.Name)
		s.Equal(currentRevison.Name, updatedRevision.Name, "current revision and updated revision should be the same")
		s.Len(s.getAllRevisionNames(), 1, "should have only one revision after first loop")
	}
	// update object status
	s.testObj.Status.CollisionCount = &collisionCount
	s.testObj.Status.CurrentRevision = currentRevison.Name
	s.testObj.Status.UpdateRevision = updatedRevision.Name
	v1Revision := currentRevison.Name

	// updating template spec should construct a new updated CcontrollerRevision
	s.testObj.Spec.Template.Labels["version"] = "v2"
	currentRevison, updatedRevision, revisions, collisionCount, created, err = s.manager.ConstructRevisions(context.Background(), s.testObj)
	if s.NoError(err) {
		s.NotNil(collisionCount, "collision must not be nil")
		s.EqualValues(0, collisionCount, "collision count should be 0")
		s.True(created, "template changed should create new revision")
		s.Len(revisions, 2, "should have 2 revisions after updating template spec")
		s.NotEmpty(currentRevison.Name)
		s.NotEmpty(updatedRevision.Name)
		s.NotEqual(currentRevison.Name, updatedRevision.Name, "current revision and updated revision should not be the same")
		s.Len(s.getAllRevisionNames(), 2, "should have 2 revisions")
	}

	// update object status
	s.testObj.Status.CollisionCount = &collisionCount
	s.testObj.Status.CurrentRevision = updatedRevision.Name
	s.testObj.Status.UpdateRevision = updatedRevision.Name

	// sync again
	currentRevison, updatedRevision, revisions, collisionCount, created, err = s.manager.ConstructRevisions(context.Background(), s.testObj)
	if s.NoError(err) {
		s.EqualValues(0, collisionCount, "collision count should be 0")
		s.False(created, "template does not change, should not create new revision")
		s.Len(revisions, 2, "should have 2 revisions")
		s.NotEmpty(currentRevison.Name)
		s.NotEmpty(updatedRevision.Name)
		s.Equal(currentRevison.Name, updatedRevision.Name, "current revision and updated revision should be the same")
		s.Len(s.getAllRevisionNames(), 2, "should have 2 revisions")
	}

	// revert to v1
	s.testObj.Spec.Template.Labels["version"] = "v1"
	currentRevison, updatedRevision, revisions, collisionCount, created, err = s.manager.ConstructRevisions(context.Background(), s.testObj)
	if s.NoError(err) {
		s.EqualValues(0, collisionCount, "collision count should be 0")
		s.False(created, "v1 revision is already exist, should not create new revision")
		s.Len(revisions, 2, "should have 2 revisions")
		s.NotEmpty(currentRevison.Name)
		s.NotEmpty(updatedRevision.Name)
		s.NotEqual(currentRevison.Name, updatedRevision.Name, "current revision and updated revision should not be the same")
		s.Equal(v1Revision, updatedRevision.Name, "updated revision should be the same with v1 revision %s", v1Revision)
		s.Len(s.getAllRevisionNames(), 2, "should have 2 revisions")
	}
}

func (s *historyManagerTestSuite) TestRevisionCleanUp() {
	// we only need 0 unused revision, but actually 2 unused revisions will be remained
	s.testObj.Spec.RevisionHistoryLimit = ptr.To[int32](0)

	tests := []struct {
		version string
		wantLen int
		message string
	}{
		{
			version: "v1",
			wantLen: 1,
			message: "v1 revision is just created",
		},
		{
			version: "v2",
			wantLen: 2,
			message: "v1 revision is in use, v2 revision is just created",
		},
		{
			version: "v3",
			wantLen: 3,
			message: "v1 is unused but remained, v2 revision is in use, v3 revision is just created",
		},
		{
			version: "v4",
			wantLen: 3,
			message: "v1 is deleted, v2 unused but remained, v3 revision is in use, v4 is just created",
		},
		{
			version: "v5",
			wantLen: 3,
			message: "v1 v2 are deleted, v3 unused but remained, v4 is in use, v5 is just created",
		},
	}

	allRevisionNames := map[string]string{}
	for _, test := range tests {
		// change template
		s.T().Logf("change template label to %s", test.version)
		s.testObj.Spec.Template.Labels["version"] = test.version
		_, updatedRevision, revisions, collisionCount, _, _ := s.manager.ConstructRevisions(context.Background(), s.testObj)
		s.Len(revisions, test.wantLen, test.message)
		s.Len(s.getAllRevisionNames(), test.wantLen)

		// update object status
		s.testObj.Status.CollisionCount = &collisionCount
		s.testObj.Status.CurrentRevision = updatedRevision.Name
		s.testObj.Status.UpdateRevision = updatedRevision.Name
		allRevisionNames[test.version] = updatedRevision.Name
	}

	s.T().Logf("all revisions names: %v", allRevisionNames)
	s.T().Logf("revisions in storage: %v", s.getAllRevisionNames())

	// there are 3 revisions now, v3,v4 is unused, v5 is currentReivision and updatedRevision
	// Normally, v3 will be deleted if v6 is created, but now we set currentRevision to v3, then v4 will be deleted
	s.testObj.Spec.Template.Labels["version"] = "v6"
	s.testObj.Status.CurrentRevision = allRevisionNames["v3"]
	currentRevison, updatedRevision, revisions, _, _, _ := s.manager.ConstructRevisions(context.Background(), s.testObj)
	s.Equal(s.testObj.Status.CurrentRevision, currentRevison.Name)
	s.Equal(allRevisionNames["v3"], currentRevison.Name)
	s.NotEqual(s.testObj.Status.UpdateRevision, updatedRevision.Name, "status.updateRevision is v5 ant updatedRevision.Name should be v6")
	s.Len(revisions, 3)
	s.Len(s.getAllRevisionNames(), 3)
}

func (s *historyManagerTestSuite) getAllRevisionNames() []string {
	revisions := &appsv1.ControllerRevisionList{}
	err := s.client.List(context.TODO(), revisions, client.InNamespace(testNamespace))
	s.Require().NoError(err)

	return lo.Map(revisions.Items, func(item appsv1.ControllerRevision, _ int) string {
		return item.Name
	})
}

type revisionOwnerStatefulSet struct{}

func (a *revisionOwnerStatefulSet) GetGroupVersionKind() schema.GroupVersionKind {
	return statefulSetGVK
}

func (a *revisionOwnerStatefulSet) GetMatchLabels(obj metav1.Object) map[string]string {
	sts := obj.(*appsv1.StatefulSet)
	return sts.Spec.Selector.MatchLabels
}

func (a *revisionOwnerStatefulSet) GetCollisionCount(obj metav1.Object) *int32 {
	sts := obj.(*appsv1.StatefulSet)
	return sts.Status.CollisionCount
}

func (a *revisionOwnerStatefulSet) GetHistoryLimit(obj metav1.Object) int32 {
	sts := obj.(*appsv1.StatefulSet)
	return *sts.Spec.RevisionHistoryLimit
}

func (a *revisionOwnerStatefulSet) GetPatch(obj metav1.Object) ([]byte, error) {
	// mock patch
	sts := obj.(*appsv1.StatefulSet)
	return json.Marshal(sts.Spec.Template)
}

func (a *revisionOwnerStatefulSet) GetCurrentRevision(obj metav1.Object) string {
	sts := obj.(*appsv1.StatefulSet)
	return sts.Status.CurrentRevision
}

func (a *revisionOwnerStatefulSet) GetInUsedRevisions(obj metav1.Object) (sets.String, error) {
	sts := obj.(*appsv1.StatefulSet)
	return sets.NewString(sts.Status.CurrentRevision, sts.Status.UpdateRevision), nil
}
