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

package expectations

import (
	"context"
	"time"

	"github.com/stretchr/testify/suite"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var podGVK = corev1.SchemeGroupVersion.WithKind("Pod")

func newPod(name string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: corev1.NamespaceDefault,
			Name:      name,
		},
	}
	return pod
}

type cacheExpactationTestSuite struct {
	suite.Suite

	testKey      string
	clock        *clock.FakeClock
	now          time.Time
	client       client.Client
	expectations *CacheExpectations
}

func (s *cacheExpactationTestSuite) SetupSuite() {
	s.testKey = "test"
	s.client = fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()
}

func (s *cacheExpactationTestSuite) SetupTest() {
	s.now = time.Now()
	s.clock = clock.NewFakeClock(s.now)
	s.expectations = NewxCacheExpectations(s.client, scheme.Scheme, s.clock)
}

func (s *cacheExpactationTestSuite) TearDownTest() {
	s.now = time.Now()
	s.clock = clock.NewFakeClock(s.now)
	s.client.DeleteAllOf(context.Background(), &corev1.Pod{})
}

func (s *cacheExpactationTestSuite) TestCacheExpectationItem() {
	item := newCacheExpatationItem(s.testKey, s.clock)
	item.Set(nil)
	s.Equal(s.now, item.timestamp)

	s.True(item.Fulfilled())
	item.Set(func() bool { return true })
	s.True(item.Fulfilled())
	item.Set(func() bool { return false })
	s.False(item.Fulfilled())

	s.clock.SetTime(s.now.Add(ExpectationsTimeout + time.Minute))
	s.True(item.isExpired())
	s.True(item.Fulfilled())
}

func (s *cacheExpactationTestSuite) TestCacheExpectation() {
	e := newCacheExpectation(s.testKey, s.clock, s.client, scheme.Scheme)

	err := e.ExpectCreation(podGVK, corev1.NamespaceDefault, "create-pod")
	s.Require().NoError(err)
	err = e.ExpectDeletion(podGVK, corev1.NamespaceDefault, "delete-pod")
	s.Require().NoError(err)

	satisfied := e.Fulfilled()
	s.False(satisfied, "waiting for create-pod")
	s.Len(e.items.ListKeys(), 1)

	// create fake pods
	err = s.client.Create(context.Background(), newPod("create-pod"))
	s.Require().NoError(err)
	err = s.client.Create(context.Background(), newPod("update-pod"))
	s.Require().NoError(err)
	err = e.ExpectUpdation(podGVK, corev1.NamespaceDefault, "update-pod", "2")
	s.Require().NoError(err)
	satisfied = e.Fulfilled()
	s.False(satisfied, "Waiting for update-pod")
	s.Len(e.items.ListKeys(), 1)

	// update resource version
	s.updatePodRV("update-pod")
	satisfied = e.Fulfilled()
	s.True(satisfied, "satisfied")

	s.Empty(e.items.ListKeys(), "items should be empty")
}

func (s *cacheExpactationTestSuite) TestCacheExpectations() {
	satisfied := s.expectations.SatisfiedExpectations(s.testKey)
	s.True(satisfied, "satified for empty expectation")

	s.expectations.ExpectCreation(s.testKey, podGVK, corev1.NamespaceDefault, "create-pod")
	satisfied = s.expectations.SatisfiedExpectations(s.testKey)
	s.False(satisfied, "waiting for create-pod")

	s.clock.SetTime(s.now.Add(ExpectationsTimeout + time.Minute))
	satisfied = s.expectations.SatisfiedExpectations(s.testKey)
	s.True(satisfied, "satified for expired expectation")

	s.expectations.DeleteExpectations(s.testKey)
	_, ok, _ := s.expectations.GetExpectations(s.testKey)
	s.False(ok, "expectation should be deleted")
}

func (s *cacheExpactationTestSuite) updatePodRV(name string) {
	pod := &corev1.Pod{}
	err := s.client.Get(context.Background(), client.ObjectKey{Name: name, Namespace: corev1.NamespaceDefault}, pod)
	s.Require().NoError(err)
	pod.Labels = map[string]string{"test": time.Now().String()}
	err = s.client.Update(context.Background(), pod)
	s.Require().NoError(err)
}
