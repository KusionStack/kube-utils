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
	"testing"

	"github.com/stretchr/testify/suite"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/kubectl/pkg/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	testNamespace = "revision-test"
	testName      = "test-revision"
)

func newTestStatefulSet() *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testName,
			UID:       "test-udi",
		},
		Spec: appsv1.StatefulSetSpec{
			RevisionHistoryLimit: ptr.To[int32](10),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"test":    "ref-manager",
					"version": "v1",
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

func newFakePod(owner metav1.Object) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      "test-pod-" + rand.String(5),
			Labels: map[string]string{
				"test":    "ref-manager",
				"version": "v1",
			},
		},
	}
	if owner != nil {
		owner := metav1.NewControllerRef(owner, schema.GroupVersionKind{
			Group:   "apps",
			Version: "v1",
			Kind:    "StatefulSet",
		})
		p.OwnerReferences = append(p.OwnerReferences, *owner)
	}
	return p
}

type ownerReferenceTestSute struct {
	suite.Suite
	client           client.Client
	owner            *appsv1.StatefulSet
	ownerGVK         schema.GroupVersionKind
	ownerRef         metav1.OwnerReference
	deferentOwner    *appsv1.StatefulSet
	deferentOwnerRef metav1.OwnerReference
}

func (s *ownerReferenceTestSute) SetupSuite() {
	s.client = fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()
	s.ownerGVK = appsv1.SchemeGroupVersion.WithKind("StatefulSet")
}

func (s *ownerReferenceTestSute) SetupTest() {
	s.owner = newTestStatefulSet()
	err := s.client.Create(context.TODO(), s.owner)
	s.NoError(err)
	s.ownerRef = *metav1.NewControllerRef(s.owner, s.ownerGVK)

	s.deferentOwner = newTestStatefulSet()
	s.deferentOwner.Name = "deferent-owner"
	s.deferentOwner.UID = "deferent-uid"
	err = s.client.Create(context.TODO(), s.deferentOwner)
	s.NoError(err)
	s.deferentOwnerRef = *metav1.NewControllerRef(s.deferentOwner, s.ownerGVK)
}

func (s *ownerReferenceTestSute) TearDownTest() {
	err := s.client.Delete(context.TODO(), s.owner)
	s.NoError(err)
	err = s.client.Delete(context.TODO(), s.deferentOwner)
	s.NoError(err)
	err = s.client.DeleteAllOf(context.TODO(), &corev1.Pod{}, client.InNamespace(testNamespace))
	s.NoError(err)
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestRevisionManagerTestSuite(t *testing.T) {
	suite.Run(t, new(ownerReferenceTestSute))
}
