/*
Copyright 2023 The KusionStack Authors.

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

package cache

import (
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const (
	TestPodNamespaceFieldIndex       = "testPodNamespaceIndex"
	TestPodContainerNumberFieldIndex = "testPodContainerNumberIndex"
)

var (
	env    *envtest.Environment
	mgr    manager.Manager
	ctx    context.Context
	cancel context.CancelFunc

	pod    *corev1.Pod
	deploy *appsv1.Deployment

	c client.Client
)

var _ = Describe("cache support no DeepCopy", func() {
	It("support list without DeepCopy", func() {
		podListNoDeepCopy1 := &corev1.PodList{}
		Expect(c.List(ctx, podListNoDeepCopy1, DisableDeepCopy)).Should(Succeed())
		podListNoDeepCopy2 := &corev1.PodList{}
		Expect(c.List(ctx, podListNoDeepCopy2, DisableDeepCopy)).Should(Succeed())
		podListDeepCopy := &corev1.PodList{}
		Expect(c.List(ctx, podListDeepCopy)).Should(Succeed())

		Expect(podListNoDeepCopy1.Items[0]).Should(BeEquivalentTo(podListNoDeepCopy2.Items[0]))
		Expect(podListNoDeepCopy1.Items[0]).ShouldNot(BeEquivalentTo(podListDeepCopy.Items[0]))
	})

	It("support list without DeepCopy using field index", func() {
		fieldIndexOption := &client.ListOptions{
			FieldSelector: fields.AndSelectors(fields.OneTermEqualSelector(TestPodNamespaceFieldIndex, pod.Namespace),
				fields.OneTermEqualSelector(TestPodContainerNumberFieldIndex, fmt.Sprintf("%d", len(pod.Spec.Containers)))),
		}
		podListNoDeepCopy1 := &corev1.PodList{}
		Expect(c.List(ctx, podListNoDeepCopy1, DisableDeepCopy, fieldIndexOption)).Should(Succeed())
		podListNoDeepCopy2 := &corev1.PodList{}
		Expect(c.List(ctx, podListNoDeepCopy2, DisableDeepCopy, fieldIndexOption)).Should(Succeed())
		podListDeepCopy := &corev1.PodList{}
		Expect(c.List(ctx, podListDeepCopy)).Should(Succeed())

		Expect(podListNoDeepCopy1.Items[0]).Should(BeEquivalentTo(podListNoDeepCopy2.Items[0]))
		Expect(podListNoDeepCopy1.Items[0]).ShouldNot(BeEquivalentTo(podListDeepCopy.Items[0]))
	})

	It("support get without DeepCopy", func() {
		podWithoutDeepCopy1 := &ObjectWithoutDeepCopy{Object: &corev1.Pod{}}
		Expect(c.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, podWithoutDeepCopy1)).Should(Succeed())
		podWithoutDeepCopy2 := &ObjectWithoutDeepCopy{Object: &corev1.Pod{}}
		Expect(c.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, podWithoutDeepCopy2)).Should(Succeed())
		podWithDeepCopy := &corev1.Pod{}
		Expect(c.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, podWithDeepCopy)).Should(Succeed())

		Expect(podWithoutDeepCopy1.Object.(*corev1.Pod).Spec.Containers[0].ReadinessProbe).ShouldNot(BeNil())
		Expect(podWithoutDeepCopy2.Object.(*corev1.Pod).Spec.Containers[0].ReadinessProbe).ShouldNot(BeNil())
		Expect(podWithDeepCopy.Spec.Containers[0].ReadinessProbe).ShouldNot(BeNil())
		Expect(podWithoutDeepCopy1.Object.(*corev1.Pod).Spec.Containers[0].ReadinessProbe).Should(BeIdenticalTo(podWithoutDeepCopy2.Object.(*corev1.Pod).Spec.Containers[0].ReadinessProbe))
		Expect(podWithoutDeepCopy1.Object.(*corev1.Pod).Spec.Containers[0].ReadinessProbe).ShouldNot(BeIdenticalTo(podWithDeepCopy.Spec.Containers[0].ReadinessProbe))

		Expect(errors.IsNotFound(c.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: "not-found"}, podWithoutDeepCopy1))).Should(BeTrue())
	})

	It("support uncached object", func() {
		deployUncached0 := &ObjectWithoutDeepCopy{Object: &appsv1.Deployment{}}
		Expect(c.Get(ctx, types.NamespacedName{Namespace: deploy.Namespace, Name: deploy.Name}, deployUncached0)).ShouldNot(Succeed())
		deployUncached1 := &appsv1.Deployment{}
		Expect(c.Get(ctx, types.NamespacedName{Namespace: deploy.Namespace, Name: deploy.Name}, deployUncached1)).Should(Succeed())
		deployUncached2 := &appsv1.Deployment{}
		Expect(c.Get(ctx, types.NamespacedName{Namespace: deploy.Namespace, Name: deploy.Name}, deployUncached2)).Should(Succeed())

		Expect(deployUncached1.Spec.Selector).ShouldNot(BeNil())
		Expect(deployUncached2.Spec.Selector).ShouldNot(BeNil())
		Expect(deployUncached1.Spec.Selector).ShouldNot(BeIdenticalTo(deployUncached2.Spec.Selector))
	})
})

func initPod() {
	pod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "foo",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "foo",
					Image: "nginx:v1",
					ReadinessProbe: &corev1.Probe{
						Handler: corev1.Handler{
							HTTPGet: &corev1.HTTPGetAction{
								Path: "/healthz",
								Port: intstr.FromInt(8080),
							},
						},
					},
				},
			},
		},
	}
	Expect(c.Create(ctx, pod)).Should(Succeed())
	Eventually(func() bool {
		return c.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, pod) == nil
	}, 5*time.Second, 1*time.Second).Should(BeTrue())
}

func initDeploy() {
	deploy = &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "foo",
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": "foo",
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": "foo",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "foo",
							Image: "nginx:v1",
						},
					},
				},
			},
		},
	}
	Expect(c.Create(ctx, deploy)).Should(Succeed())
	Eventually(func() bool {
		return c.Get(ctx, types.NamespacedName{Namespace: deploy.Namespace, Name: deploy.Name}, deploy) == nil
	}, 5*time.Second, 1*time.Second).Should(BeTrue())
}

func TestCache(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Cache Test Suite")
}

var _ = BeforeSuite(func() {
	env = &envtest.Environment{}

	config, err := env.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(config).NotTo(BeNil())

	ctx, cancel = context.WithCancel(context.Background())

	mgr, err = manager.New(config, manager.Options{
		MetricsBindAddress:    "0",
		NewCache:              NewCacheSupportDisableDeepCopy,
		NewClient:             DefaultNewClient,
		ClientDisableCacheFor: []client.Object{&appsv1.Deployment{}},
	})
	Expect(err).NotTo(HaveOccurred())
	c = mgr.GetClient()

	c := mgr.GetCache()
	c.IndexField(ctx, &corev1.Pod{}, TestPodNamespaceFieldIndex, func(obj client.Object) []string {
		po := obj.(*corev1.Pod)
		return []string{po.Namespace}
	})
	c.IndexField(ctx, &corev1.Pod{}, TestPodContainerNumberFieldIndex, func(obj client.Object) []string {
		po := obj.(*corev1.Pod)
		return []string{fmt.Sprintf("%d", len(po.Spec.Containers))}
	})

	go func() {
		err = mgr.Start(ctx)
		Expect(err).NotTo(HaveOccurred())
	}()

	initPod()
	initDeploy()
})

var _ = AfterSuite(func() {
	cancel()
	By("tearing down the test environment")

	Expect(env.Stop()).NotTo(HaveOccurred())
})
