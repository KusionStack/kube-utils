/**
 * Copyright 2023 KusionStack Authors.
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

package workqueue

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("Test defaults", func() {
	const (
		timeout  = time.Second * 3
		interval = time.Millisecond * 100

		testConfigmap = "test1"
		testNamespace = "default"
	)

	var (
		configmapData = map[string]string{"hello": "world"}
	)

	Context("Use default workqueue priority", func() {
		It("Should get default workqueue priority if the item has no labels", func() {
			getPriorityFunc := DefaultGetPriorityFuncBuilder(k8sClient)
			configmap := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testConfigmap,
					Namespace: testNamespace,
				},
				Data: configmapData,
			}

			priority := getPriorityFunc(configmap)
			Expect(priority).To(Equal(DefaultWorkQueuePriority))
		})

		It("Should get workqueue priority from item labels", func() {
			getPriorityFunc := DefaultGetPriorityFuncBuilder(k8sClient)
			configmap := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testConfigmap,
					Namespace: testNamespace,
					Labels: map[string]string{
						DefaultWorkQueuePriorityLabel: "3",
					},
				},
				Data: configmapData,
			}

			priority := getPriorityFunc(configmap)
			Expect(priority).To(Equal(3))
		})

		It("Should get workqueue priority from namesapce labels", func() {
			getPriorityFunc := DefaultGetPriorityFuncBuilder(k8sClient)
			configmap := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testConfigmap,
					Namespace: testNamespace,
				},
			}

			namespace := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: testNamespace,
					Labels: map[string]string{
						DefaultWorkQueuePriorityLabel: "4",
					},
				},
			}
			err := k8sClient.Update(ctx, namespace)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() bool {
				namespace := &corev1.Namespace{}
				if err := k8sClient.Get(ctx, client.ObjectKey{Name: testNamespace}, namespace); err != nil {
					return false
				}
				if namespace.Labels == nil {
					return false
				}
				return namespace.Labels[DefaultWorkQueuePriorityLabel] == "4"
			}, timeout, interval).Should(BeTrue())

			priority := getPriorityFunc(configmap)
			Expect(priority).To(Equal(4))
		})
	})

	Context("Use custom workqueue priority", func() {
		It("Should get custom workqueue priority if the item has no labels", func() {
			getPriorityFunc := GetPriorityFuncBuilder(k8sClient, 1, "custom-priority")
			configmap := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testConfigmap,
					Namespace: testNamespace,
				},
				Data: configmapData,
			}

			priority := getPriorityFunc(configmap)
			Expect(priority).To(Equal(1))
		})

		It("Should get custom workqueue priority from item labels", func() {
			getPriorityFunc := GetPriorityFuncBuilder(k8sClient, 1, "custom-priority")
			configmap := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testConfigmap,
					Namespace: testNamespace,
					Labels: map[string]string{
						"custom-priority": "2",
					},
				},
				Data: configmapData,
			}

			priority := getPriorityFunc(configmap)
			Expect(priority).To(Equal(2))
		})

		It("Should get custom workqueue priority from namesapce labels", func() {
			getPriorityFunc := GetPriorityFuncBuilder(k8sClient, 1, "custom-priority")
			configmap := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testConfigmap,
					Namespace: testNamespace,
				},
			}

			namespace := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: testNamespace,
					Labels: map[string]string{
						"custom-priority": "3",
					},
				},
			}
			err := k8sClient.Update(ctx, namespace)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() bool {
				namespace := &corev1.Namespace{}
				if err := k8sClient.Get(ctx, client.ObjectKey{Name: testNamespace}, namespace); err != nil {
					return false
				}
				if namespace.Labels == nil {
					return false
				}
				return namespace.Labels["custom-priority"] == "3"
			}, timeout, interval).Should(BeTrue())

			priority := getPriorityFunc(configmap)
			Expect(priority).To(Equal(3))
		})
	})
})
