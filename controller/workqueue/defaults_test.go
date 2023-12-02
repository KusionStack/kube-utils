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
	"context"
	"encoding/json"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("Test defaults", func() {
	const (
		testConfigmap = "configmap1"
		testNamespace = "default"
	)

	var (
		objectGetter = func() client.Object {
			return &corev1.ConfigMap{}
		}
		req = reconcile.Request{NamespacedName: client.ObjectKey{
			Name:      testConfigmap,
			Namespace: testNamespace,
		}}
	)

	Context("Use default workqueue priority", func() {
		It("Should get default workqueue priority if the item has no annotations", func() {
			err := ensureConfigmap(k8sClient, testNamespace, testConfigmap, DefaultAnnotationWorkQueuePriority, nil)
			Expect(err).NotTo(HaveOccurred())

			getPriorityFunc := DefaultGetPriorityFuncBuilder(k8sClient, objectGetter)
			priority := getPriorityFunc(req)
			Expect(priority).To(Equal(DefaultWorkQueuePriority))
		})

		It("Should get workqueue priority from default item priority annotation", func() {
			err := ensureConfigmap(k8sClient, testNamespace, testConfigmap, DefaultAnnotationWorkQueuePriority, intPtr(3))
			Expect(err).NotTo(HaveOccurred())

			getPriorityFunc := DefaultGetPriorityFuncBuilder(k8sClient, objectGetter)
			priority := getPriorityFunc(req)
			Expect(priority).To(Equal(3))
		})

		It("Should get workqueue priority from default namesapce priority annotation", func() {
			err := ensureConfigmap(k8sClient, testNamespace, testConfigmap, DefaultAnnotationWorkQueuePriority, nil)
			Expect(err).NotTo(HaveOccurred())

			err = ensureNamespace(k8sClient, testNamespace, DefaultAnnotationWorkQueuePriority, intPtr(4))
			Expect(err).NotTo(HaveOccurred())

			getPriorityFunc := DefaultGetPriorityFuncBuilder(k8sClient, objectGetter)
			priority := getPriorityFunc(req)
			Expect(priority).To(Equal(4))
		})
	})

	Context("Use custom workqueue priority", func() {
		It("Should get default workqueue priority if the item has no annotations", func() {
			err := ensureConfigmap(k8sClient, testNamespace, testConfigmap, "custom-priority", nil)
			Expect(err).NotTo(HaveOccurred())

			getPriorityFunc := GetPriorityFuncBuilder(k8sClient, objectGetter, "custom-priority", 1)
			priority := getPriorityFunc(req)
			Expect(priority).To(Equal(1))
		})

		It("Should get workqueue priority from custom item priority annotation", func() {
			err := ensureConfigmap(k8sClient, testNamespace, testConfigmap, "custom-priority", intPtr(3))
			Expect(err).NotTo(HaveOccurred())

			getPriorityFunc := GetPriorityFuncBuilder(k8sClient, objectGetter, "custom-priority", 1)
			priority := getPriorityFunc(req)
			Expect(priority).To(Equal(3))
		})

		It("Should get workqueue priority from custom namesapce priority annotation", func() {
			err := ensureConfigmap(k8sClient, testNamespace, testConfigmap, "custom-priority", nil)
			Expect(err).NotTo(HaveOccurred())

			err = ensureNamespace(k8sClient, testNamespace, "custom-priority", intPtr(4))
			Expect(err).NotTo(HaveOccurred())

			getPriorityFunc := GetPriorityFuncBuilder(k8sClient, objectGetter, "custom-priority", 1)
			priority := getPriorityFunc(req)
			Expect(priority).To(Equal(4))
		})
	})

	Context("Use custom workqueue priority with endTime", func() {
		It("Should get default workqueue priority from custom item priority annotation if the endTime is before now", func() {
			endTime := metav1.NewTime(time.Now().Add(-time.Hour))
			err := ensureConfigmapWithEndTime(k8sClient, testNamespace, testConfigmap, "custom-priority", intPtr(3), &endTime)
			Expect(err).NotTo(HaveOccurred())

			getPriorityFunc := GetPriorityFuncBuilder(k8sClient, objectGetter, "custom-priority", 1)
			priority := getPriorityFunc(req)
			Expect(priority).To(Equal(1))
		})

		It("Should get workqueue priority from custom item priority annotation if the endTime is after now", func() {
			endTime := metav1.NewTime(time.Now().Add(time.Hour))
			err := ensureConfigmapWithEndTime(k8sClient, testNamespace, testConfigmap, "custom-priority", intPtr(3), &endTime)
			Expect(err).NotTo(HaveOccurred())

			getPriorityFunc := GetPriorityFuncBuilder(k8sClient, objectGetter, "custom-priority", 1)
			priority := getPriorityFunc(req)
			Expect(priority).To(Equal(3))
		})

		It("Should get default workqueue priority from custom namesapce priority annotation if the endTime is before now", func() {
			err := ensureConfigmap(k8sClient, testNamespace, testConfigmap, "custom-priority", nil)
			Expect(err).NotTo(HaveOccurred())

			endTime := metav1.NewTime(time.Now().Add(-time.Hour))
			err = ensureNamespaceWithEndTime(k8sClient, testNamespace, "custom-priority", intPtr(4), &endTime)
			Expect(err).NotTo(HaveOccurred())

			getPriorityFunc := GetPriorityFuncBuilder(k8sClient, objectGetter, "custom-priority", 1)
			priority := getPriorityFunc(req)
			Expect(priority).To(Equal(1))
		})

		It("Should get workqueue priority from custom namesapce priority annotation if the endTime is after now", func() {
			err := ensureConfigmap(k8sClient, testNamespace, testConfigmap, "custom-priority", nil)
			Expect(err).NotTo(HaveOccurred())

			endTime := metav1.NewTime(time.Now().Add(time.Hour))
			err = ensureNamespaceWithEndTime(k8sClient, testNamespace, "custom-priority", intPtr(4), &endTime)
			Expect(err).NotTo(HaveOccurred())

			getPriorityFunc := GetPriorityFuncBuilder(k8sClient, objectGetter, "custom-priority", 1)
			priority := getPriorityFunc(req)
			Expect(priority).To(Equal(4))
		})
	})
})

func intPtr(i int) *int {
	return &i
}

func ensureConfigmap(cli client.Client, namespace, name, priorityAnnoKey string, priority *int) error {
	return ensureConfigmapWithEndTime(cli, namespace, name, priorityAnnoKey, priority, nil)
}

func ensureConfigmapWithEndTime(cli client.Client, namespace, name, priorityAnnoKey string, priority *int, endTime *metav1.Time) error {
	// Ensure the configmap exists
	configmap := &corev1.ConfigMap{}
	err := cli.Get(context.Background(), client.ObjectKey{Name: name, Namespace: namespace}, configmap)
	if err != nil {
		if errors.IsNotFound(err) {
			annos := map[string]string{}
			if priority != nil {
				workQueuePriority := &WorkQueuePriority{Priority: priority, EndTime: endTime}
				priorityAnnoValueB, _ := json.Marshal(workQueuePriority)
				annos[priorityAnnoKey] = string(priorityAnnoValueB)
			}
			err := cli.Create(context.Background(), &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:        name,
					Namespace:   namespace,
					Annotations: annos,
				},
			})
			return err
		}
		return err
	}

	_, ok := configmap.Annotations[priorityAnnoKey]
	if priority == nil {
		if !ok {
			return nil
		}
		configmap.Annotations = map[string]string{}
	} else {
		workQueuePriority := &WorkQueuePriority{Priority: priority, EndTime: endTime}
		priorityAnnoValueB, _ := json.Marshal(workQueuePriority)
		configmap.Annotations = map[string]string{
			priorityAnnoKey: string(priorityAnnoValueB),
		}
	}

	// Update the configmap
	err = cli.Update(context.Background(), configmap)
	if err != nil {
		return err
	}
	Eventually(func() bool {
		configmap1 := &corev1.ConfigMap{}
		err := cli.Get(context.Background(), client.ObjectKey{Name: name, Namespace: namespace}, configmap1)
		if err != nil {
			return false
		}
		return configmap1.ResourceVersion >= configmap.ResourceVersion
	}, time.Second*3, time.Millisecond*100).Should(BeTrue())

	return nil
}

func ensureNamespace(cli client.Client, name, priorityAnnoKey string, priority *int) error {
	return ensureNamespaceWithEndTime(cli, name, priorityAnnoKey, priority, nil)
}

func ensureNamespaceWithEndTime(cli client.Client, name, priorityAnnoKey string, priority *int, endTime *metav1.Time) error {
	// Ensure the namespace exists
	namespace := &corev1.Namespace{}
	err := cli.Get(context.Background(), client.ObjectKey{Name: name}, namespace)
	if err != nil {
		if errors.IsNotFound(err) {
			annos := map[string]string{}
			if priority != nil {
				workQueuePriority := &WorkQueuePriority{Priority: priority, EndTime: endTime}
				priorityAnnoValueB, _ := json.Marshal(workQueuePriority)
				annos[priorityAnnoKey] = string(priorityAnnoValueB)
			}
			err := cli.Create(context.Background(), &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:        name,
					Annotations: annos,
				},
			})
			return err
		}
		return err
	}

	_, ok := namespace.Annotations[priorityAnnoKey]
	if priority == nil {
		if !ok {
			return nil
		}
		namespace.Annotations = map[string]string{}
	} else {
		workQueuePriority := &WorkQueuePriority{Priority: priority, EndTime: endTime}
		priorityAnnoValueB, _ := json.Marshal(workQueuePriority)
		namespace.Annotations = map[string]string{
			priorityAnnoKey: string(priorityAnnoValueB),
		}
	}

	// Update the namespace
	err = cli.Update(context.Background(), namespace)
	if err != nil {
		return err
	}
	Eventually(func() bool {
		namespace1 := &corev1.Namespace{}
		err := cli.Get(context.Background(), client.ObjectKey{Name: name}, namespace1)
		if err != nil {
			return false
		}
		return namespace1.ResourceVersion >= namespace.ResourceVersion
	}, time.Second*3, time.Millisecond*100).Should(BeTrue())

	return nil
}
