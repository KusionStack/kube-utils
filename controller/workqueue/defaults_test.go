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
		It("Should get default workqueue priority if the item has no labels", func() {
			err := ensureConfigmap(k8sClient, testNamespace, testConfigmap, DefaultWorkQueuePriorityLabel, "")
			Expect(err).NotTo(HaveOccurred())

			getPriorityFunc := DefaultGetPriorityFuncBuilder(k8sClient, objectGetter)
			priority := getPriorityFunc(req)
			Expect(priority).To(Equal(DefaultWorkQueuePriority))
		})

		It("Should get workqueue priority from default item priority label", func() {
			err := ensureConfigmap(k8sClient, testNamespace, testConfigmap, DefaultWorkQueuePriorityLabel, "3")
			Expect(err).NotTo(HaveOccurred())

			getPriorityFunc := DefaultGetPriorityFuncBuilder(k8sClient, objectGetter)
			priority := getPriorityFunc(req)
			Expect(priority).To(Equal(3))
		})

		It("Should get workqueue priority from default namesapce priority label", func() {
			err := ensureConfigmap(k8sClient, testNamespace, testConfigmap, DefaultWorkQueuePriorityLabel, "")
			Expect(err).NotTo(HaveOccurred())

			err = ensureNamespace(k8sClient, testNamespace, DefaultWorkQueuePriorityLabel, "4")
			Expect(err).NotTo(HaveOccurred())

			getPriorityFunc := DefaultGetPriorityFuncBuilder(k8sClient, objectGetter)
			priority := getPriorityFunc(req)
			Expect(priority).To(Equal(4))
		})
	})

	Context("Use custom workqueue priority", func() {
		It("Should get default workqueue priority if the item has no labels", func() {
			err := ensureConfigmap(k8sClient, testNamespace, testConfigmap, "custom-priority", "")
			Expect(err).NotTo(HaveOccurred())

			getPriorityFunc := GetPriorityFuncBuilder(k8sClient, objectGetter, "custom-priority", 1)
			priority := getPriorityFunc(req)
			Expect(priority).To(Equal(1))
		})

		It("Should get workqueue priority from custom item priority label", func() {
			err := ensureConfigmap(k8sClient, testNamespace, testConfigmap, "custom-priority", "3")
			Expect(err).NotTo(HaveOccurred())

			getPriorityFunc := GetPriorityFuncBuilder(k8sClient, objectGetter, "custom-priority", 1)
			priority := getPriorityFunc(req)
			Expect(priority).To(Equal(3))
		})

		It("Should get workqueue priority from custom namesapce priority label", func() {
			err := ensureConfigmap(k8sClient, testNamespace, testConfigmap, "custom-priority", "")
			Expect(err).NotTo(HaveOccurred())

			err = ensureNamespace(k8sClient, testNamespace, "custom-priority", "4")
			Expect(err).NotTo(HaveOccurred())

			getPriorityFunc := GetPriorityFuncBuilder(k8sClient, objectGetter, "custom-priority", 1)
			priority := getPriorityFunc(req)
			Expect(priority).To(Equal(4))
		})
	})
})

func ensureConfigmap(cli client.Client, namespace, name, priorityLabelKey, priorityLabelValue string) error {
	// Ensure the configmap exists
	configmap := &corev1.ConfigMap{}
	err := cli.Get(context.Background(), client.ObjectKey{Name: name, Namespace: namespace}, configmap)
	if err != nil {
		if errors.IsNotFound(err) {
			labels := map[string]string{}
			if priorityLabelValue != "" {
				labels[priorityLabelKey] = priorityLabelValue
			}

			err := cli.Create(context.Background(), &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: namespace,
					Labels:    labels,
				},
			})
			return err
		}
		return err
	}

	// If the label is already set, we don't need to update it
	labelValue, ok := configmap.Labels[priorityLabelKey]
	if !ok && priorityLabelValue == "" {
		return nil
	} else if ok && labelValue == priorityLabelValue {
		return nil
	}

	// If the label is not set, we need to set it
	if priorityLabelValue == "" {
		configmap.Labels = map[string]string{}
	} else {
		configmap.Labels = map[string]string{
			priorityLabelKey: priorityLabelValue,
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

func ensureNamespace(cli client.Client, name, priorityLabelKey, priorityLabelValue string) error {
	// Ensure the namespace exists
	namespace := &corev1.Namespace{}
	err := cli.Get(context.Background(), client.ObjectKey{Name: name}, namespace)
	if err != nil {
		if errors.IsNotFound(err) {
			labels := map[string]string{}
			if priorityLabelValue != "" {
				labels[priorityLabelKey] = priorityLabelValue
			}
			err := cli.Create(context.Background(), &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   name,
					Labels: labels,
				},
			})
			return err
		}
		return err
	}

	// If the label is already set, we don't need to update it
	labelValue, ok := namespace.Labels[priorityLabelKey]
	if !ok && priorityLabelValue == "" {
		return nil
	} else if ok && labelValue == priorityLabelValue {
		return nil
	}

	// If the label is not set, we need to set it
	if priorityLabelValue == "" {
		namespace.Labels = map[string]string{}
	} else {
		namespace.Labels = map[string]string{
			priorityLabelKey: priorityLabelValue,
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
