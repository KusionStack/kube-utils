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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ = Describe("Test defaults", func() {
	const (
		testObject    = "object1"
		testNamespace = "default"
	)

	req := reconcile.Request{NamespacedName: client.ObjectKey{
		Name:      testObject,
		Namespace: testNamespace,
	}}

	Context("Use default workqueue priority label", func() {
		It("Should get default priority if default workqueue priority label nil", func() {
			err := ensureNamespace(k8sClient, testNamespace, DefaultWorkQueuePriorityLabel, "")
			Expect(err).NotTo(HaveOccurred())

			getPriorityFunc := DefaultGetPriorityFuncBuilder(k8sClient)
			priority := getPriorityFunc(req)
			Expect(priority).To(Equal(DefaultWorkQueuePriority))
		})

		It("Should get workqueue priority from default workqueue priority label", func() {
			err := ensureNamespace(k8sClient, testNamespace, DefaultWorkQueuePriorityLabel, "4")
			Expect(err).NotTo(HaveOccurred())

			getPriorityFunc := DefaultGetPriorityFuncBuilder(k8sClient)
			priority := getPriorityFunc(req)
			Expect(priority).To(Equal(4))
		})
	})

	Context("Use custom workqueue priority label", func() {
		It("Should get default priority if custom workqueue priority label nil", func() {
			err := ensureNamespace(k8sClient, testNamespace, "custom-priority", "4")
			Expect(err).NotTo(HaveOccurred())

			getPriorityFunc := GetPriorityFuncBuilder(k8sClient, "custom-priority", 1)
			priority := getPriorityFunc(req)
			Expect(priority).To(Equal(4))
		})

		It("Should get workqueue priority from custom workqueue priority label", func() {
			err := ensureNamespace(k8sClient, testNamespace, "custom-priority", "4")
			Expect(err).NotTo(HaveOccurred())

			getPriorityFunc := GetPriorityFuncBuilder(k8sClient, "custom-priority", 1)
			priority := getPriorityFunc(req)
			Expect(priority).To(Equal(4))
		})
	})
})

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
	priority, ok := namespace.Labels[priorityLabelKey]
	if priorityLabelValue == "" {
		if !ok {
			return nil
		}
		namespace.Labels = map[string]string{}
	} else {
		if ok && priority == priorityLabelValue {
			return nil
		}
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
