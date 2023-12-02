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
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ = Describe("Test prioriry_queue", func() {
	const (
		controllerName = "controller1"
		testConfigmap  = "configmap1"
		testNamespace  = "default"
	)

	var (
		objectGetter = func() client.Object {
			return &corev1.ConfigMap{}
		}
	)

	Context("Get lotteries", func() {
		It("Invalid numOfPriorityLotteries", func() {
			_, err := getLotteries([]int{})
			Expect(err).To(HaveOccurred())

			_, err = getLotteries([]int{0})
			Expect(err).To(HaveOccurred())

			_, err = getLotteries([]int{-1})
			Expect(err).To(HaveOccurred())

			_, err = getLotteries([]int{5, 4, 3, 2, 1})
			Expect(err).To(HaveOccurred())

			_, err = getLotteries([]int{0, 1, 2, 3, 4})
			Expect(err).To(HaveOccurred())
		})

		It("Valid numOfPriorityLotteries", func() {
			_, err := getLotteries([]int{1, 2, 3, 4, 5})
			Expect(err).To(Succeed())
		})

		It("Get expected lotteries", func() {
			lotteries, err := getLotteries([]int{1, 2, 3, 4, 5})
			Expect(err).To(Succeed())
			Expect(len(lotteries)).To(Equal(15))
			Expect(lotteries[0]).To(Equal(0))
			Expect(lotteries[1]).To(Equal(1))
			Expect(lotteries[2]).To(Equal(1))
			Expect(lotteries[3]).To(Equal(2))
			Expect(lotteries[4]).To(Equal(2))
			Expect(lotteries[5]).To(Equal(2))
			Expect(lotteries[6]).To(Equal(3))
			Expect(lotteries[7]).To(Equal(3))
			Expect(lotteries[8]).To(Equal(3))
			Expect(lotteries[9]).To(Equal(3))
			Expect(lotteries[10]).To(Equal(4))
			Expect(lotteries[11]).To(Equal(4))
			Expect(lotteries[12]).To(Equal(4))
			Expect(lotteries[13]).To(Equal(4))
			Expect(lotteries[14]).To(Equal(4))
		})
	})

	Context("Shuffle lotteries", func() {
		It("The number of shuffled lotteries is right", func() {
			lotteries, err := getLotteries([]int{1, 2, 3, 4, 5})
			Expect(err).To(Succeed())
			Expect(len(lotteries)).To(Equal(15))

			shuffleLotteries(lotteries)
			count := make(map[int]int)
			for _, lottery := range lotteries {
				count[lottery]++
			}
			Expect(count[0]).To(Equal(1))
			Expect(count[1]).To(Equal(2))
			Expect(count[2]).To(Equal(3))
			Expect(count[3]).To(Equal(4))
			Expect(count[4]).To(Equal(5))
		})

		It("Shuffled lotteries have different lottery sequences", func() {
			lotteries1, err := getLotteries([]int{1, 2, 3, 4, 5, 6, 7, 8})
			Expect(err).To(Succeed())
			Expect(len(lotteries1)).To(Equal(36))

			shuffleLotteries(lotteries1)

			var lotteries2 []int
			copy(lotteries2, lotteries1)
			shuffleLotteries(lotteries2)

			Expect(lotteries1).NotTo(Equal(lotteries2))
		})
	})

	Context("PriorityQueue", func() {
		It("Failed to create PriorityQueue when name is empty", func() {
			cfg := &PriorityQueueConfig{
				GetPriorityFunc: func(obj interface{}) int {
					return 0
				},
				NumOfPriorityLotteries: []int{1, 2, 3, 4, 5},
			}
			_, err := NewPriorityQueue(cfg)
			Expect(err).To(HaveOccurred())
		})

		It("Failed to create PriorityQueue when GetPriorityFunc is nil", func() {
			cfg := &PriorityQueueConfig{
				Name:                   controllerName,
				NumOfPriorityLotteries: []int{1, 2, 3, 4, 5},
			}
			_, err := NewPriorityQueue(cfg)
			Expect(err).To(HaveOccurred())
		})

		It("Failed to create PriorityQueue when NumOfPriorityLotteries is invalid", func() {
			cfg := &PriorityQueueConfig{
				Name:                       controllerName,
				GetPriorityFunc:            func(obj interface{}) int { return 0 },
				NumOfPriorityLotteries:     []int{5, 2, 3, 4, 5},
				UnfinishedWorkUpdatePeriod: 100,
			}
			_, err := NewPriorityQueue(cfg)
			Expect(err).To(HaveOccurred())
		})

		It("Succeed to create PriorityQueue using DefaultNumOfPriorityLotteries", func() {
			cfg := &PriorityQueueConfig{
				Name:                       controllerName,
				GetPriorityFunc:            func(obj interface{}) int { return 0 },
				UnfinishedWorkUpdatePeriod: 100,
			}
			_, err := NewPriorityQueue(cfg)
			Expect(err).NotTo(HaveOccurred())
		})

		It("Succeed to create PriorityQueue", func() {
			cfg := &PriorityQueueConfig{
				Name:                       controllerName,
				GetPriorityFunc:            DefaultGetPriorityFuncBuilder(k8sClient, objectGetter),
				UnfinishedWorkUpdatePeriod: 100,
			}
			priorityQueue, err := NewPriorityQueue(cfg)
			Expect(err).To(Succeed())
			Expect(priorityQueue).NotTo(BeNil())

			err = ensureConfigmap(k8sClient, testNamespace, "configmap1", DefaultAnnotationWorkQueuePriority, intPtr(10))
			Expect(err).NotTo(HaveOccurred())

			priorityQueue.Add(reconcile.Request{NamespacedName: client.ObjectKey{
				Name:      "configmap1",
				Namespace: testNamespace,
			}})
			Expect(priorityQueue.Len()).To(Equal(1))

			item, shutdown := priorityQueue.Get()
			Expect(item).NotTo(BeNil())
			Expect(shutdown).To(BeFalse())

			err = ensureConfigmap(k8sClient, testNamespace, "configmap2", DefaultAnnotationWorkQueuePriority, intPtr(10))
			Expect(err).NotTo(HaveOccurred())

			priorityQueue.Add(reconcile.Request{NamespacedName: client.ObjectKey{
				Name:      "configmap2",
				Namespace: testNamespace,
			}})

			err = ensureConfigmap(k8sClient, testNamespace, "configmap3", DefaultAnnotationWorkQueuePriority, intPtr(10))
			Expect(err).NotTo(HaveOccurred())

			priorityQueue.Add(reconcile.Request{NamespacedName: client.ObjectKey{
				Name:      "configmap3",
				Namespace: testNamespace,
			}})
			Expect(priorityQueue.Len()).To(Equal(2))

			item, shutdown = priorityQueue.Get()
			Expect(item).NotTo(BeNil())
			Expect(shutdown).To(BeFalse())
			Expect(priorityQueue.Len()).To(Equal(1))

			item, shutdown = priorityQueue.Get()
			Expect(item).NotTo(BeNil())
			Expect(shutdown).To(BeFalse())
			Expect(priorityQueue.Len()).To(Equal(0))
		})

		It("Do not discard item when the priority is invalid", func() {
			getPriorityFunc := DefaultGetPriorityFuncBuilder(k8sClient, objectGetter)

			cfg := &PriorityQueueConfig{
				Name:                       controllerName,
				GetPriorityFunc:            getPriorityFunc,
				UnfinishedWorkUpdatePeriod: 100,
			}
			priorityQueue, err := NewPriorityQueue(cfg)
			Expect(err).To(Succeed())
			Expect(priorityQueue).NotTo(BeNil())

			err = ensureConfigmap(k8sClient, testNamespace, testConfigmap, DefaultAnnotationWorkQueuePriority, intPtr(len(DefaultNumOfPriorityLotteries)+1))
			Expect(err).NotTo(HaveOccurred())

			priority := getPriorityFunc(reconcile.Request{NamespacedName: client.ObjectKey{
				Name:      "configmap1",
				Namespace: testNamespace,
			}})
			Expect(priority).To(Equal(len(DefaultNumOfPriorityLotteries) + 1))

			priorityQueue.Add(reconcile.Request{NamespacedName: client.ObjectKey{
				Name:      testConfigmap,
				Namespace: testNamespace,
			}})
			Expect(priorityQueue.Len()).To(Equal(1))
		})

		It("Higher priority items have a higher chance of being processed", func() {
			getPriorityFunc := DefaultGetPriorityFuncBuilder(k8sClient, objectGetter)

			cfg := &PriorityQueueConfig{
				Name:                       controllerName,
				GetPriorityFunc:            getPriorityFunc,
				UnfinishedWorkUpdatePeriod: 100,
			}
			priorityQueue, err := NewPriorityQueue(cfg)
			Expect(err).To(Succeed())
			Expect(priorityQueue).NotTo(BeNil())

			for i := 0; i < 100; i++ {
				name := fmt.Sprintf("confitmap0%d", i)

				err := ensureConfigmap(k8sClient, testNamespace, name, DefaultAnnotationWorkQueuePriority, intPtr(DefaultWorkQueuePriority))
				Expect(err).NotTo(HaveOccurred())

				priorityQueue.Add(reconcile.Request{NamespacedName: client.ObjectKey{
					Name:      name,
					Namespace: testNamespace,
				}})
			}
			Expect(priorityQueue.Len()).To(Equal(100))

			for i := 0; i < 100; i++ {
				name := fmt.Sprintf("confitmap1%d", i)

				err := ensureConfigmap(k8sClient, testNamespace, name, DefaultAnnotationWorkQueuePriority, intPtr(DefaultWorkQueuePriority+1))
				Expect(err).NotTo(HaveOccurred())

				priorityQueue.Add(reconcile.Request{NamespacedName: client.ObjectKey{
					Name:      name,
					Namespace: testNamespace,
				}})
			}
			Expect(priorityQueue.Len()).To(Equal(200))

			for i := 0; i < 100; i++ {
				name := fmt.Sprintf("confitmap2%d", i)

				err := ensureConfigmap(k8sClient, testNamespace, name, DefaultAnnotationWorkQueuePriority, intPtr(DefaultWorkQueuePriority+2))
				Expect(err).NotTo(HaveOccurred())

				priorityQueue.Add(reconcile.Request{NamespacedName: client.ObjectKey{
					Name:      name,
					Namespace: testNamespace,
				}})
			}
			Expect(priorityQueue.Len()).To(Equal(300))

			var (
				finishTime [3]time.Time
				count      [3]int
			)
			for i := 0; i < 300; i++ {
				item, shutdown := priorityQueue.Get()
				Expect(item).NotTo(BeNil())
				Expect(shutdown).To(BeFalse())

				priority := getPriorityFunc(item)
				switch priority {
				case DefaultWorkQueuePriority:
					count[0]++
					if count[0] == 100 {
						finishTime[0] = time.Now()
					}
				case DefaultWorkQueuePriority + 1:
					count[1]++
					if count[1] == 100 {
						finishTime[1] = time.Now()
					}
				case DefaultWorkQueuePriority + 2:
					count[2]++
					if count[2] == 100 {
						finishTime[2] = time.Now()
					}
				}
			}
			Expect(priorityQueue.Len()).To(Equal(0))

			Expect(count[0]).To(Equal(100))
			Expect(count[1]).To(Equal(100))
			Expect(count[2]).To(Equal(100))

			Expect(finishTime[0].After(finishTime[1])).To(BeTrue())
			Expect(finishTime[1].After(finishTime[2])).To(BeTrue())
		})
	})
})
