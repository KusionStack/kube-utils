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
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	DefaultWorkQueuePriorityLabel     = "kusionstack.io/workqueue-priority"
	DefaultUnfinishedWorkUpdatePeriod = 500 * time.Millisecond
	DefaultWorkQueuePriority          = 2
)

var (
	DefaultNumOfPriorityLotteries = []int{1, 2, 4, 8, 16}
)

func DefaultGetPriorityFuncBuilder(cli client.Client) GetPriorityFunc {
	return GetPriorityFuncBuilder(cli, DefaultWorkQueuePriority, DefaultWorkQueuePriorityLabel)
}

// GetPriorityFunc is the function to get the priority of an item
// We use the label to get the priority of an item
// If the label is not set in the item, we will get the priority from the namespace label
func GetPriorityFuncBuilder(cli client.Client, defaultWorkQueuePriority int, workQueuePriorityLabel string) GetPriorityFunc {
	if cli == nil {
		panic("cli is required")
	}
	if workQueuePriorityLabel == "" {
		panic("workQueuePriorityLabel is required")
	}

	return func(item interface{}) int {
		clientObject, ok := item.(client.Object)
		if !ok {
			return defaultWorkQueuePriority
		}

		var priorityLableValue string
		labels := clientObject.GetLabels()
		if len(labels) != 0 {
			priorityLableValue = labels[workQueuePriorityLabel]
		}

		if priorityLableValue == "" {
			name := clientObject.GetNamespace()
			if name == "" {
				return defaultWorkQueuePriority
			}

			namespace := &corev1.Namespace{}
			if err := cli.Get(context.Background(), client.ObjectKey{Name: name}, namespace); err != nil {
				klog.V(4).ErrorS(err, "Failed to get namespace", "namespace", name)
				return defaultWorkQueuePriority
			} else {
				labels := namespace.GetLabels()
				if len(labels) == 0 {
					return defaultWorkQueuePriority
				}
				priorityLableValue = namespace.Labels[workQueuePriorityLabel]
			}
		}

		if priorityLableValue == "" {
			return defaultWorkQueuePriority
		}

		priority, err := strconv.Atoi(priorityLableValue)
		if err != nil {
			klog.V(4).ErrorS(err, "Failed to convert label value to int", "priorityLableValue", priorityLableValue)
			return defaultWorkQueuePriority
		}
		return priority
	}
}
