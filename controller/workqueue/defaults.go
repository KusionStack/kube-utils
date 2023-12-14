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
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
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
	return GetPriorityFuncBuilder(cli, DefaultWorkQueuePriorityLabel, DefaultWorkQueuePriority)
}

// GetPriorityFunc is the function to get the priority of an item
// We use the namespace label to get the priority of an item
func GetPriorityFuncBuilder(cli client.Client, workQueuePriorityLabel string, defaultWorkQueuePriority int) GetPriorityFunc {
	if cli == nil {
		panic("cli is required")
	}
	if workQueuePriorityLabel == "" {
		panic("workQueuePriorityLabel is required")
	}

	return func(item interface{}) int {
		req, ok := item.(reconcile.Request)
		if !ok || req.Namespace == "" {
			return defaultWorkQueuePriority
		}

		var priorityLableValue string
		namespace := &corev1.Namespace{}
		if err := cli.Get(context.Background(), client.ObjectKey{Name: req.Namespace}, namespace); err != nil {
			klog.Errorf("Failed to get namespace: %v, error: %v", req.Namespace, err)
			return defaultWorkQueuePriority
		} else {
			labels := namespace.GetLabels()
			if len(labels) == 0 {
				return defaultWorkQueuePriority
			}
			priorityLableValue, ok = namespace.Labels[workQueuePriorityLabel]
			if !ok {
				return defaultWorkQueuePriority
			}
		}

		priority, err := strconv.Atoi(priorityLableValue)
		if err != nil {
			klog.Errorf("Failed to convert label value: %q to int, error: %v", priorityLableValue, err)
			return defaultWorkQueuePriority
		}
		return priority
	}
}
