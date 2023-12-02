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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	DefaultAnnotationWorkQueuePriority = "kusionstack.io/workqueue-priority"
	DefaultUnfinishedWorkUpdatePeriod  = 500 * time.Millisecond
	DefaultWorkQueuePriority           = 2
)

var (
	DefaultNumOfPriorityLotteries = []int{1, 2, 4, 8, 16}
)

type WorkQueuePriority struct {
	Priority *int         `json:"priority,omitempty"`
	EndTime  *metav1.Time `json:"endTime,omitempty"`
}

func DefaultGetPriorityFuncBuilder(cli client.Client, objectGetter func() client.Object) GetPriorityFunc {
	return GetPriorityFuncBuilder(cli, objectGetter, DefaultAnnotationWorkQueuePriority, DefaultWorkQueuePriority)
}

// GetPriorityFunc is the function to get the priority of an item
// We use the annotation to get the priority of an item
// If the annotation is not set in the item, we will get the priority from the namespace annotation
func GetPriorityFuncBuilder(cli client.Client, objectGetter func() client.Object, annotationWorkQueuePriority string, defaultWorkQueuePriority int) GetPriorityFunc {
	if cli == nil {
		panic("cli is required")
	}
	if annotationWorkQueuePriority == "" {
		panic("annotationWorkQueuePriority is required")
	}

	return func(item interface{}) int {
		req, ok := item.(reconcile.Request)
		if !ok {
			return defaultWorkQueuePriority
		}

		// Get the priority from the item annotation
		object := objectGetter()
		err := cli.Get(context.Background(), req.NamespacedName, object)
		if err != nil {
			klog.Errorf("Failed to get object: %v, error: %v", req.NamespacedName, err)
			return defaultWorkQueuePriority
		}

		var priorityAnnoValue string
		annos := object.GetAnnotations()
		if len(annos) != 0 {
			priorityAnnoValue = annos[annotationWorkQueuePriority]
		}

		// Get the priority from the namespace annotation
		if priorityAnnoValue == "" {
			if req.Namespace == "" {
				return defaultWorkQueuePriority
			}

			namespace := &corev1.Namespace{}
			if err := cli.Get(context.Background(), client.ObjectKey{Name: req.Namespace}, namespace); err != nil {
				klog.Errorf("Failed to get namespace: %v, error: %v", req.Namespace, err)
				return defaultWorkQueuePriority
			} else {
				annos := namespace.GetAnnotations()
				if len(annos) == 0 {
					return defaultWorkQueuePriority
				}
				priorityAnnoValue, ok = annos[annotationWorkQueuePriority]
				if !ok {
					return defaultWorkQueuePriority
				}
			}
		}

		// Check if the priority is valid
		var workQueuePriority WorkQueuePriority
		if err := json.Unmarshal([]byte(priorityAnnoValue), &workQueuePriority); err != nil {
			klog.Errorf("Failed to unmarshal annotation value: %q, error: %v", priorityAnnoValue, err)
			return defaultWorkQueuePriority
		}

		// If endTime is not set, or endTime is set and currentTime is before endTime, we will use the priority
		currentTime := metav1.NewTime(time.Now())
		if workQueuePriority.EndTime == nil ||
			(workQueuePriority.EndTime != nil && currentTime.Before(workQueuePriority.EndTime)) {

			if workQueuePriority.Priority != nil && *workQueuePriority.Priority >= 0 {
				return *workQueuePriority.Priority
			}
		}
		return defaultWorkQueuePriority
	}
}
