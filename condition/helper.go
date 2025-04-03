// Copyright 2023 The KusionStack Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package condition

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func NewCondition(ctype string, status metav1.ConditionStatus, reason, message string) *metav1.Condition {
	return &metav1.Condition{
		Type:               ctype,
		Status:             status,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	}
}

func GetCondition(conditions []metav1.Condition, ctype string) *metav1.Condition {
	for i := range conditions {
		c := conditions[i]
		if c.Type == ctype {
			return &c
		}
	}
	return nil
}

func SetCondition(conditions []metav1.Condition, condition metav1.Condition) []metav1.Condition {
	if len(condition.Type) == 0 {
		// invalid input condition
		return conditions
	}
	currentCondition := GetCondition(conditions, condition.Type)
	if currentCondition != nil {
		if conditionEquals(*currentCondition, condition) {
			return conditions
		}
		if currentCondition.Status == condition.Status {
			// inherite LastTransitionTime from current condition
			condition.LastTransitionTime = currentCondition.LastTransitionTime
		}
	}
	result := RejectConditionByType(conditions, condition.Type)
	result = append(result, condition)
	return result
}

func RejectConditionByType(conditions []metav1.Condition, ctype string) []metav1.Condition {
	result := []metav1.Condition{}

	for i := range conditions {
		c := conditions[i]
		if c.Type == ctype {
			continue
		}
		result = append(result, c)
	}
	return result
}

func conditionEquals(a, b metav1.Condition) bool {
	if a.Type == b.Type &&
		a.Status == b.Status &&
		a.Reason == b.Reason &&
		a.Message == b.Message {
		return true
	}
	return false
}
