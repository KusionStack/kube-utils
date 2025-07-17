/*
 * Copyright 2024-2025 KusionStack Authors.
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

package synccontrols

import (
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"kusionstack.io/kube-api/apps/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"kusionstack.io/kube-utils/xset/api"
)

func GetInstanceID(target client.Object) (int, error) {
	if target.GetLabels() == nil {
		return -1, fmt.Errorf("no labels found for instance ID")
	}

	val, exist := target.GetLabels()[TargetInstanceIDLabelKey]
	if !exist {
		return -1, fmt.Errorf("failed to find instance ID label %s", TargetInstanceIDLabelKey)
	}

	id, err := strconv.ParseInt(val, 10, 32)
	if err != nil {
		// ignore invalid target instance ID
		return -1, fmt.Errorf("failed to parse instance ID with value %s: %w", val, err)
	}

	return int(id), nil
}

func NewTargetFrom(setController api.XSetController, owner api.XSetObject, revision *appsv1.ControllerRevision, id int, updateFuncs ...func(client.Object) error) (client.Object, error) {
	targetObj, err := setController.GetXObjectFromRevision(revision)
	if err != nil {
		return nil, err
	}

	meta := setController.XSetMeta()
	ownerRef := metav1.NewControllerRef(owner, meta.GroupVersionKind())
	targetObj.SetOwnerReferences(append(targetObj.GetOwnerReferences(), *ownerRef))
	targetObj.SetNamespace(owner.GetNamespace())
	targetObj.SetGenerateName(GetTargetsPrefix(owner.GetName()))

	labels := targetObj.GetLabels()
	labels[TargetInstanceIDLabelKey] = fmt.Sprintf("%d", id)
	labels[appsv1.ControllerRevisionHashLabelKey] = revision.GetName()
	controlByKusionStack(targetObj)

	for _, fn := range updateFuncs {
		if err := fn(targetObj); err != nil {
			return targetObj, err
		}
	}

	return targetObj, nil
}

func RealValue(val *int32) int32 {
	if val == nil {
		return 0
	}

	return *val
}

const ConditionUpdatePeriodBackOff = 30 * time.Second

func AddOrUpdateCondition(status *api.XSetStatus, conditionType api.XSetConditionType, err error, reason, message string) {
	condStatus := metav1.ConditionTrue
	if err != nil {
		condStatus = metav1.ConditionFalse
	}

	existCond := GetCondition(status, string(conditionType))
	if existCond != nil && existCond.Reason == reason && existCond.Status == condStatus {
		now := metav1.Now()
		if now.Sub(existCond.LastTransitionTime.Time) < ConditionUpdatePeriodBackOff {
			return
		}
	}

	cond := NewCondition(string(conditionType), condStatus, reason, message)
	SetCondition(status, cond)
}

func NewCondition(condType string, status metav1.ConditionStatus, reason, msg string) *metav1.Condition {
	return &metav1.Condition{
		Type:               condType,
		Status:             status,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            msg,
	}
}

// SetCondition adds/replaces the given condition in the replicaset status. If the condition that we
// are about to add already exists and has the same status and reason then we are not going to update.
func SetCondition(status *api.XSetStatus, condition *metav1.Condition) {
	currentCond := GetCondition(status, condition.Type)
	if currentCond != nil && currentCond.Status == condition.Status && currentCond.Reason == condition.Reason && currentCond.LastTransitionTime == condition.LastTransitionTime {
		return
	}
	newConditions := filterOutCondition(status.Conditions, condition.Type)
	newConditions = append(newConditions, *condition)
	status.Conditions = newConditions
}

// GetCondition returns a inplace set condition with the provided type if it exists.
func GetCondition(status *api.XSetStatus, condType string) *metav1.Condition {
	for _, c := range status.Conditions {
		if c.Type == condType {
			return &c
		}
	}
	return nil
}

func GetTargetsPrefix(controllerName string) string {
	// use the dash (if the name isn't too long) to make the target name a bit prettier
	prefix := fmt.Sprintf("%s-", controllerName)
	if len(apimachineryvalidation.NameIsDNSSubdomain(prefix, true)) != 0 {
		prefix = controllerName
	}
	return prefix
}

func IsTargetUpdatedRevision(target client.Object, revision string) bool {
	if target.GetLabels() == nil {
		return false
	}

	return target.GetLabels()[appsv1.ControllerRevisionHashLabelKey] == revision
}

func ObjectKeyString(obj client.Object) string {
	if obj.GetNamespace() == "" {
		return obj.GetName()
	}
	return obj.GetNamespace() + "/" + obj.GetName()
}

// filterOutCondition returns a new slice of replicaset conditions without conditions with the provided type.
func filterOutCondition(conditions []metav1.Condition, condType string) []metav1.Condition {
	var newConditions []metav1.Condition
	for _, c := range conditions {
		if c.Type == condType {
			continue
		}
		newConditions = append(newConditions, c)
	}
	return newConditions
}

func controlByKusionStack(obj client.Object) {
	if obj.GetLabels() == nil {
		obj.SetLabels(map[string]string{})
	}

	if v, ok := obj.GetLabels()[v1alpha1.ControlledByKusionStackLabelKey]; !ok || v != "true" {
		obj.GetLabels()[v1alpha1.ControlledByKusionStackLabelKey] = "true"
	}
}
