/*
 Copyright 2023-2025 The KusionStack Authors.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package opslifecycle

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"kusionstack.io/kube-utils/xset/api"
)

// IDToLabelsMap returns a map of pod id to labels map and a map of operation type to number of pods.
func IDToLabelsMap(target client.Object) (map[string]map[string]string, map[string]int, error) {
	idToLabelsMap := map[string]map[string]string{}
	typeToNumsMap := map[string]int{}

	ids := sets.String{}
	labels := target.GetLabels()
	for k := range labels {
		if strings.HasPrefix(k, TargetOperatingLabelPrefix) ||
			strings.HasPrefix(k, TargetOperateLabelPrefix) ||
			strings.HasPrefix(k, TargetOperatedLabelPrefix) {
			s := strings.Split(k, "/")
			if len(s) < 2 {
				return nil, nil, fmt.Errorf("invalid label %s", k)
			}
			ids.Insert(s[1])
		}
	}

	for id := range ids {
		for _, val := range []string{TargetOperationTypeLabelPrefix, TargetDoneOperationTypeLabelPrefix} {
			operationType, ok := labels[fmt.Sprintf("%s/%s", val, id)]
			if !ok {
				continue
			}

			if _, ok := typeToNumsMap[operationType]; !ok {
				typeToNumsMap[operationType] = 1
			} else {
				typeToNumsMap[operationType] += 1
			}
			break
		}

		for _, prefix := range WellKnownLabelPrefixesWithID {
			label := fmt.Sprintf("%s/%s", prefix, id)
			value, ok := labels[label]
			if !ok {
				continue
			}

			labelsMap, ok := idToLabelsMap[id]
			if !ok {
				labelsMap = make(map[string]string)
				idToLabelsMap[id] = labelsMap
			}
			labelsMap[prefix] = value
		}
	}
	return idToLabelsMap, typeToNumsMap, nil
}

// NumOfLifecycleOnTarget returns the nums of lifecycles on pod
func NumOfLifecycleOnTarget(target client.Object) (int, error) {
	if target == nil {
		return 0, nil
	}
	newIDToLabelsMap, _, err := IDToLabelsMap(target)
	return len(newIDToLabelsMap), err
}

var WhenBeginDelete api.UpdateFunc = func(obj client.Object) (bool, error) {
	return AddLabel(obj, TargetPreparingDeleteLabel, strconv.FormatInt(time.Now().UnixNano(), 10)), nil
}

func AddLabel(po client.Object, k, v string) bool {
	labels := po.GetLabels()
	if labels == nil {
		labels = map[string]string{}
		po.SetLabels(labels)
	}
	if _, ok := labels[k]; !ok {
		labels[k] = v
		return true
	}
	return false
}

// IsDuringOps decides whether the Target is during ops or not
// DuringOps means the Target's OpsLifecycle phase is in or after PreCheck phase and before Finish phase.
func IsDuringOps(adapter api.LifecycleAdapter, obj client.Object) bool {
	_, hasID := checkOperatingID(adapter, obj)
	_, hasType := checkOperationType(adapter, obj)

	return hasID && hasType
}

// Begin is used for an CRD Operator to begin a lifecycle
func Begin(c client.Client, adapter api.LifecycleAdapter, obj client.Object, updateFunc ...api.UpdateFunc) (updated bool, err error) {
	if obj.GetLabels() == nil {
		obj.SetLabels(map[string]string{})
	}

	operatingID, hasID := checkOperatingID(adapter, obj)
	operationType, hasType := checkOperationType(adapter, obj)
	var needUpdate bool

	// ensure operatingID and operationType
	if hasID && hasType {
		if operationType != adapter.GetType() {
			err = fmt.Errorf("operatingID %s already has operationType %s", operatingID, operationType)
			return false, err
		}
	} else {
		// check another id/type = this.type
		currentTypeIDs := queryByOperationType(adapter, obj)
		if currentTypeIDs != nil && currentTypeIDs.Len() > 0 && !adapter.AllowMultiType() {
			err = fmt.Errorf("operationType %s exists: %v", adapter.GetType(), currentTypeIDs)
			return updated, err
		}

		if !hasID {
			needUpdate = true
			setOperatingID(adapter, obj)
		}
		if !hasType {
			needUpdate = true
			setOperationType(adapter, obj)
		}
	}

	updated, err = DefaultUpdateAll(obj, append(updateFunc, adapter.WhenBegin)...)
	if err != nil {
		return updated, err
	}

	if needUpdate || updated {
		err = c.Update(context.Background(), obj)
		return err == nil, err
	}

	return false, nil
}

// BeginWithCleaningOld is used for an CRD Operator to begin a lifecycle with cleaning the old lifecycle
func BeginWithCleaningOld(c client.Client, adapter api.LifecycleAdapter, obj client.Object, updateFunc ...api.UpdateFunc) (updated bool, err error) {
	if targetInUpdateLifecycle, err := IsLifecycleOnTarget(adapter.GetID(), obj); err != nil {
		return false, fmt.Errorf("fail to check %s TargetOpsLifecycle on Target %s/%s: %w", adapter.GetID(), obj.GetNamespace(), obj.GetName(), err)
	} else if targetInUpdateLifecycle {
		if err := Undo(c, adapter, obj); err != nil {
			return false, err
		}
	}
	return Begin(c, adapter, obj, updateFunc...)
}

// AllowOps is used to check whether the TargetOpsLifecycle phase is in UPGRADE to do following operations.
func AllowOps(adapter api.LifecycleAdapter, operationDelaySeconds int32, obj client.Object) (requeueAfter *time.Duration, allow bool) {
	if !IsDuringOps(adapter, obj) {
		return nil, false
	}

	startedTimestampStr, started := checkOperate(adapter, obj)
	if !started || operationDelaySeconds <= 0 {
		return nil, started
	}

	startedTimestamp, err := strconv.ParseInt(startedTimestampStr, 10, 64)
	if err != nil {
		return nil, started
	}

	startedTime := time.Unix(0, startedTimestamp)
	duration := time.Since(startedTime)
	delay := time.Duration(operationDelaySeconds) * time.Second
	if duration < delay {
		du := delay - duration
		return &du, started
	}

	return nil, started
}

// Finish is used for an CRD Operator to finish a lifecycle
func Finish(c client.Client, adapter api.LifecycleAdapter, obj client.Object, updateFuncs ...api.UpdateFunc) (updated bool, err error) {
	operatingID, hasID := checkOperatingID(adapter, obj)
	operationType, hasType := checkOperationType(adapter, obj)

	if hasType && operationType != adapter.GetType() {
		return false, fmt.Errorf("operatingID %s has invalid operationType %s", operatingID, operationType)
	}

	var needUpdate bool
	if hasID || hasType {
		needUpdate = true
		deleteOperatingID(adapter, obj)
	}

	updated, err = DefaultUpdateAll(obj, append(updateFuncs, adapter.WhenFinish)...)
	if err != nil {
		return
	}
	if needUpdate || updated {
		err = c.Update(context.Background(), obj)
		return err == nil, err
	}

	return false, err
}

// Undo is used for an CRD Operator to undo a lifecycle
func Undo(c client.Client, adapter api.LifecycleAdapter, obj client.Object) error {
	setUndo(adapter, obj)
	return c.Update(context.Background(), obj)
}

func checkOperatingID(adapter api.LifecycleAdapter, obj client.Object) (val string, ok bool) {
	labelID := fmt.Sprintf("%s/%s", TargetOperatingLabelPrefix, adapter.GetID())
	_, ok = obj.GetLabels()[labelID]
	return adapter.GetID(), ok
}

func checkOperationType(adapter api.LifecycleAdapter, obj client.Object) (val api.OperationType, ok bool) {
	labelType := fmt.Sprintf("%s/%s", TargetOperationTypeLabelPrefix, adapter.GetID())
	labelVal := obj.GetLabels()[labelType]
	val = api.OperationType(labelVal)
	return val, val == adapter.GetType()
}

func checkOperate(adapter api.LifecycleAdapter, obj client.Object) (val string, ok bool) {
	labelOperate := fmt.Sprintf("%s/%s", TargetOperateLabelPrefix, adapter.GetID())
	val, ok = obj.GetLabels()[labelOperate]
	return
}

func setOperatingID(adapter api.LifecycleAdapter, obj client.Object) {
	labelID := fmt.Sprintf("%s/%s", TargetOperatingLabelPrefix, adapter.GetID())
	obj.GetLabels()[labelID] = fmt.Sprintf("%d", time.Now().UnixNano())
	return
}

func setOperationType(adapter api.LifecycleAdapter, obj client.Object) {
	labelType := fmt.Sprintf("%s/%s", TargetOperationTypeLabelPrefix, adapter.GetID())
	obj.GetLabels()[labelType] = string(adapter.GetType())
	return
}

// setOperate only for test
func setOperate(adapter api.LifecycleAdapter, obj client.Object) {
	labelOperate := fmt.Sprintf("%s/%s", TargetOperateLabelPrefix, adapter.GetID())
	now := time.Now().UnixNano()
	obj.GetLabels()[labelOperate] = strconv.FormatInt(now, 10)
	return
}

func setUndo(adapter api.LifecycleAdapter, obj client.Object) {
	labelUndo := fmt.Sprintf("%s/%s", TargetUndoOperationTypeLabelPrefix, adapter.GetID())
	obj.GetLabels()[labelUndo] = string(adapter.GetType())
}

func deleteOperatingID(adapter api.LifecycleAdapter, obj client.Object) {
	labelID := fmt.Sprintf("%s/%s", TargetOperatingLabelPrefix, adapter.GetID())
	delete(obj.GetLabels(), labelID)
	return
}

func queryByOperationType(adapter api.LifecycleAdapter, obj client.Object) sets.String {
	res := sets.String{}
	valType := adapter.GetType()

	for k, v := range obj.GetLabels() {
		if strings.HasPrefix(k, TargetOperationTypeLabelPrefix) && v == string(valType) {
			res.Insert(k)
		}
	}

	return res
}

func DefaultUpdateAll(target client.Object, updateFuncs ...api.UpdateFunc) (updated bool, err error) {
	for _, updateFunc := range updateFuncs {
		ok, updateErr := updateFunc(target)
		if updateErr != nil {
			return updated, updateErr
		}
		updated = updated || ok
	}
	return updated, nil
}

func IsLifecycleOnTarget(operatingID string, target client.Object) (bool, error) {
	if target == nil {
		return false, fmt.Errorf("nil Target")
	}

	labels := target.GetLabels()
	if labels == nil {
		return false, nil
	}

	if val, ok := labels[fmt.Sprintf("%s/%s", TargetOperatingLabelPrefix, operatingID)]; ok {
		return val != "", nil
	}

	return false, nil
}
