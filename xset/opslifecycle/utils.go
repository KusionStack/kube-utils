/*
 Copyright 2024-2025 The KusionStack Authors.

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

type UpdateFunc func(object client.Object) (bool, error)

// IDToLabelsMap returns a map of pod id to labels map and a map of operation type to number of pods.
func IDToLabelsMap(m *LabelManagerImpl, target client.Object) (map[string]map[string]string, map[string]int, error) {
	idToLabelsMap := map[string]map[string]string{}
	typeToNumsMap := map[string]int{}

	ids := sets.String{}
	labels := target.GetLabels()
	for k := range labels {
		if strings.HasPrefix(k, m.Get(api.OperatingLabelPrefix)) ||
			strings.HasPrefix(k, m.Get(api.OperateLabelPrefix)) {
			s := strings.Split(k, "/")
			if len(s) < 2 {
				return nil, nil, fmt.Errorf("invalid label %s", k)
			}
			ids.Insert(s[1])
		}
	}

	for id := range ids {
		if operationType, ok := labels[fmt.Sprintf("%s/%s", m.Get(api.OperationTypeLabelPrefix), id)]; ok {
			typeToNumsMap[operationType] += 1
		}

		for _, prefix := range m.wellKnownLabelPrefixesWithID {
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
func NumOfLifecycleOnTarget(m *LabelManagerImpl, target client.Object) (int, error) {
	if target == nil {
		return 0, nil
	}
	newIDToLabelsMap, _, err := IDToLabelsMap(m, target)
	return len(newIDToLabelsMap), err
}

func WhenBeginDelete(m api.LifeCycleLabelManager, obj client.Object) (bool, error) {
	return AddLabel(obj, m.Get(api.PreparingDeleteLabel), strconv.FormatInt(time.Now().UnixNano(), 10)), nil
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
func IsDuringOps(m api.LifeCycleLabelManager, adapter api.LifecycleAdapter, obj client.Object) bool {
	_, hasID := checkOperatingID(m, adapter, obj)
	_, hasType := checkOperationType(m, adapter, obj)

	return hasID && hasType
}

// Begin is used for an CRD Operator to begin a lifecycle
func Begin(m api.LifeCycleLabelManager, c client.Client, adapter api.LifecycleAdapter, obj client.Object, updateFuncs ...UpdateFunc) (updated bool, err error) {
	if obj.GetLabels() == nil {
		obj.SetLabels(map[string]string{})
	}

	operatingID, hasID := checkOperatingID(m, adapter, obj)
	operationType, hasType := checkOperationType(m, adapter, obj)
	var needUpdate bool

	// ensure operatingID and operationType
	if hasID && hasType {
		if operationType != adapter.GetType() {
			err = fmt.Errorf("operatingID %s already has operationType %s", operatingID, operationType)
			return false, err
		}
	} else {
		// check another id/type = this.type
		currentTypeIDs := queryByOperationType(m, adapter, obj)
		if currentTypeIDs != nil && currentTypeIDs.Len() > 0 && !adapter.AllowMultiType() {
			err = fmt.Errorf("operationType %s exists: %v", adapter.GetType(), currentTypeIDs)
			return updated, err
		}

		if !hasID {
			needUpdate = true
			setOperatingID(m, adapter, obj)
		}
		if !hasType {
			needUpdate = true
			setOperationType(m, adapter, obj)
		}
	}

	updated, err = DefaultUpdateAll(obj, append(updateFuncs, adapter.WhenBegin)...)
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
func BeginWithCleaningOld(m api.LifeCycleLabelManager, c client.Client, adapter api.LifecycleAdapter, obj client.Object, updateFunc ...UpdateFunc) (updated bool, err error) {
	if targetInUpdateLifecycle, err := IsLifecycleOnTarget(m, adapter.GetID(), obj); err != nil {
		return false, fmt.Errorf("fail to check %s TargetOpsLifecycle on Target %s/%s: %w", adapter.GetID(), obj.GetNamespace(), obj.GetName(), err)
	} else if targetInUpdateLifecycle {
		if err := Undo(m, c, adapter, obj); err != nil {
			return false, err
		}
	}
	return Begin(m, c, adapter, obj, updateFunc...)
}

// AllowOps is used to check whether the TargetOpsLifecycle phase is in UPGRADE to do following operations.
func AllowOps(m api.LifeCycleLabelManager, adapter api.LifecycleAdapter, operationDelaySeconds int32, obj client.Object) (requeueAfter *time.Duration, allow bool) {
	if !IsDuringOps(m, adapter, obj) {
		return nil, false
	}

	startedTimestampStr, started := checkOperate(m, adapter, obj)
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
func Finish(m api.LifeCycleLabelManager, c client.Client, adapter api.LifecycleAdapter, obj client.Object, updateFuncs ...UpdateFunc) (updated bool, err error) {
	operatingID, hasID := checkOperatingID(m, adapter, obj)
	operationType, hasType := checkOperationType(m, adapter, obj)

	if hasType && operationType != adapter.GetType() {
		return false, fmt.Errorf("operatingID %s has invalid operationType %s", operatingID, operationType)
	}

	var needUpdate bool
	if hasID || hasType {
		needUpdate = true
		deleteOperatingID(m, adapter, obj)
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
func Undo(m api.LifeCycleLabelManager, c client.Client, adapter api.LifecycleAdapter, obj client.Object) error {
	setUndo(m, adapter, obj)
	return c.Update(context.Background(), obj)
}

func checkOperatingID(m api.LifeCycleLabelManager, adapter api.LifecycleAdapter, obj client.Object) (val string, ok bool) {
	labelID := fmt.Sprintf("%s/%s", m.Get(api.OperatingLabelPrefix), adapter.GetID())
	_, ok = obj.GetLabels()[labelID]
	return adapter.GetID(), ok
}

func checkOperationType(m api.LifeCycleLabelManager, adapter api.LifecycleAdapter, obj client.Object) (val api.OperationType, ok bool) {
	labelType := fmt.Sprintf("%s/%s", m.Get(api.OperationTypeLabelPrefix), adapter.GetID())
	labelVal := obj.GetLabels()[labelType]
	val = api.OperationType(labelVal)
	return val, val == adapter.GetType()
}

func checkOperate(m api.LifeCycleLabelManager, adapter api.LifecycleAdapter, obj client.Object) (val string, ok bool) {
	labelOperate := fmt.Sprintf("%s/%s", m.Get(api.OperateLabelPrefix), adapter.GetID())
	val, ok = obj.GetLabels()[labelOperate]
	return
}

func setOperatingID(m api.LifeCycleLabelManager, adapter api.LifecycleAdapter, obj client.Object) {
	labelID := fmt.Sprintf("%s/%s", m.Get(api.OperatingLabelPrefix), adapter.GetID())
	obj.GetLabels()[labelID] = fmt.Sprintf("%d", time.Now().UnixNano())
	return
}

func setOperationType(m api.LifeCycleLabelManager, adapter api.LifecycleAdapter, obj client.Object) {
	labelType := fmt.Sprintf("%s/%s", m.Get(api.OperationTypeLabelPrefix), adapter.GetID())
	obj.GetLabels()[labelType] = string(adapter.GetType())
	return
}

// setOperate only for test, expected to be called by adapter
func setOperate(m api.LifeCycleLabelManager, adapter api.LifecycleAdapter, obj client.Object) {
	labelOperate := fmt.Sprintf("%s/%s", m.Get(api.OperateLabelPrefix), adapter.GetID())
	obj.GetLabels()[labelOperate] = fmt.Sprintf("%d", time.Now().UnixNano())
	return
}

func setUndo(m api.LifeCycleLabelManager, adapter api.LifecycleAdapter, obj client.Object) {
	labelUndo := fmt.Sprintf("%s/%s", m.Get(api.UndoOperationTypeLabelPrefix), adapter.GetID())
	obj.GetLabels()[labelUndo] = string(adapter.GetType())
}

func deleteOperatingID(m api.LifeCycleLabelManager, adapter api.LifecycleAdapter, obj client.Object) {
	labelID := fmt.Sprintf("%s/%s", m.Get(api.OperatingLabelPrefix), adapter.GetID())
	delete(obj.GetLabels(), labelID)
	return
}

func queryByOperationType(m api.LifeCycleLabelManager, adapter api.LifecycleAdapter, obj client.Object) sets.String {
	res := sets.String{}
	valType := adapter.GetType()

	for k, v := range obj.GetLabels() {
		if strings.HasPrefix(k, m.Get(api.OperationTypeLabelPrefix)) && v == string(valType) {
			res.Insert(k)
		}
	}

	return res
}

func IsLifecycleOnTarget(m api.LifeCycleLabelManager, operatingID string, target client.Object) (bool, error) {
	if target == nil {
		return false, fmt.Errorf("nil target")
	}

	labels := target.GetLabels()
	if labels == nil {
		return false, nil
	}

	if val, ok := labels[fmt.Sprintf("%s/%s", m.Get(api.OperatingLabelPrefix), operatingID)]; ok {
		return val != "", nil
	}

	return false, nil
}

func CancelOpsLifecycle(m api.LifeCycleLabelManager, client client.Client, adapter api.LifecycleAdapter, target client.Object) error {
	if target == nil {
		return nil
	}

	// only cancel when lifecycle exist on pod
	if exist, err := IsLifecycleOnTarget(m, adapter.GetID(), target); err != nil {
		return fmt.Errorf("fail to check %s PodOpsLifecycle on Pod %s/%s: %w", adapter.GetID(), target.GetNamespace(), target.GetName(), err)
	} else if !exist {
		return nil
	}

	return Undo(m, client, adapter, target)
}

func DefaultUpdateAll(target client.Object, updateFuncs ...UpdateFunc) (updated bool, err error) {
	for _, updateFunc := range updateFuncs {
		ok, updateErr := updateFunc(target)
		if updateErr != nil {
			return updated, updateErr
		}
		updated = updated || ok
	}
	return updated, nil
}
