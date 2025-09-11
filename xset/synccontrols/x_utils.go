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
	"context"
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clientutils "kusionstack.io/kube-utils/client"
	"kusionstack.io/kube-utils/condition"
	controllerutils "kusionstack.io/kube-utils/controller/utils"
	"kusionstack.io/kube-utils/xset/api"
)

func GetInstanceID(xsetLabelAnnoMgr api.XSetLabelAnnotationManager, target client.Object) (int, error) {
	if target.GetLabels() == nil {
		return -1, fmt.Errorf("no labels found for instance ID")
	}

	instanceIdLabelKey := xsetLabelAnnoMgr.Value(api.XInstanceIdLabelKey)
	val, exist := target.GetLabels()[instanceIdLabelKey]
	if !exist {
		return -1, fmt.Errorf("failed to find instance ID label %s", instanceIdLabelKey)
	}

	id, err := strconv.ParseInt(val, 10, 32)
	if err != nil {
		// ignore invalid target instance ID
		return -1, fmt.Errorf("failed to parse instance ID with value %s: %w", val, err)
	}

	return int(id), nil
}

func NewTargetFrom(setController api.XSetController, xsetLabelAnnoMgr api.XSetLabelAnnotationManager, owner api.XSetObject, revision *appsv1.ControllerRevision, id int, updateFuncs ...func(client.Object) error) (client.Object, error) {
	targetObj, err := setController.GetXObjectFromRevision(revision)
	if err != nil {
		return nil, err
	}

	meta := setController.XSetMeta()
	ownerRef := metav1.NewControllerRef(owner, meta.GroupVersionKind())
	targetObj.SetOwnerReferences(append(targetObj.GetOwnerReferences(), *ownerRef))
	targetObj.SetNamespace(owner.GetNamespace())
	targetObj.SetGenerateName(GetTargetsPrefix(owner.GetName()))

	xsetLabelAnnoMgr.Set(targetObj, api.XInstanceIdLabelKey, fmt.Sprintf("%d", id))
	targetObj.GetLabels()[appsv1.ControllerRevisionHashLabelKey] = revision.GetName()
	controlByXSet(xsetLabelAnnoMgr, targetObj)

	for _, fn := range updateFuncs {
		if err := fn(targetObj); err != nil {
			return targetObj, err
		}
	}

	return targetObj, nil
}

const ConditionUpdatePeriodBackOff = 30 * time.Second

func AddOrUpdateCondition(status *api.XSetStatus, conditionType api.XSetConditionType, err error, reason, message string) {
	condStatus := metav1.ConditionTrue
	if err != nil {
		condStatus = metav1.ConditionFalse
	}

	existCond := condition.GetCondition(status.Conditions, string(conditionType))
	if existCond != nil && existCond.Reason == reason && existCond.Status == condStatus {
		now := metav1.Now()
		if now.Sub(existCond.LastTransitionTime.Time) < ConditionUpdatePeriodBackOff {
			return
		}
	}

	cond := condition.NewCondition(string(conditionType), condStatus, reason, message)
	status.Conditions = condition.SetCondition(status.Conditions, *cond)
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

func controlByXSet(xsetLabelAnnoMgr api.XSetLabelAnnotationManager, obj client.Object) {
	if obj.GetLabels() == nil {
		obj.SetLabels(map[string]string{})
	}
	if v, ok := xsetLabelAnnoMgr.Get(obj.GetLabels(), api.ControlledByXSetLabel); !ok || v != "true" {
		xsetLabelAnnoMgr.Set(obj, api.ControlledByXSetLabel, "true")
	}
}

func IsControlledByXSet(xsetLabelManager api.XSetLabelAnnotationManager, obj client.Object) bool {
	if obj.GetLabels() == nil {
		return false
	}

	v, ok := xsetLabelManager.Get(obj.GetLabels(), api.ControlledByXSetLabel)
	return ok && v == "true"
}

func ApplyTemplatePatcher(ctx context.Context, xsetController api.XSetController, c client.Client, xset api.XSetObject, targets []*TargetWrapper) error {
	_, patchErr := controllerutils.SlowStartBatch(len(targets), controllerutils.SlowStartInitialBatchSize, false, func(i int, _ error) error {
		if targets[i].Object == nil || targets[i].PlaceHolder {
			return nil
		}
		_, err := clientutils.UpdateOnConflict(ctx, c, c, targets[i].Object, xsetController.GetXSetTemplatePatcher(xset))
		return err
	})
	return patchErr
}

func CompareTarget(l, r client.Object, checkReadyFunc func(object client.Object) (bool, *metav1.Time)) bool {
	// If both targets are ready, the latest ready one is smaller
	lReady, lReadyTime := checkReadyFunc(l)
	rReady, rReadyTime := checkReadyFunc(r)
	if lReady && rReady && !lReadyTime.Equal(rReadyTime) {
		return afterOrZero(lReadyTime, rReadyTime)
	}
	// Empty creation time targets < newer targets < older targets
	lCreationTime, rCreationTime := l.GetCreationTimestamp(), r.GetCreationTimestamp()
	if !(&lCreationTime).Equal(&rCreationTime) {
		return afterOrZero(&lCreationTime, &rCreationTime)
	}
	return false
}

func afterOrZero(t1, t2 *metav1.Time) bool {
	if t1.Time.IsZero() || t2.Time.IsZero() {
		return t1.Time.IsZero()
	}
	return t1.After(t2.Time)
}
