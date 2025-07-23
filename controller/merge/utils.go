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

package merge

import (
	"encoding/json"

	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ThreeWayMergeToTarget Use three-way merge to get a updated instance.
func ThreeWayMergeToTarget(currentRevisionTarget, updateRevisionTarget, currentTarget, emptyObj client.Object) error {
	currentRevisionTargetBytes, err := json.Marshal(currentRevisionTarget)
	if err != nil {
		return err
	}
	updateRevisionTargetBytes, err := json.Marshal(updateRevisionTarget)
	if err != nil {
		return err
	}

	// 1. find the extra changes based on current revision
	patch, err := strategicpatch.CreateTwoWayMergePatch(currentRevisionTargetBytes, updateRevisionTargetBytes, emptyObj)
	if err != nil {
		return err
	}

	// 2. apply above changes to current target object
	// We don't apply the diff between currentTarget and currentRevisionTarget to updateRevisionTarget,
	// because the TargetTemplate changes should have the highest priority.
	currentTargetBytes, err := json.Marshal(currentTarget)
	if err != nil {
		return err
	}
	if updateRevisionTargetBytes, err = strategicpatch.StrategicMergePatch(currentTargetBytes, patch, emptyObj); err != nil {
		return err
	}

	err = json.Unmarshal(updateRevisionTargetBytes, currentTarget)
	return err
}
