/*
Copyright 2025 The KusionStack Authors.

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

package history

import (
	"testing"

	"github.com/stretchr/testify/suite"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestHistoryManagerTestSuite(t *testing.T) {
	suite.Run(t, new(historyManagerTestSuite))
}

func TestHistoryInterfaceTestSuite(t *testing.T) {
	suite.Run(t, new(revisionControlTestSuite))
}

func newStatefulSetControllerRevision(obj *appsv1.StatefulSet, revision int64, collisionCount *int32) *appsv1.ControllerRevision {
	patch, _ := (&revisionOwnerStatefulSet{}).GetPatch(obj)
	cr, _ := NewControllerRevision(
		obj,
		statefulSetGVK,
		obj.Spec.Selector.MatchLabels,
		runtime.RawExtension{
			Raw: patch,
		},
		revision,
		collisionCount,
	)
	return cr
}
