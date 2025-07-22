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
	"reflect"
	"testing"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"kusionstack.io/kube-utils/xset/api"
)

const (
	mockLabelKey   = "mockLabel"
	mockLabelValue = "mockLabelValue"
	testNamespace  = "default"
	testName       = "target-1"
)

var (
	allowTypes = false
	scheme     = runtime.NewScheme()
)

func init() {
	corev1.AddToScheme(scheme)
}

func TestLifecycle(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	g := gomega.NewGomegaWithT(t)

	a := &mockAdapter{id: "id-1", operationType: "type-1"}
	b := &mockAdapter{id: "id-2", operationType: "type-1"}
	mgr := NewLabelManager(nil)

	inputs := []struct {
		hasOperating, hasConflictID bool
		started                     bool
		err                         error
		allow                       bool
	}{
		{
			hasOperating: false,
			started:      false,
		},
		{
			hasOperating: true,
			started:      false,
		},
		{
			hasConflictID: true,
			started:       false,
			err:           fmt.Errorf("operationType %s exists: %v", a.GetType(), sets.NewString(fmt.Sprintf("%s/%s", mgr.Get(api.OperationTypeLabelPrefix), b.GetID()))),
		},
		{
			hasConflictID: true,
			started:       false,
			allow:         true,
			err:           nil,
		},
	}

	for i, input := range inputs {
		allowTypes = input.allow

		target := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   testNamespace,
				Name:        fmt.Sprintf("%s-%d", testName, i),
				Labels:      map[string]string{},
				Annotations: map[string]string{},
			},
		}
		g.Expect(c.Create(context.TODO(), target)).Should(gomega.Succeed())

		if input.hasOperating {
			setOperatingID(mgr, a, target)
			setOperationType(mgr, a, target)
			a.WhenBegin(target)
		}

		if input.hasConflictID {
			setOperatingID(mgr, b, target)
			setOperationType(mgr, b, target)
		}

		_, err := Begin(mgr, c, a, target)
		g.Expect(reflect.DeepEqual(err, input.err)).Should(gomega.BeTrue())
		if err != nil {
			continue
		}
		g.Expect(target.Labels[mockLabelKey]).Should(gomega.BeEquivalentTo(mockLabelValue))

		setOperate(mgr, a, target)
		started, err := Begin(mgr, c, a, target)
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
		g.Expect(started).Should(gomega.BeTrue())
		g.Expect(IsDuringOps(mgr, a, target)).Should(gomega.BeTrue())

		finished, err := Finish(mgr, c, a, target)
		g.Expect(err).ShouldNot(gomega.HaveOccurred())
		g.Expect(finished).Should(gomega.BeTrue())
		g.Expect(target.Labels[mockLabelKey]).Should(gomega.BeEquivalentTo(""))
		g.Expect(IsDuringOps(mgr, a, target)).Should(gomega.BeFalse())
	}
}

type mockAdapter struct {
	id            string
	operationType api.OperationType
}

func (m *mockAdapter) GetID() string {
	return m.id
}

func (m *mockAdapter) GetType() api.OperationType {
	return m.operationType
}

func (m *mockAdapter) AllowMultiType() bool {
	return allowTypes
}

func (m *mockAdapter) WhenBegin(target client.Object) (bool, error) {
	target.GetLabels()[mockLabelKey] = mockLabelValue
	return true, nil
}

func (m *mockAdapter) WhenFinish(target client.Object) (bool, error) {
	delete(target.GetLabels(), mockLabelKey)
	return true, nil
}
