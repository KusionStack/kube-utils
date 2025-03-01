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
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetCondition(t *testing.T) {
	type args struct {
		conditions []metav1.Condition
		ctype      string
	}
	tests := []struct {
		name string
		args args
		want *metav1.Condition
	}{
		{
			name: "not found",
			args: args{
				conditions: []metav1.Condition{
					*NewCondition("Test", metav1.ConditionTrue, "", ""),
				},
				ctype: "NotExists",
			},
			want: nil,
		},
		{
			name: "found",
			args: args{
				conditions: []metav1.Condition{
					*NewCondition("Test", metav1.ConditionTrue, "", ""),
				},
				ctype: "Test",
			},
			want: NewCondition("Test", metav1.ConditionTrue, "", ""),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetCondition(tt.args.conditions, tt.args.ctype)
			if tt.want == nil {
				assert.Nil(t, got)
			} else {
				assert.True(t, conditionEquals(*got, *tt.want))
			}
		})
	}
}

func TestSetCondition(t *testing.T) {
	conditions := []metav1.Condition{
		*NewCondition("Test1", metav1.ConditionTrue, "", ""),
		*NewCondition("Test2", metav1.ConditionTrue, "", ""),
	}

	newCond := NewCondition("Test3", metav1.ConditionTrue, "", "")

	// add new condition
	got := SetCondition(conditions, *newCond)
	assert.Len(t, got, 3)

	// set exisiting condition but no update
	newCond = NewCondition("Test1", metav1.ConditionTrue, "", "")
	got = SetCondition(conditions, *newCond)
	assert.Len(t, got, 2)
	assert.Equal(t, got[0].Type, "Test1")

	// update existing condition, this condition will in the tail
	newCond = NewCondition("Test1", metav1.ConditionFalse, "", "")
	got = SetCondition(conditions, *newCond)
	assert.Len(t, got, 2)
	assert.Equal(t, got[0].Type, "Test2")
	assert.Equal(t, got[1].Type, "Test1")
	assert.Equal(t, got[1].Status, metav1.ConditionFalse)
}

func TestRejectConditionByType(t *testing.T) {
	conditions := []metav1.Condition{
		*NewCondition("Test1", metav1.ConditionTrue, "", ""),
		*NewCondition("Test2", metav1.ConditionTrue, "", ""),
	}
	got := RejectConditionByType(conditions, "NotExists")
	assert.Len(t, got, 2)
	got = RejectConditionByType(conditions, "Test1")
	assert.Len(t, got, 1)
	assert.NotNil(t, GetCondition(got, "Test2"))
}
