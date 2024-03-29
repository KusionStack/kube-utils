/**
 * Copyright 2024 KusionStack Authors.
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

package resourcetopo

import (
	"testing"

	"golang.org/x/exp/slices"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func Test_compareResourceRelation(t *testing.T) {
	tests := []struct {
		name  string
		want  int
		argsA ResourceRelation
		argsB ResourceRelation
	}{
		{
			name:  "all empty equal",
			want:  0,
			argsA: ResourceRelation{},
			argsB: ResourceRelation{},
		},
		{
			name:  "A empty and B not empty",
			want:  -1,
			argsA: ResourceRelation{},
			argsB: ResourceRelation{
				PostMeta: PodMeta,
			},
		},
		{
			name: "direct ref equal",
			argsA: ResourceRelation{
				PostMeta: PodMeta,
				DirectRefs: []types.NamespacedName{
					{
						Name:      "pod1",
						Namespace: namespaceDefault,
					},
					{
						Name:      "pod2",
						Namespace: namespaceDefault,
					},
					{
						Name:      "pod3",
						Namespace: namespaceDefault,
					},
				},
				LabelSelector: nil,
			},
			argsB: ResourceRelation{
				PostMeta: PodMeta,
				DirectRefs: []types.NamespacedName{
					{
						Name:      "pod2",
						Namespace: namespaceDefault,
					},
					{
						Name:      "pod3",
						Namespace: namespaceDefault,
					},
					{
						Name:      "pod1",
						Namespace: namespaceDefault,
					},
				},
				LabelSelector: nil,
			},
		},
		{
			name: "label selector equal",
			argsA: ResourceRelation{
				PostMeta: PodMeta,
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"apps": "test",
						"a":    "b",
					},
					MatchExpressions: []metav1.LabelSelectorRequirement{
						{
							Key:      "app",
							Operator: metav1.LabelSelectorOpDoesNotExist,
						},
					},
				},
			},
			argsB: ResourceRelation{
				PostMeta: PodMeta,
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"a":    "b",
						"apps": "test",
					},
					MatchExpressions: []metav1.LabelSelectorRequirement{
						{
							Key:      "app",
							Operator: metav1.LabelSelectorOpDoesNotExist,
						},
					},
				},
			},
		},

		{
			name: "direct ref size not equal",
			want: -2,
			argsA: ResourceRelation{
				PostMeta: PodMeta,
				DirectRefs: []types.NamespacedName{
					{
						Name:      "pod1",
						Namespace: namespaceDefault,
					},
				},
			},
			argsB: ResourceRelation{
				PostMeta: PodMeta,
				DirectRefs: []types.NamespacedName{
					{
						Name:      "pod2",
						Namespace: namespaceDefault,
					},
					{
						Name:      "pod3",
						Namespace: namespaceDefault,
					},
					{
						Name:      "pod1",
						Namespace: namespaceDefault,
					},
				},
			},
		},
		{
			name: "direct ref not equal",
			want: 1,
			argsA: ResourceRelation{
				PostMeta: PodMeta,
				DirectRefs: []types.NamespacedName{
					{
						Name:      "pod1",
						Namespace: "ns1",
					},
				},
			},
			argsB: ResourceRelation{
				PostMeta: PodMeta,
				DirectRefs: []types.NamespacedName{
					{
						Name:      "pod1",
						Namespace: "ns0",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := compareResourceRelation(tt.argsA, tt.argsB); got != tt.want {
				t.Errorf("compareResourceRelation() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_slicesCompare(t *testing.T) {
	type args[S interface{ ~[]E }, E any] struct {
		slice1           S
		slice2           S
		expectDiffSlice1 S
		expectDiffSlice2 S
	}
	type testCase struct {
		name string
		args args[[]int, int]
	}
	tests := []testCase{
		{
			name: "empty equal",
			args: args[[]int, int]{
				slice1:           []int{},
				slice2:           []int{},
				expectDiffSlice1: []int{},
				expectDiffSlice2: []int{},
			},
		},
		{
			name: "equal slices",
			args: args[[]int, int]{
				slice1:           []int{1, 3, 5},
				slice2:           []int{1, 3, 5},
				expectDiffSlice1: []int{},
				expectDiffSlice2: []int{},
			},
		},
		{
			name: "empty slice1",
			args: args[[]int, int]{
				slice1:           []int{},
				slice2:           []int{1, 3, 5},
				expectDiffSlice1: []int{},
				expectDiffSlice2: []int{1, 3, 5},
			},
		},
		{
			name: "empty slice2",
			args: args[[]int, int]{
				slice1:           []int{1, 3, 5},
				slice2:           []int{},
				expectDiffSlice1: []int{1, 3, 5},
				expectDiffSlice2: []int{},
			},
		},
		{
			name: "case4",
			args: args[[]int, int]{
				slice1:           []int{1, 3, 5, 7, 9},
				slice2:           []int{1, 5, 7},
				expectDiffSlice1: []int{3, 9},
				expectDiffSlice2: []int{},
			},
		},
		{
			name: "case5",
			args: args[[]int, int]{
				slice1:           []int{1, 3, 5, 7, 9},
				slice2:           []int{2, 5, 6, 8, 10},
				expectDiffSlice1: []int{1, 3, 7, 9},
				expectDiffSlice2: []int{2, 6, 8, 10},
			},
		},
		{
			name: "case6",
			args: args[[]int, int]{
				slice1:           []int{1, 3, 5, 7, 9},
				slice2:           []int{2, 4, 6, 8, 10},
				expectDiffSlice1: []int{1, 3, 5, 7, 9},
				expectDiffSlice2: []int{2, 4, 6, 8, 10},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diffSlice1, diffSlice2 := []int{}, []int{}
			sortedSlicesCompare(tt.args.slice1, tt.args.slice2, func(i int) {
				diffSlice1 = append(diffSlice1, i)
			}, func(i int) {
				diffSlice2 = append(diffSlice2, i)
			}, func(a int, b int) int {
				return a - b
			})
			if !slices.Equal(diffSlice1, tt.args.expectDiffSlice1) {
				t.Errorf("sortedSlicesCompare got %v, want %v", diffSlice1, tt.args.expectDiffSlice1)
			}
			if !slices.Equal(diffSlice2, tt.args.expectDiffSlice2) {
				t.Errorf("sortedSlicesCompare got %v, want %v", diffSlice2, tt.args.expectDiffSlice2)
			}
		})
	}
}
