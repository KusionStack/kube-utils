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

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/utils/ptr"
)

func TestHashControllerRevisionRawData(t *testing.T) {
	tests := []struct {
		name  string
		data  []byte
		probe *int32
		want  string // WARNING: you should never change this value, It is used to ensure hash algorithm consistency
	}{
		{
			name:  "nil data",
			data:  nil,
			probe: nil,
			// WARNING: you should never change this value, It is used to ensure hash algorithm consistency
			want: "65bb57b6b5",
		},
		{
			name:  "nil with probe 1",
			data:  []byte{},
			probe: ptr.To[int32](1),
			// WARNING: you should never change this value, It is used to ensure hash algorithm consistency
			want: "d8bfb7bb",
		},
		{
			name:  "nil with probe 2",
			data:  []byte{},
			probe: ptr.To[int32](2),
			// WARNING: you should never change this value, It is used to ensure hash algorithm consistency
			want: "d8bfb7b9",
		},
		{
			name:  "normal data",
			data:  []byte(`{"metadata":{"name":"test"}}`),
			probe: nil,
			// WARNING: you should never change this value, It is used to ensure hash algorithm consistency
			want: "67794bdc49",
		},
		{
			name:  "normal data with probe 1",
			data:  []byte(`{"metadata":{"name":"test"}}`),
			probe: ptr.To[int32](1),
			// WARNING: you should never change this value, It is used to ensure hash algorithm consistency
			want: "fdd8fd7c4",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, HashControllerRevisionRawData(tt.data, tt.probe))
		})
	}
}

func TestEqualRevision(t *testing.T) {
	tests := []struct {
		name string
		lrv  *appsv1.ControllerRevision
		rrv  *appsv1.ControllerRevision
		want bool
	}{
		{
			name: "nil",
			want: true,
		},
		{
			name: "equal with same label",
			lrv: func() *appsv1.ControllerRevision {
				parent := newTestStatefulSet()
				return newStatefulSetControllerRevision(parent, 1, ptr.To[int32](1))
			}(),
			rrv: func() *appsv1.ControllerRevision {
				parent := newTestStatefulSet()
				return newStatefulSetControllerRevision(parent, 1, ptr.To[int32](1))
			}(),
			want: true,
		},

		{
			name: "equal with out same label",
			lrv: func() *appsv1.ControllerRevision {
				parent := newTestStatefulSet()
				rv := newStatefulSetControllerRevision(parent, 1, ptr.To[int32](1))
				rv.Labels = nil
				return rv
			}(),
			rrv: func() *appsv1.ControllerRevision {
				parent := newTestStatefulSet()
				rv := newStatefulSetControllerRevision(parent, 1, ptr.To[int32](2))
				rv.Labels = nil
				return rv
			}(),
			want: true,
		},
		{
			name: "equal even collision is changed",
			lrv: func() *appsv1.ControllerRevision {
				parent := newTestStatefulSet()
				return newStatefulSetControllerRevision(parent, 1, ptr.To[int32](1))
			}(),
			rrv: func() *appsv1.ControllerRevision {
				parent := newTestStatefulSet()
				return newStatefulSetControllerRevision(parent, 1, ptr.To[int32](2))
			}(),
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, EqualRevision(tt.lrv, tt.rrv))
		})
	}
}
