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

package extractor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	type args struct {
		paths            []string
		ignoreMissingKey bool
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{name: "invalid path", args: args{paths: []string{`{`}, ignoreMissingKey: false}, wantErr: true},
		{name: "fieldPath extractor", args: args{paths: []string{`{}`}, ignoreMissingKey: false}, wantErr: false},
		{name: "fieldPath extractor", args: args{paths: []string{``}, ignoreMissingKey: false}, wantErr: false},
		{name: "fieldPath extractor", args: args{paths: []string{`{.metadata.labels.name}`}, ignoreMissingKey: false}, wantErr: false},
		{name: "fieldPath extractor", args: args{paths: []string{`{.metadata.labels['name']}`}, ignoreMissingKey: false}, wantErr: false},
		{name: "jsonPath extractor", args: args{paths: []string{`{.metadata.labels.name}{.metadata.labels.app}`}, ignoreMissingKey: false}, wantErr: true},
		{name: "jsonPath extractor", args: args{paths: []string{`{.metadata.labels['name', 'app']}`}, ignoreMissingKey: false}, wantErr: false},
		{name: "jsonPath extractor", args: args{paths: []string{`{.spec.containers[*].name}`}, ignoreMissingKey: false}, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(tt.args.paths, IgnoreMissingKey(tt.args.ignoreMissingKey))
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.NotNil(t, got)
		})
	}
}

func TestExtractors_Extract(t *testing.T) {
	containerNamePath := `{.spec.containers[*].name}`
	containerImagePath := `{.spec.containers[*].image}`
	kindPath := "{.kind}"
	apiVersionPath := "{.apiVersion}"

	type args struct {
		paths []string
		input map[string]any
	}
	tests := []struct {
		name string
		args args
		// want    string
		wantObj map[string]any
		wantErr bool
	}{
		{
			name: "extract nothing",
			args: args{
				paths: []string{`{}`},
				input: podData,
			},
			wantObj: nil,
			// want:    `{}`,
			wantErr: false,
		},
		{
			name: "merge name and image",
			args: args{
				paths: []string{containerImagePath, containerNamePath},
				input: podData,
			},
			wantObj: map[string]any{
				"spec": map[string]any{
					"containers": []any{
						map[string]any{"name": "pause1"},
						map[string]any{"name": "pause2"},
					},
				},
			},
			// want:    `{"spec":{"containers":[{"name":"pause1"},{"name":"pause2"}]}}`,
			wantErr: false,
		},
		{
			name: "name kind apiVersion",
			args: args{
				paths: []string{containerNamePath, kindPath, apiVersionPath},
				input: podData,
			},
			// want: `{"apiVersion":"v1","kind":"Pod","spec":{"containers":[{"name":"pause1"},{"name":"pause2"}]}}`,
			wantObj: map[string]any{
				"apiVersion": "v1",
				"kind":       "Pod",
				"spec": map[string]any{
					"containers": []any{
						map[string]any{"name": "pause1"},
						map[string]any{"name": "pause2"},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ex, err := New(tt.args.paths, IgnoreMissingKey(true))
			require.NoError(t, err)

			got, err := ex.Extract(tt.args.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			assert.Equal(t, tt.wantObj, got)
		})
	}
}
