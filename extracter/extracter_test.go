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

package extracter

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestNew(t *testing.T) {
	type args struct {
		paths            []string
		allowMissingKeys bool
	}
	tests := []struct {
		name    string
		args    args
		want    Extracter
		wantErr bool
	}{
		{name: "invalid path", args: args{paths: []string{`{`}, allowMissingKeys: false}, want: nil, wantErr: true},
		{name: "fieldPath extracter", args: args{paths: []string{`{}`}, allowMissingKeys: false}, want: &NestedFieldPathExtracter{}, wantErr: false},
		{name: "fieldPath extracter", args: args{paths: []string{``}, allowMissingKeys: false}, want: &NestedFieldPathExtracter{}, wantErr: false},
		{name: "fieldPath extracter", args: args{paths: []string{`{.metadata.labels.name}`}, allowMissingKeys: false}, want: &NestedFieldPathExtracter{}, wantErr: false},
		{name: "fieldPath extracter", args: args{paths: []string{`{.metadata.labels['name']}`}, allowMissingKeys: false}, want: &NestedFieldPathExtracter{}, wantErr: false},
		{name: "jsonPath extracter", args: args{paths: []string{`{.metadata.labels.name}{.metadata.labels.app}`}, allowMissingKeys: false}, want: nil, wantErr: true},
		{name: "jsonPath extracter", args: args{paths: []string{`{.metadata.labels['name', 'app']}`}, allowMissingKeys: false}, want: &JSONPathExtracter{}, wantErr: false},
		{name: "jsonPath extracter", args: args{paths: []string{`{.spec.containers[*].name}`}, allowMissingKeys: false}, want: &JSONPathExtracter{}, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(tt.args.paths, tt.args.allowMissingKeys)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if reflect.TypeOf(tt.want) != reflect.TypeOf(got) {
				t.Errorf("New() = %T, want %T", got, tt.want)
			}
		})
	}
}

func TestExtracters_Extract(t *testing.T) {
	containerNamePath := `{.spec.containers[*].name}`
	containerImagePath := `{.spec.containers[*].image}`
	kindPath := "{.kind}"
	apiVersionPath := "{.apiVersion}"

	type args struct {
		paths []string
		input map[string]interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "merge name and image", args: args{paths: []string{containerImagePath, containerNamePath}, input: podData},
			want: `{"spec":{"containers":[{"name":"pause1"},{"name":"pause2"}]}}`, wantErr: false,
		},
		{
			name: "name kind apiVersion", args: args{paths: []string{containerNamePath, kindPath, apiVersionPath}, input: podData},
			want: `{"apiVersion":"v1","kind":"Pod","spec":{"containers":[{"name":"pause1"},{"name":"pause2"}]}}`, wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ex, err := New(tt.args.paths, true)
			if (err != nil) != tt.wantErr {
				t.Errorf("Extracters_Extract() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			got, err := ex.Extract(tt.args.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Extracters_Extract() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			data, _ := json.Marshal(got)
			if string(data) != tt.want {
				t.Errorf("Extracters_Extract() = %v, want %v", string(data), tt.want)
			}
		})
	}
}
