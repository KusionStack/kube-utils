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
	"reflect"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		text    string
		want    *parser
		wantErr bool
	}{
		{name: "multi paths", text: "{.kind} {.apiVersion}", want: nil, wantErr: true},
		{name: "range path", text: "{range .spec.containers[*]}{.name}{end}", want: nil, wantErr: true},
		{name: "one path", text: "{.kind}", want: &parser{}, wantErr: false},
		{name: "empty brace", text: "{}", want: &parser{}, wantErr: false},
		{name: "empty", text: "", want: &parser{}, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.text, tt.text)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if reflect.TypeOf(got) != reflect.TypeOf(tt.want) {
				t.Errorf("Parse() = %v, want %v", got, tt.want)
			}
		})
	}
}
