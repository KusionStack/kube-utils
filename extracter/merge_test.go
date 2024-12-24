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
	"bytes"
	"encoding/json"
	"testing"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func BenchmarkJSONPathMerge(b *testing.B) {
	tests := []jsonPathTest{
		{"kind", `{.kind}`, podData, "", false},
		{"apiVersion", "{.apiVersion}", podData, "", false},
		{"metadata", "{.metadata}", podData, "", false},
	}

	extracters := make([]Extracter, 0)
	for _, test := range tests {
		ex, err := test.Prepare(false)
		if err != nil {
			if !test.expectError {
				b.Errorf("in %s, parse %s error %v", test.name, test.template, err)
			}
			return
		}
		extracters = append(extracters, ex)
	}

	ex := Extracters{extracters: extracters}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		ex.Extract(podData)
	}
}

func BenchmarkFieldPathMerge(b *testing.B) {
	fields := []string{"kind", "apiVersion", "metadata"}

	extracters := make([]Extracter, 0)
	for _, f := range fields {
		extracters = append(extracters, NewNestedFieldPathExtracter([]string{f}, false))
	}

	ex := Extracters{extracters: extracters}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		ex.Extract(podData)
	}
}

func BenchmarkTmpl(b *testing.B) {
	tmpl := `{"kind": "{{ .Object.kind }}","apiVersion": "{{ .Object.apiVersion}}","metadata": {{ toJson .Object.metadata }}}`
	obj := unstructured.Unstructured{Object: podData}

	t, _ := template.New("transformTemplate").Funcs(sprig.FuncMap()).Parse(tmpl)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		var buf bytes.Buffer
		t.Execute(&buf, obj)

		var dest unstructured.Unstructured
		json.Unmarshal(buf.Bytes(), &dest)
	}
}
