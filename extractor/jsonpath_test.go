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
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type jsonPathTest struct {
	name        string
	template    string
	input       map[string]any
	expect      string
	expectError bool
}

func (t *jsonPathTest) Prepare(opts ...Option) (Extractor, error) {
	parser, err := parseJsonPath(t.template)
	if err != nil {
		return nil, err
	}
	o := options{}
	for _, opt := range opts {
		opt.ApplyTo(&o)
	}
	jp := newJSONPatch(o, parser)
	return jp, nil
}

func benchmarkJSONPath(b *testing.B, test jsonPathTest, opts ...Option) {
	jp, err := test.Prepare(opts...)
	if err != nil {
		if !test.expectError {
			b.Errorf("in %s, parse %s error %v", test.name, test.template, err)
			return
		}
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		jp.Extract(test.input)
	}
}

func testJSONPath(t *testing.T, tests []jsonPathTest, opts ...Option) {
	for _, test := range tests {
		jp, err := test.Prepare(opts...)
		if err != nil {
			if !test.expectError {
				t.Errorf("in %s, parse %s error %v", test.name, test.template, err)
			}
			continue
		}

		got, err := jp.Extract(test.input)

		if test.expectError {
			if err == nil {
				t.Errorf(`in %s, expected execute error, got %q`, test.name, got)
			}
		} else if err != nil {
			t.Errorf("in %s, execute error %v", test.name, err)
		}

		bytes_, _ := json.Marshal(got)
		out := string(bytes_)

		if out != test.expect {
			t.Errorf(`in %s, expect to get "%s", got "%s"`, test.name, test.expect, out)
		}
	}
}

var (
	pod = []byte(`{
		"apiVersion": "v1",
		"kind": "Pod",
		"metadata": {
			"labels": {
				"name": "pause",
				"app": "pause"
			},
			"name": "pause",
			"namespace": "default"
		},
		"spec": {
			"containers": [
				{
					"image": "registry.k8s.io/pause:3.8",
					"imagePullPolicy": "IfNotPresent",
					"name": "pause1",
					"resources": {
						"limits": {
							"cpu": "100m",
							"memory": "128Mi"
						},
						"requests": {
							"cpu": "100m",
							"memory": "128Mi"
						}
					}
				},
				{
					"image": "registry.k8s.io/pause:3.8",
					"imagePullPolicy": "IfNotPresent",
					"name": "pause2",
					"resources": {
						"limits": {
							"cpu": "10m",
							"memory": "64Mi"
						},
						"requests": {
							"cpu": "10m",
							"memory": "64Mi"
						}
					}
				}
			]
		}
}`)

	podData map[string]any

	arbitrary = map[string]any{
		"e": struct {
			F1 string `json:"f1"`
			F2 string `json:"f2"`
		}{F1: "f1", F2: "f2"},
	}
)

func init() {
	json.Unmarshal(pod, &podData)
}

func TestJSONPath(t *testing.T) {
	podTests := []jsonPathTest{
		{"empty", ``, podData, `null`, false},
		{"containers name", `{.kind}`, podData, `{"kind":"Pod"}`, false},
		{"containers name", `{.spec.containers[*].name}`, podData, `{"spec":{"containers":[{"name":"pause1"},{"name":"pause2"}]}}`, false},
		{"containers name (range)", `{range .spec.containers[*]}{.name}{.image}{end}`, podData, `null`, true},
		{"containers name and image", `{.spec.containers[*]['name', 'image']}`, podData, `{"spec":{"containers":[{"image":"registry.k8s.io/pause:3.8","name":"pause1"},{"image":"registry.k8s.io/pause:3.8","name":"pause2"}]}}`, false},
		{"containers name and image (depend on relaxing)", `.spec.containers[*]['name', 'image']`, podData, `{"spec":{"containers":[{"image":"registry.k8s.io/pause:3.8","name":"pause1"},{"image":"registry.k8s.io/pause:3.8","name":"pause2"}]}}`, false},
		{"containers name and cpu", `{.spec.containers[*]['name', 'resources.requests.cpu']}`, podData, `{"spec":{"containers":[{"name":"pause1","resources":{"requests":{"cpu":"100m"}}},{"name":"pause2","resources":{"requests":{"cpu":"10m"}}}]}}`, false},
		{"container pause1 name and image", `{.spec.containers[?(@.name=="pause1")]['name', 'image']}`, podData, `{"spec":{"containers":[{"image":"registry.k8s.io/pause:3.8","name":"pause1"}]}}`, false},
		{"pick one label", `{.metadata.labels.name}`, podData, `{"metadata":{"labels":{"name":"pause"}}}`, false},
		{"not exist label", `{.metadata.labels.xx.dd}`, podData, `null`, true},
	}

	testJSONPath(t, podTests, IgnoreMissingKey(false))

	allowMissingTests := []jsonPathTest{
		{"containers image", `{.spec.containers[*]['xname', 'image']}`, podData, `{"spec":{"containers":[{"image":"registry.k8s.io/pause:3.8"},{"image":"registry.k8s.io/pause:3.8"}]}}`, false},
		{"not exist key", `{.spec.containers[*]['name', 'xx.dd']}`, podData, `{"spec":{"containers":[{"name":"pause1"},{"name":"pause2"}]}}`, false},
		{"not exist label", `{.metadata.labels.xx.dd}`, podData, `{"metadata":{"labels":{}}}`, false},
	}

	testJSONPath(t, allowMissingTests, IgnoreMissingKey(true))
}

func TestJSONPathReuse(t *testing.T) {
	parser, err := parseJsonPath(`{.spec.containers[*]['name', 'image']}`)
	require.NoError(t, err)
	extractor := newJSONPatch(options{ignoreMissingKey: true}, parser)
	got, err := extractor.Extract(podData)
	require.NoError(t, err)

	goup := sync.WaitGroup{}
	for range 100 {
		goup.Add(1)
		go func() {
			defer goup.Done()
			got2, err := extractor.Extract(podData)
			assert.NoError(t, err)
			assert.Equal(t, got, got2)
		}()
	}
	goup.Wait()
}
