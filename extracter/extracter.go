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
	"errors"
	"fmt"

	"k8s.io/client-go/util/jsonpath"
)

type Extracter interface {
	Extract(data map[string]interface{}) (map[string]interface{}, error)
}

// parse is unlike the jsonpath.Parse, which supports multi-paths input.
// The input like `{.kind} {.apiVersion}` or
// `{range .spec.containers[*]}{.name}{end}` will result in an error.
func parse(name, text string) (*Parser, error) {
	p, err := jsonpath.Parse(name, text)
	if err != nil {
		return nil, err
	}

	if len(p.Root.Nodes) > 1 {
		return nil, errors.New("not support multi-paths input")
	}

	return p, nil
}

// New creates an Extracter. For each jsonPaths, FieldPathExtracter will
// be parsed whenever possible, as it has better performance
func New(jsonPaths []string, allowMissingKeys bool) (Extracter, error) {
	var extracters []Extracter

	for _, p := range jsonPaths {
		parser, err := parse(p, p)
		if err != nil {
			return nil, fmt.Errorf("error in parsing path %q: %w", p, err)
		}

		rootNodes := parser.Root.Nodes
		if len(rootNodes) == 0 {
			extracters = append(extracters, NewNestedFieldPathExtracter(nil, allowMissingKeys))
			continue
		}

		if len(rootNodes) == 1 {
			nodes := rootNodes[0].(*jsonpath.ListNode).Nodes
			fields := make([]string, 0, len(nodes))
			for _, node := range nodes {
				if node.Type() == jsonpath.NodeField {
					fields = append(fields, node.(*jsonpath.FieldNode).Value)
				}
			}

			if len(nodes) == len(fields) {
				fp := NewNestedFieldPathExtracter(fields, allowMissingKeys)
				extracters = append(extracters, fp)
				continue
			}
		}

		jp := &JSONPathExtracter{name: parser.Name, parser: parser, allowMissingKeys: allowMissingKeys}
		extracters = append(extracters, jp)
	}

	if len(extracters) == 1 {
		return extracters[0], nil
	}

	return &Extracters{extracters}, nil
}

// Extracters makes it easy when you want to extract multi fields and merge them.
type Extracters struct {
	extracters []Extracter
}

// Extract calls all extracters in order and merges their outputs by calling MergeFields.
func (e *Extracters) Extract(data map[string]interface{}) (map[string]interface{}, error) {
	var merged map[string]interface{}

	for _, ex := range e.extracters {
		field, err := ex.Extract(data)
		if err != nil {
			return nil, err
		}

		if merged == nil {
			merged = field
		} else {
			merged = MergeFields(merged, field)
		}
	}

	return merged, nil
}
