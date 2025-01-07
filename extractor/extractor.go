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
	"fmt"

	"k8s.io/client-go/util/jsonpath"
)

type Extractor interface {
	Extract(data map[string]interface{}) (map[string]interface{}, error)
}

func NewOrDie(jsonPaths []string, opts ...Option) Extractor {
	extractor, err := New(jsonPaths, opts...)
	if err != nil {
		panic(err)
	}
	return extractor
}

// New creates an Extractor. For each jsonPaths, FieldPathExtractor will
// be parsed whenever possible, as it has better performance
func New(jsonPaths []string, opts ...Option) (Extractor, error) {
	var extractors []Extractor

	options := options{}
	for _, opt := range opts {
		opt.ApplyTo(&options)
	}

	for _, p := range jsonPaths {
		parser, err := parseJsonPath(p)
		if err != nil {
			return nil, fmt.Errorf("error in parsing path %q: %w", p, err)
		}

		rootNodes := parser.Root.Nodes
		if len(rootNodes) == 0 {
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
				fp := newNestFieldPath(options, fields...)
				extractors = append(extractors, fp)
				continue
			}
		}

		jp := newJSONPathExtractor(options, parser)
		extractors = append(extractors, jp)
	}

	if len(extractors) == 1 {
		return extractors[0], nil
	}

	return NewAggregate(extractors...), nil
}

// aggregate makes it easy when you want to extract multi fields and merge them.
type aggregate struct {
	extractors []Extractor
}

func NewAggregate(extractors ...Extractor) Extractor {
	return &aggregate{extractors: extractors}
}

// Extract calls all extractors in order and merges their outputs by calling mergeFields.
func (e *aggregate) Extract(data map[string]interface{}) (map[string]interface{}, error) {
	var merged map[string]interface{}

	for _, ex := range e.extractors {
		field, err := ex.Extract(data)
		if err != nil {
			return nil, err
		}

		if merged == nil {
			merged = field
		} else {
			merged = mergeFields(merged, field)
		}
	}

	return merged, nil
}

type Option interface {
	ApplyTo(o *options)
}

type options struct {
	ignoreMissingKey bool
}

type IgnoreMissingKey bool

func (o IgnoreMissingKey) ApplyTo(opt *options) {
	opt.ignoreMissingKey = bool(o)
}
