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
)

// newNestFieldPath constructs a FieldPathExtractor.
func newNestFieldPath(opts options, nestedField ...string) Extractor {
	return &nestedFieldPath{
		options:     opts,
		nestedField: nestedField,
	}
}

// nestedFieldPath is used to wrap NestedFieldNoCopy function as an Extractor.
type nestedFieldPath struct {
	options
	nestedField []string
}

// Extract outputs the nestedField's value and its upstream structure.
func (n *nestedFieldPath) Extract(data map[string]interface{}) (map[string]interface{}, error) {
	return nestedFieldNoCopy(data, n.ignoreMissingKey, n.nestedField...)
}

// nestedFieldNoCopy is similar to JSONPath.Extract. The difference is that it
// can only operate on map and does not support list, but has better performance.
func nestedFieldNoCopy(data map[string]interface{}, ignoreMissingKey bool, fields ...string) (map[string]interface{}, error) {
	if len(fields) == 0 {
		return nil, nil
	}

	result := map[string]interface{}{}
	cur := result

	for i, field := range fields {
		if val, ok := data[field]; ok {
			if i != len(fields)-1 {
				if data, ok = val.(map[string]interface{}); !ok {
					return nil, fmt.Errorf("%v is of the type %T, expected map[string]interface{}", val, val)
				}

				m := map[string]interface{}{}
				cur[field] = m
				cur = m
			} else {
				cur[field] = val
			}
		} else {
			if ignoreMissingKey {
				return result, nil
			}
			return nil, fmt.Errorf("field %q not exist", field)
		}
	}

	return result, nil
}
