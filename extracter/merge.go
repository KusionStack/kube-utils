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
)

// MergeFields merges src into dst.
//
// Note: the merge operation on two nested list is replacing.
func MergeFields(dst, src map[string]interface{}) map[string]interface{} {
	for key, val := range src {
		if cur, ok := dst[key]; ok {
			if reflect.TypeOf(val) != reflect.TypeOf(cur) {
				return nil
			}

			switch cur := cur.(type) {
			case []interface{}:
				dst[key] = val.([]interface{})
			case map[string]interface{}:
				dst[key] = MergeFields(cur, val.(map[string]interface{}))
			default:
				dst[key] = val
			}
		} else {
			dst[key] = val
		}
	}
	return dst
}
