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
	"errors"
	"fmt"
	"regexp"

	"k8s.io/client-go/util/jsonpath"
)

// ParseJsonPath is unlike the jsonpath.ParseJsonPath, which supports multi-paths input.
// The input like `{.kind} {.apiVersion}` or
// `{range .spec.containers[*]}{.name}{end}` will result in an error.
//
// It also relaxes the JSONPath expressions internally,
// so inputs like `.kind` (without curly braces) are acceptable.
func ParseJsonPath(text string) (*jsonpath.Parser, error) {
	relaxed, err := RelaxedJSONPathExpression(text)
	if err != nil {
		return nil, err
	}

	p, err := jsonpath.Parse(text, relaxed)
	if err != nil {
		return nil, err
	}

	if len(p.Root.Nodes) > 1 {
		return nil, errors.New("not support multi-paths input")
	}

	return p, nil
}

var jsonRegexp = regexp.MustCompile(`^\{\.?([^{}]+)\}$|^\.?([^{}]+)$`)

// RelaxedJSONPathExpression attempts to be flexible with JSONPath expressions, it accepts:
//   - metadata.name (no leading '.' or curly braces '{...}'
//   - {metadata.name} (no leading '.')
//   - .metadata.name (no curly braces '{...}')
//   - {.metadata.name} (complete expression)
//
// And transforms them all into a valid jsonpath expression:
//
//	{.metadata.name}
//
// Copied from kubectl.
func RelaxedJSONPathExpression(pathExpression string) (string, error) {
	if len(pathExpression) == 0 || pathExpression == "{}" {
		return pathExpression, nil
	}
	submatches := jsonRegexp.FindStringSubmatch(pathExpression)
	if submatches == nil {
		return "", fmt.Errorf("unexpected path string, expected a 'name1.name2' or '.name1.name2' or '{name1.name2}' or '{.name1.name2}'")
	}
	if len(submatches) != 3 {
		return "", fmt.Errorf("unexpected submatch list: %v", submatches)
	}
	var fieldSpec string
	if len(submatches[1]) != 0 {
		fieldSpec = submatches[1]
	} else {
		fieldSpec = submatches[2]
	}
	return fmt.Sprintf("{.%s}", fieldSpec), nil
}
