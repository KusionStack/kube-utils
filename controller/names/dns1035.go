/*
Copyright 2025 The KusionStack Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package names

import (
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

// DNS1035LabelGenerator generates a valid DNS label (compliant with RFC 1035).
var DNS1035LabelGenerator = newNameGenerator(validation.DNS1035LabelMaxLength, fixDNS1035Label)

func fixDNS1035Label(label string) string {
	// Convert to lowercase
	label = strings.ToLower(label)

	var builder strings.Builder
	firstChar := true

	// Process each character in the label
	for i := 0; i < len(label); i++ {
		c := label[i]

		if firstChar {
			// First character must be letter
			if c >= 'a' && c <= 'z' {
				builder.WriteByte(c)
				firstChar = false
			}
			// Skip non-alphanumeric characters at the beginning
			continue
		}

		// Subsequent characters: allow alphanumeric and dash
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' {
			builder.WriteByte(c)
		} else {
			// Replace invalid characters with dash
			builder.WriteByte('-')
		}
	}

	result := builder.String()
	return strings.TrimRight(result, "-")
}
