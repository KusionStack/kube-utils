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

var (
	// DNS1123LabelGenerator generates a valid DNS label (compliant with RFC 1123).
	DNS1123LabelGenerator = newNameGenerator(validation.DNS1123LabelMaxLength, fixDNS1123Label)
	// DNS1123SubdomainGenerator generates a valid DNS subdomain (compliant with RFC 1123).
	DNS1123SubdomainGenerator = newNameGenerator(validation.DNS1123SubdomainMaxLength, fixDNS1123Subdomain)
)

func fixDNS1123Label(label string) string {
	// Convert to lowercase
	label = strings.ToLower(label)

	var builder strings.Builder
	firstChar := true

	// Process each character in the label
	for i := 0; i < len(label); i++ {
		c := label[i]
		if firstChar {
			// First character must be alphanumeric
			if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
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

func fixDNS1123Subdomain(input string) string {
	// Convert to lowercase
	input = strings.ToLower(input)
	labels := strings.Split(input, ".")
	result := []string{}
	for _, label := range labels {
		fixedLabel := fixDNS1123Label(label)
		if len(fixedLabel) > 0 {
			result = append(result, fixedLabel)
		}
	}
	return strings.Join(result, ".")
}
