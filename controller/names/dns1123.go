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

// GenerateDNS1123Label generates a valid DNS label (compliant with RFC 1123).
// The result is usually combined by the base and uniqueName, such as "base-uniqueName".
// If the generated name is too long, the suffix of base will be truncated to ensure the
// final name is shorter than 63 characters.
//
// Usually:
// - base is the name of workload, such as "deployment", "statefulset", "daemonset".
// - uniqueName is a random string, such as "12345" or ordinal index.
func GenerateDNS1123Label(base, unique string) string {
	return genereateDNS1123LabelByMaxLength(base, unique, validation.DNS1123LabelMaxLength)
}

// GenerateDNS1123LabelByMaxLength generates a valid DNS label (compliant with RFC 1123)
// limited by the specified maximum length.
func GenereateDNS1123LabelByMaxLength(base, unique string, maxLength int) string {
	return genereateDNS1123LabelByMaxLength(base, unique, maxLength)
}

func genereateDNS1123LabelByMaxLength(base, unique string, maxLength int) string {
	return genericNameGenerator(base, unique, maxLength, validation.DNS1123LabelMaxLength, fixDNS1123Label)
}

// GenerateDNS1123Subdomain generates a valid DNS subdomain (compliant with RFC 1123).
// The result is usually combined by the base and uniqueName, such as "base-uniqueName".
// If the generated name is too long, the suffix of base will be truncated to ensure the
// final name is shorter than 253 characters.
//
// Usually:
// - base is the name of workload, such as "deployment", "statefulset", "daemonset".
// - uniqueName is a random string, such as "12345" or ordinal index.
func GenerateDNS1123Subdomain(base, unique string) string {
	return generateDNS1123SubdomainByMaxLength(base, unique, validation.DNS1123SubdomainMaxLength)
}

// GenerateDNS1123SubdomainByMaxLength generates a valid DNS subdomain (compliant with RFC 1123)
// limited by the specified maximum length.
func GenerateDNS1123SubdomainByMaxLength(base, unique string, maxLength int) string {
	return generateDNS1123SubdomainByMaxLength(base, unique, maxLength)
}

func generateDNS1123SubdomainByMaxLength(base, unique string, maxLength int) string {
	return genericNameGenerator(base, unique, maxLength, validation.DNS1123SubdomainMaxLength, fixDNS1123Subdomain)
}

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
