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

const (
	// copy from k8s.io/apiserver/pkg/storage/names
	randomLength           = 5
	MaxGeneratedNameLength = validation.DNS1035LabelMaxLength - randomLength
)

// GenerateDNS1035Label generates a valid DNS label (compliant with RFC 1035).
// The result is usually combined by the base and uniqueName, such as "base-uniqueName".
// And all "." will be replaced with "-". If the generated name is too long, the suffix
// of base will be truncated to ensure the final name is shorter than 63 characters.
// Usually:
// - base is the name of workload, such as "deployment", "statefulset", "daemonset".
// - uniqueName is a random string, such as "12345" or ordinal index.
func GenerateDNS1035Label(base, uniqueName string) string {
	return generateDNS1035LabelByMaxLength(base, uniqueName, validation.DNS1035LabelMaxLength)
}

// GenerateDNS1035LabelByMaxLength generates a valid DNS label (compliant with RFC 1035)
// limited by the specified maximum length.
func GenerateDNS1035LabelByMaxLength(base, uniqueName string, maxLength int) string {
	return generateDNS1035LabelByMaxLength(base, uniqueName, maxLength)
}

func generateDNS1035LabelByMaxLength(base, unique string, maxLength int) string {
	return genericNameGenerator(base, unique, maxLength, validation.DNS1035LabelMaxLength, fixDNS1035Label)
}

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

func genericNameGenerator(base, unique string, maxLength, maxLimit int, fixFn func(string) string) string {
	if maxLength <= 0 {
		return ""
	}
	if maxLength > maxLimit {
		maxLength = maxLimit
	}

	result := unique
	if len(result) >= maxLength {
		result = result[:maxLength]
	}

	maxPrefixLength := maxLength - len(result)
	if maxPrefixLength > 0 {
		// add prefix
		if len(base) > maxPrefixLength-1 {
			base = base[:maxPrefixLength-1]
		}

		result = base + "-" + result
	}

	return fixFn(result)
}
