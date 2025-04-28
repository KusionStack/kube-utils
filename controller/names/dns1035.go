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
	return GenerateDNS1035LabelByMaxLength(base, uniqueName, validation.DNS1035LabelMaxLength)
}

// GenerateDNS1035LabelByMaxLength generates a valid DNS label (compliant with RFC 1035)
// limited by the specified maximum length.
func GenerateDNS1035LabelByMaxLength(base, uniqueName string, maxLength int) string {
	result := generateDNS1035LabelByMaxLength(base, uniqueName, maxLength)
	// remove all suffix "-"
	return strings.TrimRight(result, "-")
}

// GenerateDNS1035LabelGenerateName generates a valid DNS label prefix (compliant with RFC 1035)
//
// Usually you can set the result in metadata.generateName. kube-apiserver will combine the prefix
// with a unique suffix. Currently, the suffix is a random string with length 5.
func GenerateDNS1035LabelGenerateName(base string) string {
	return GenerateDNS1035LabelPrefixByMaxLength(base, MaxGeneratedNameLength)
}

// GenerateDNS1035LabelPrefixByMaxLength generates a valid DNS label prefix (compliant with RFC 1035)
// limited by the specified maximum length.
func GenerateDNS1035LabelPrefixByMaxLength(base string, maxLength int) string {
	return generateDNS1035LabelByMaxLength(base, "", maxLength)
}

func generateDNS1035LabelByMaxLength(base, unique string, maxLength int) string {
	if maxLength <= 0 {
		return ""
	}
	if maxLength > validation.DNS1035LabelMaxLength {
		maxLength = validation.DNS1035LabelMaxLength
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

	// to lower
	result = strings.ToLower(result)

	b := strings.Builder{}

	firstLetter := false
	// fix result to match DNS1035Label prefix
	// 1. first letter must be [a-z]
	// 2. only contain characters in [-a-z0-9]
	for i := range len(result) {
		if !firstLetter {
			if result[i] < 'a' || result[i] > 'z' {
				// DNS1035Label must start with a lowercase letter
				continue
			}
			firstLetter = true
			b.WriteByte(result[i])
			continue
		}
		if (result[i] >= 'a' && result[i] <= 'z') || (result[i] >= '0' && result[i] <= '9') {
			// write a-z, 0-9
			b.WriteByte(result[i])
		} else {
			// change other characters to "-"
			b.WriteByte('-')
		}
	}
	return b.String()
}
