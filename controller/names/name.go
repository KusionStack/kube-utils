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
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
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
	if maxLength <= 0 {
		return ""
	}
	if maxLength > validation.DNS1035LabelMaxLength {
		maxLength = validation.DNS1035LabelMaxLength
	}

	uniqueName = strings.ReplaceAll(uniqueName, ".", "-")
	uniqueNameLength := len(uniqueName)
	if uniqueNameLength > maxLength {
		return uniqueName[:maxLength]
	}
	maxPrefixLength := maxLength - uniqueNameLength
	prefix := generateDNS1035LabelPrefix(base, maxPrefixLength)
	return fmt.Sprintf("%s%s", prefix, uniqueName)
}

func generateDNS1035LabelPrefix(base string, maxLength int) string {
	// replace all "." with "-"
	base = strings.ReplaceAll(base, ".", "-")
	if len(base) > maxLength-1 {
		base = base[:maxLength-1]
	}
	// append a "-"
	base += "-"
	return base
}
