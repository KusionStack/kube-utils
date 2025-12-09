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

// NameGenerator is an interface for generating names for resources.
type NameGenerator interface {
	// GenerateName generates a valid name.
	// The result is usually combined by the base and uniqueName, such as "base-uniqueName".
	// The generator may have a default max length limit. If the generated name is too long,
	// it will be truncated to ensure the final name is shorter than the limit.
	//
	// Usually:
	// - base is the name of workload, such as "deployment", "statefulset", "daemonset".
	// - uniqueName is a random string, such as "12345" or ordinal index.
	GenerateName(base, unique string) string
	// GenerateNameWithMaxLength generates a valid name with max length limit.
	// The result is usually combined by the base and uniqueName, such as "base-uniqueName".
	// If the generated name is too long, the suffix of base will be truncated to ensure the
	// final name is shorter than maxLength.
	//
	// Usually:
	// - base is the name of workload, such as "deployment", "statefulset", "daemonset".
	// - uniqueName is a random string, such as "12345" or ordinal index.
	GenerateNameWithMaxLength(base, unique string, maxLength int) string
}

type formatFn func(string) string

// nameGenerator is an implementation of NameGenerator.
type nameGenerator struct {
	formatFixFn    formatFn
	maxLangthLimit int
}

func newNameGenerator(lengthLimit int, fixFn formatFn) NameGenerator {
	return &nameGenerator{
		maxLangthLimit: lengthLimit,
		formatFixFn:    fixFn,
	}
}

// GenerateName generates a valid name.
func (g *nameGenerator) GenerateName(base, unique string) string {
	return g.GenerateNameWithMaxLength(base, unique, g.maxLangthLimit)
}

// GenerateNameWithMaxLength generates a valid name with max length limit.
func (g *nameGenerator) GenerateNameWithMaxLength(base, unique string, maxLength int) string {
	if maxLength <= 0 {
		return ""
	}
	if maxLength > g.maxLangthLimit {
		maxLength = g.maxLangthLimit
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

	return g.formatFixFn(result)
}
