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
	"testing"

	"github.com/stretchr/testify/suite"
	"k8s.io/apimachinery/pkg/util/validation"
)

type dns1123TestSuite struct {
	suite.Suite
}

func TestDNS1123TestSuite(t *testing.T) {
	suite.Run(t, new(dns1123TestSuite))
}

func (s *dns1123TestSuite) TestFixDNS1123Label() {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "valid label",
			input:    "valid-label",
			expected: "valid-label",
		},
		{
			name:     "uppercase conversion",
			input:    "Valid-Label",
			expected: "valid-label",
		},
		{
			name:     "invalid characters",
			input:    "invalid@label#123",
			expected: "invalid-label-123",
		},
		{
			name:     "starting with number",
			input:    "123label",
			expected: "123label",
		},
		{
			name:     "starting with dash",
			input:    "-label",
			expected: "label",
		},
		{
			name:     "starting with special character",
			input:    "@label",
			expected: "label",
		},
		{
			name:     "ending with dash",
			input:    "label-",
			expected: "label",
		},
		{
			name:     "multiple dashes",
			input:    "label---test",
			expected: "label---test",
		},
		{
			name:     "only special characters",
			input:    "@#$%",
			expected: "",
		},
		{
			name:     "dots in label",
			input:    "label.with.dots",
			expected: "label-with-dots",
		},
		{
			name:     "spaces and tabs",
			input:    "label with spaces",
			expected: "label-with-spaces",
		},
		{
			name:     "underscores",
			input:    "label_with_underscores",
			expected: "label-with-underscores",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			result := fixDNS1123Label(tc.input)
			s.Equal(tc.expected, result)
			if len(result) > 0 {
				s.Empty(validation.IsDNS1123Subdomain(result))
			}
		})
	}
}

func (s *dns1123TestSuite) TestFixDNS1123Subdomain() {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "valid subdomain",
			input:    "valid-subdomain.example.com",
			expected: "valid-subdomain.example.com",
		},
		{
			name:     "uppercase conversion",
			input:    "Valid-Subdomain.Example.COM",
			expected: "valid-subdomain.example.com",
		},
		{
			name:     "invalid characters in labels",
			input:    "invalid@subdomain#123.example.com",
			expected: "invalid-subdomain-123.example.com",
		},
		{
			name:     "starting with special characters",
			input:    "@subdomain.example.com",
			expected: "subdomain.example.com",
		},
		{
			name:     "ending with special characters",
			input:    "subdomain@.example.com",
			expected: "subdomain.example.com",
		},
		{
			name:     "multiple dots",
			input:    "sub..domain.example.com",
			expected: "sub.domain.example.com",
		},
		{
			name:     "leading dots",
			input:    ".subdomain.example.com",
			expected: "subdomain.example.com",
		},
		{
			name:     "trailing dots",
			input:    "subdomain.example.com.",
			expected: "subdomain.example.com",
		},
		{
			name:     "only special characters",
			input:    "@#$%.@#$%",
			expected: "",
		},
		{
			name:     "spaces in labels",
			input:    "sub domain.example com",
			expected: "sub-domain.example-com",
		},
		{
			name:     "underscores in labels",
			input:    "sub_domain.example_com",
			expected: "sub-domain.example-com",
		},
		{
			name:     "mixed case with dots and dashes",
			input:    "My-Sub.Domain.Example-Test.COM",
			expected: "my-sub.domain.example-test.com",
		},
		{
			name:     "single label",
			input:    "singlelabel",
			expected: "singlelabel",
		},
		{
			name:     "label with numbers",
			input:    "label123.test456.com",
			expected: "label123.test456.com",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			result := fixDNS1123Subdomain(tc.input)
			s.Equal(tc.expected, result)
			if len(result) > 0 {
				s.Empty(validation.IsDNS1123Subdomain(result))
			}
		})
	}
}

func (s *dns1123TestSuite) TestGenerateDNS1123SubdomainByMaxLength() {
	testCases := []struct {
		name      string
		base      string
		unique    string
		maxLength int
		expected  string
	}{
		{
			name:      "normal case",
			base:      "test",
			unique:    "unique",
			maxLength: 20,
			expected:  "test-unique",
		},
		{
			name:      "base too long",
			base:      "extremelylongbasename",
			unique:    "short",
			maxLength: 10,
			expected:  "extr-short",
		},
		{
			name:      "unique too long",
			base:      "short",
			unique:    "extremelylonguniquename",
			maxLength: 10,
			expected:  "extremelyl",
		},
		{
			name:      "zero max length",
			base:      "test",
			unique:    "unique",
			maxLength: 0,
			expected:  "",
		},
		{
			name:      "negative max length",
			base:      "test",
			unique:    "unique",
			maxLength: -5,
			expected:  "",
		},
		{
			name:      "max length exceeds DNS limit",
			base:      "test",
			unique:    "unique",
			maxLength: 300,
			expected:  "test-unique",
		},
		{
			name:      "special characters in base and unique",
			base:      "Test@Base",
			unique:    "Unique#123",
			maxLength: 20,
			expected:  "test-base-unique-123",
		},
		{
			name:      "empty base",
			base:      "",
			unique:    "unique",
			maxLength: 10,
			expected:  "unique",
		},
		{
			name:      "empty unique",
			base:      "base",
			unique:    "",
			maxLength: 10,
			expected:  "base",
		},
		{
			name:      "both empty",
			base:      "",
			unique:    "",
			maxLength: 10,
			expected:  "",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			result := generateDNS1123SubdomainByMaxLength(tc.base, tc.unique, tc.maxLength)
			s.Equal(tc.expected, result)
			if len(result) > 0 {
				s.Empty(validation.IsDNS1123Subdomain(result))
			}
		})
	}
}
