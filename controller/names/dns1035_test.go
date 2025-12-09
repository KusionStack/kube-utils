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

type dns1035TestSuite struct {
	suite.Suite
}

func TestDNS1035TestSuite(t *testing.T) {
	suite.Run(t, new(dns1035TestSuite))
}

func (s *dns1035TestSuite) Test_fixDNS1035Label() {
	tests := []struct {
		name  string
		label string
		want  string
	}{
		{
			name:  "empty string",
			label: "",
			want:  "",
		},
		{
			name:  "valid lowercase",
			label: "valid-label",
			want:  "valid-label",
		},
		{
			name:  "uppercase to lowercase",
			label: "VALID-LABEL",
			want:  "valid-label",
		},
		{
			name:  "invalid characters replaced with dash",
			label: "invalid.label@123",
			want:  "invalid-label-123",
		},
		{
			name:  "starting with number - skip until letter",
			label: "123abc",
			want:  "abc",
		},
		{
			name:  "starting with special characters - skip until letter",
			label: "@#$abc",
			want:  "abc",
		},
		{
			name:  "starting with dash - skip until letter",
			label: "-abc",
			want:  "abc",
		},
		{
			name:  "only numbers and special characters",
			label: "123@#$",
			want:  "",
		},
		{
			name:  "trailing dashes trimmed",
			label: "label---",
			want:  "label",
		},
		{
			name:  "mixed case with special characters",
			label: "My.Test.Label@123",
			want:  "my-test-label-123",
		},
		{
			name:  "single letter",
			label: "A",
			want:  "a",
		},
		{
			name:  "starting with underscore and number",
			label: "_123abc",
			want:  "abc",
		},
		{
			name:  "consecutive special characters become single dash",
			label: "test..label@@123",
			want:  "test--label--123",
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			got := fixDNS1035Label(tt.label)
			s.Equal(tt.want, got)
			if len(got) > 0 {
				s.Empty(validation.IsDNS1035Label(got))
			}
		})
	}
}

func (s *dns1035TestSuite) Test_GenerateDNS1035Label() {
	tests := []struct {
		name      string
		base      string
		unique    string
		maxLength int
		want      string
	}{
		{
			name:      "normal case",
			base:      "test",
			unique:    "0",
			maxLength: validation.DNS1035LabelMaxLength,
			want:      "test-0",
		},
		{
			name:      "maxLength == DNS1035LabelMaxLength",
			base:      "test-123456789012345678901234567890123456789012345678901234567890",
			unique:    "0",
			maxLength: validation.DNS1035LabelMaxLength,
			want:      "test-12345678901234567890123456789012345678901234567890123456-0",
		},
		{
			name:      "maxLength > DNS1035LabelMaxLength",
			base:      "test-123456789012345678901234567890123456789012345678901234567890",
			unique:    "0",
			maxLength: 70,
			want:      "test-12345678901234567890123456789012345678901234567890123456-0",
		},
		{
			name:      "normal case",
			base:      "test-123456789012345678901234567890123456789012345678901234567890",
			unique:    "0",
			maxLength: 7,
			want:      "test--0",
		},
		{
			name:      "unique too long, trim right",
			base:      "xzvcyqjsk",
			unique:    "ab-1234567890123456789012345678901234567890123456789012345678901234567890",
			maxLength: validation.DNS1035LabelMaxLength,
			want:      "ab-123456789012345678901234567890123456789012345678901234567890",
		},
		{
			name:      "unique too long, trim left and right",
			base:      "xzvcyqjsk",
			unique:    "1-ab-1234567890123456789012345678901234567890123456789012345678901234567890",
			maxLength: validation.DNS1035LabelMaxLength,
			want:      "ab-1234567890123456789012345678901234567890123456789012345678",
		},
		{
			name:      "maxLength == 0",
			base:      "xzvcyqjsk",
			unique:    "ab-123456789012345678901234567890123456789012345678901234567890",
			maxLength: 0,
			want:      "",
		},
		{
			name:      "trimleft base",
			base:      "0-abcd",
			unique:    "0",
			maxLength: validation.DNS1035LabelMaxLength,
			want:      "abcd-0",
		},
		{
			name:      "replace invalid characters to '-'",
			base:      "test.#@_1",
			unique:    "0",
			maxLength: 63,
			want:      "test----1-0",
		},
		{
			name:      "to lower case",
			base:      "TEST.1",
			unique:    "0",
			maxLength: 63,
			want:      "test-1-0",
		},
		{
			name:      "to lower case",
			base:      "test-1",
			unique:    "!@#$%^&*()_+",
			maxLength: 63,
			want:      "test-1",
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			got := DNS1035LabelGenerator.GenerateNameWithMaxLength(tt.base, tt.unique, tt.maxLength)
			s.Equal(tt.want, got)
			if len(got) > 0 {
				s.Empty(validation.IsDNS1035Label(got), "should be a valid DNS1035 label")
			}
		})
	}
}
