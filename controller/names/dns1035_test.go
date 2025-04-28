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

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/validation"
)

func Test_generateDNS1035LabelPrefix(t *testing.T) {
	tests := []struct {
		name      string
		base      string
		maxLength int
		want      string
	}{
		{
			name:      "maxLength > DNS1035LabelMaxLength",
			base:      "ab-1234567890123456789012345678901234567890123456789012345678901234567890",
			maxLength: 70,
			want:      "ab-12345678901234567890123456789012345678901234567890123456789-",
		},
		{
			name:      "maxLength == DNS1035LabelMaxLength",
			base:      "test-123456789012345678901234567890123456789012345678901234567890",
			maxLength: MaxGeneratedNameLength,
			want:      "test-1234567890123456789012345678901234567890123456789012-",
		},
		{
			name:      "normal case",
			base:      "test-123456789012345678901234567890123456789012345678901234567890",
			maxLength: 10,
			want:      "test-1234-",
		},
		{
			name:      "normal case",
			base:      "test-123456789012345678901234567890123456789012345678901234567890",
			maxLength: 5,
			want:      "test-",
		},
		{
			name:      "normal case",
			base:      "test-123456789012345678901234567890123456789012345678901234567890",
			maxLength: 6,
			want:      "test--",
		},
		{
			name:      "normal case",
			base:      "test-1",
			maxLength: MaxGeneratedNameLength,
			want:      "test-1-",
		},
		{
			name:      "replace invalid characters to '-'",
			base:      "test.#@1",
			maxLength: MaxGeneratedNameLength,
			want:      "test---1-",
		},
		{
			name:      "maxLength == 0",
			base:      "test.1",
			maxLength: 0,
			want:      "",
		},
		{
			name:      "trimleft base",
			base:      "0-abcd",
			maxLength: validation.DNS1035LabelMaxLength,
			want:      "abcd-",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GenerateDNS1035LabelPrefixByMaxLength(tt.base, tt.maxLength)
			assert.Equal(t, tt.want, got)
			if len(got) > 0 {
				assert.Equal(t, byte('-'), got[len(got)-1], "prefix should end with '-'")
			}
		})
	}
}

func Test_GenerateDNS1035Label(t *testing.T) {
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
			maxLength: MaxGeneratedNameLength,
			want:      "test----1-0",
		},
		{
			name:      "to lower case",
			base:      "TEST.1",
			unique:    "0",
			maxLength: MaxGeneratedNameLength,
			want:      "test-1-0",
		},
		{
			name:      "to lower case",
			base:      "test-1",
			unique:    "!@#$%^&*()_+",
			maxLength: MaxGeneratedNameLength,
			want:      "test-1",
		},
	}

	for _, tt := range tests {
		got := GenerateDNS1035LabelByMaxLength(tt.base, tt.unique, tt.maxLength)
		assert.Equal(t, tt.want, got)
		if len(got) > 0 {
			assert.Empty(t, validation.IsDNS1035Label(got), "should be a valid DNS1035 label")
		}
	}
}
