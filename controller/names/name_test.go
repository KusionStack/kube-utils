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
	base := "test-123456789012345678901234567890123456789012345678901234567890"
	// test long base name
	prefix := generateDNS1035LabelPrefix(base, 57)
	assert.Equal(t, "test-123456789012345678901234567890123456789012345678901-", prefix)
	assert.Len(t, prefix, 57)

	base = "test-1"
	prefix = generateDNS1035LabelPrefix(base, 57)
	assert.Equal(t, prefix, "test-1-")

	base = "test.1"
	prefix = generateDNS1035LabelPrefix(base, 57)
	assert.Equal(t, prefix, "test-1-")
}

func Test_GenerateDNS1035Label(t *testing.T) {
	base := "test"
	suffix := "0"
	name := GenerateDNS1035Label(base, suffix)
	assert.Equal(t, name, "test-0")

	base = "test-123456789012345678901234567890123456789012345678901234567890"
	name = GenerateDNS1035Label(base, suffix)
	assert.Equal(t, "test-12345678901234567890123456789012345678901234567890123456-0", name)
	assert.Len(t, name, validation.DNS1035LabelMaxLength)

	name = GenerateDNS1035LabelByMaxLength(base, suffix, 7)
	assert.Equal(t, "test--0", name)
}
