/**
 * Copyright 2024 The KusionStack Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cert

import (
	"context"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

func TestFSProvider_Ensure(t *testing.T) {
	dir := "/serving/cert"
	fs := afero.NewMemMapFs()
	provider, _ := NewFSCertProvider(dir, FSOptions{
		FS: fs,
	})

	domains := []string{"one.kusionstack.io", "two.kusionstack.io"}
	for _, domain := range domains {
		certs, err := provider.Ensure(context.Background(), Config{CommonName: domain})
		assert.NoError(t, err)
		certs.Validate(domain)
		assert.NotNil(t, certs)
		certs, err = provider.Load(context.Background())
		assert.NoError(t, err)
		certs.Validate(domain)
		assert.NotNil(t, certs)
	}
}
