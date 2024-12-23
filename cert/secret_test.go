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

	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestSecretProvider_Ensure(t *testing.T) {
	provider, err := NewSecretCertProvider(newSecretClient(), "default", "test-serving-cert")
	assert.NoError(t, err)

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

func newSecretClient() SecretClient {
	clientbuilder := fake.NewClientBuilder().WithScheme(scheme.Scheme)
	client := clientbuilder.Build()

	return NewSecretClient(client, client)
}

// var _ SecretClient = &fakeSecretClient{}

// type fakeSecretClient struct {
// 	client client.Client
// }

// // Create implements SecretClient.
// // Subtle: this method shadows the method (Client).Create of fakeSecretClient.Client.
// func (f *fakeSecretClient) Create(ctx context.Context, secret *v1.Secret) error {
// 	return f.client.Create(ctx, secret)
// }

// // Get implements SecretClient.
// // Subtle: this method shadows the method (Client).Get of fakeSecretClient.Client.
// func (f *fakeSecretClient) Get(ctx context.Context, namespace string, name string) (*v1.Secret, error) {
// 	var secret v1.Secret
// 	err := f.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &secret)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &secret, nil
// }

// // Update implements SecretClient.
// // Subtle: this method shadows the method (Client).Update of fakeSecretClient.Client.
// func (f *fakeSecretClient) Update(ctx context.Context, secret *v1.Secret) error {
// 	return f.client.Update(ctx, secret)
// }
