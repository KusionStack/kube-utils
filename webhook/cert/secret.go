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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	TLSPrivateKeyKey   = corev1.TLSPrivateKeyKey
	TLSCertKey         = corev1.TLSCertKey
	TLSCACertKey       = "ca.crt"
	TLSCAPrivateKeyKey = "ca.key"
)

type SecretProvider struct {
	client    SecretClient
	namespace string
	name      string
}

type SecretClient interface {
	Get(ctx context.Context, namespace string, name string) (*corev1.Secret, error)
	Create(ctx context.Context, secret *corev1.Secret) error
	Update(ctx context.Context, secret *corev1.Secret) error
}

func NewSecretProvider(client SecretClient, namespace, name string) (*SecretProvider, error) {
	if client == nil {
		return nil, fmt.Errorf("secret client must not be nil")
	}
	return &SecretProvider{
		client:    client,
		namespace: namespace,
		name:      name,
	}, nil
}

func (p *SecretProvider) Ensure(ctx context.Context, cfg Config) (*ServingCerts, error) {
	certs, err := p.Load(ctx)
	if err != nil && !IsNotFound(err) {
		return nil, err
	}

	op := ""
	if IsNotFound(err) {
		op = "create"
	} else if err := certs.Validate(cfg.CommonName); err != nil {
		klog.ErrorS(err, "invalid certs in secret")
		op = "overwrite"
	}

	if len(op) > 0 {
		certs, err = GenerateSelfSignedCerts(cfg)
		if err != nil {
			return nil, err
		}
		var opErr error
		if op == "create" {
			opErr = p.create(ctx, certs)
		} else {
			opErr = p.overwrite(ctx, certs)
		}
		if opErr != nil {
			return nil, opErr
		}
		return certs, nil
	}

	return certs, nil
}

func (p *SecretProvider) Load(ctx context.Context) (*ServingCerts, error) {
	secret, err := p.client.Get(ctx, p.namespace, p.name)
	if err != nil {
		return nil, err
	}

	return convertSecretToCerts(secret), nil
}

func (p *SecretProvider) create(ctx context.Context, certs *ServingCerts) error {
	if certs == nil {
		return fmt.Errorf("certs are required")
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: p.namespace,
			Name:      p.name,
		},
		Type: corev1.SecretTypeTLS,
	}

	writeCertsToSecret(secret, certs)
	// create it
	// If there is another controller racer, an AlreadyExistsError may be returned.
	return p.client.Create(ctx, secret)
}

func (p *SecretProvider) overwrite(ctx context.Context, certs *ServingCerts) error {
	if certs == nil {
		return fmt.Errorf("certs are required")
	}
	secret, err := p.client.Get(ctx, p.namespace, p.name)
	if client.IgnoreNotFound(err) != nil {
		// err != NotFound, return it
		return err
	}

	if apierrors.IsNotFound(err) {
		// not found, create new one
		return p.create(ctx, certs)
	}

	// overwrite existing one
	writeCertsToSecret(secret, certs)

	// If there is another controller racer, an Conflict may be returned.
	return p.client.Update(ctx, secret)
}

func convertSecretToCerts(secret *corev1.Secret) *ServingCerts {
	return &ServingCerts{
		Key:    secret.Data[TLSPrivateKeyKey],
		Cert:   secret.Data[TLSCertKey],
		CAKey:  secret.Data[TLSCAPrivateKeyKey],
		CACert: secret.Data[TLSCACertKey],
	}
}

func writeCertsToSecret(secret *corev1.Secret, certs *ServingCerts) {
	if secret.Data == nil {
		secret.Data = make(map[string][]byte)
	}
	secret.Data[TLSPrivateKeyKey] = certs.Key
	secret.Data[TLSCertKey] = certs.Cert
	secret.Data[TLSCAPrivateKeyKey] = certs.CAKey
	secret.Data[TLSCACertKey] = certs.CACert
}
