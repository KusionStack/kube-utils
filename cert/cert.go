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
	"crypto/rsa"
	"crypto/x509"
	"fmt"
	"net"
	"time"

	certutil "github.com/zoumo/golib/cert"
)

type (
	Config   = certutil.Config
	AltNames = certutil.AltNames
)

// ServingCerts is a set of serving certificates.
type ServingCerts struct {
	Key    []byte
	Cert   []byte
	CAKey  []byte
	CACert []byte
}

// Validate checks if the serving certificates are valid for given host.
func (c *ServingCerts) Validate(host string) error {
	if len(c.Key) == 0 {
		return fmt.Errorf("private key is empty")
	}
	if len(c.Cert) == 0 {
		return fmt.Errorf("cetificate is empty")
	}
	if len(c.CAKey) == 0 {
		return fmt.Errorf("CA private key is empty")
	}
	if len(c.CACert) == 0 {
		return fmt.Errorf("CA certificate is empty")
	}

	tlsCert, err := certutil.X509KeyPair(c.Cert, c.Key)
	if err != nil {
		return fmt.Errorf("invalid x509 keypair: %w", err)
	}

	// verify cert with ca and host
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(c.CACert) {
		return fmt.Errorf("no valid CA certificate found")
	}

	options := x509.VerifyOptions{
		Roots:       pool,
		DNSName:     host,
		CurrentTime: time.Now(),
	}

	_, err = tlsCert.X509Cert.Verify(options)
	return err
}

// GenerateSelfSignedCerts generates a self-signed certificate and key for the given host.
func GenerateSelfSignedCerts(cfg Config) (*ServingCerts, error) {
	caKey, caCert, key, cert, err := generateSelfSignedCertKey(cfg)
	if err != nil {
		return nil, err
	}

	keyPEM := certutil.MarshalRSAPrivateKeyToPEM(key)
	cerPEM := certutil.MarshalCertToPEM(cert)
	caKeyPEM := certutil.MarshalRSAPrivateKeyToPEM(caKey)
	caCertPEM := certutil.MarshalCertToPEM(caCert)

	return &ServingCerts{
		CAKey:  caKeyPEM.EncodeToMemory(),
		CACert: caCertPEM.EncodeToMemory(),
		Key:    keyPEM.EncodeToMemory(),
		Cert:   cerPEM.EncodeToMemory(),
	}, nil
}

// GenerateSelfSignedCertKeyIfNotExist generates a self-signed certificate and
// write them to the given path if not exist.
func GenerateSelfSignedCertKeyIfNotExist(path string, cfg Config) error {
	fscerts, err := NewFSCertProvider(path, FSOptions{})
	if err != nil {
		return err
	}
	_, err = fscerts.Ensure(context.Background(), cfg)
	return err
}

func generateSelfSignedCertKey(cfg Config) (*rsa.PrivateKey, *x509.Certificate, *rsa.PrivateKey, *x509.Certificate, error) {
	if len(cfg.CommonName) == 0 {
		return nil, nil, nil, nil, fmt.Errorf("common name is empty")
	}

	caKey, err := certutil.NewRSAPrivateKey()
	if err != nil {
		return nil, nil, nil, nil, err
	}

	caCert, err := certutil.NewSelfSignedCACert(certutil.Config{
		CommonName: fmt.Sprintf("%s-ca@%d", cfg.CommonName, time.Now().Unix()),
	}, caKey)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	key, err := certutil.NewRSAPrivateKey()
	if err != nil {
		return nil, nil, nil, nil, err
	}

	if ip := net.ParseIP(cfg.CommonName); ip != nil {
		cfg.AltNames.IPs = append(cfg.AltNames.IPs, ip)
	} else {
		cfg.AltNames.DNSNames = append(cfg.AltNames.DNSNames, cfg.CommonName)
	}

	cert, err := certutil.NewSignedCert(cfg, key, caKey, caCert)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return caKey, caCert, key, cert, nil
}
