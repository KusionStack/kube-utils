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
	"time"

	"github.com/go-logr/logr"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	errutil "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"kusionstack.io/kube-utils/controller/mixin"
)

type WebhookCertSelfSigner struct {
	*mixin.ReconcilerMixin
	CertConfig

	fs     *FSProvider
	secret *SecretProvider
}

type CertConfig struct {
	Host           string
	AlternateHosts []string

	Namespace              string
	SecretName             string
	MutatingWebhookNames   []string
	ValidatingWebhookNames []string

	ContextWrapper func(context.Context) context.Context
}

func New(mgr manager.Manager, cfg CertConfig) *WebhookCertSelfSigner {
	return &WebhookCertSelfSigner{
		ReconcilerMixin: mixin.NewReconcilerMixin("webhook-cert-self-signer", mgr),
		CertConfig:      cfg,
	}
}

func (s *WebhookCertSelfSigner) SetupWithManager(mgr manager.Manager) error {
	var err error
	server := mgr.GetWebhookServer()
	s.fs, err = NewFSProvider(server.CertDir, FSOptions{
		CertName: server.CertName,
		KeyName:  server.KeyName,
	})
	if err != nil {
		return err
	}
	s.secret, err = NewSecretProvider(
		&secretClient{
			reader: s.APIReader,
			writer: s.Client,
			logger: s.Logger,
		},
		s.Namespace,
		s.SecretName,
	)
	if err != nil {
		return err
	}

	// manually sync certs once
	_, err = s.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: client.ObjectKey{
			Namespace: s.Namespace,
			Name:      s.SecretName,
		},
	})
	if err != nil {
		return err
	}
	ctrl, _ := controller.NewUnmanaged(s.GetControllerName(), mgr, controller.Options{
		Reconciler: s,
	})

	// add watches for secrets, webhook configs
	types := []client.Object{
		&corev1.Secret{},
		&admissionregistrationv1.ValidatingWebhookConfiguration{},
		&admissionregistrationv1.MutatingWebhookConfiguration{},
	}
	for i := range types {
		t := types[i]
		err = ctrl.Watch(
			&source.Kind{Type: t},
			s.enqueueSecret(),
			s.predictFunc(),
		)
		if err != nil {
			return err
		}
	}
	// make controller run as non-leader election
	return mgr.Add(&nonLeaderElectionController{Controller: ctrl})
}

func (s *WebhookCertSelfSigner) predictFunc() predicate.Funcs {
	mutatingWebhookNameSet := sets.NewString(s.MutatingWebhookNames...)
	validatingWebhookNameSet := sets.NewString(s.ValidatingWebhookNames...)
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		if obj == nil {
			return false
		}
		switch t := obj.(type) {
		case *corev1.Secret:
			return t.Namespace == s.Namespace && t.Name == s.SecretName
		case *admissionregistrationv1.MutatingWebhookConfiguration:
			return mutatingWebhookNameSet.Has(t.Name)
		case *admissionregistrationv1.ValidatingWebhookConfiguration:
			return validatingWebhookNameSet.Has(t.Name)
		}
		return false
	})
}

func (s *WebhookCertSelfSigner) enqueueSecret() handler.EventHandler {
	mapFunc := func(obj client.Object) []reconcile.Request {
		return []reconcile.Request{
			{
				NamespacedName: client.ObjectKey{
					Namespace: s.Namespace,
					Name:      s.SecretName,
				},
			},
		}
	}
	return handler.EnqueueRequestsFromMapFunc(mapFunc)
}

func (s *WebhookCertSelfSigner) Reconcile(ctx context.Context, _ reconcile.Request) (reconcile.Result, error) {
	if s.ContextWrapper != nil {
		ctx = s.ContextWrapper(ctx)
	}
	cfg := Config{
		CommonName: s.Host,
		AltNames: AltNames{
			DNSNames: s.AlternateHosts,
		},
	}
	servingCerts, err := s.secret.Ensure(ctx, cfg)
	if err != nil {
		if IsConflict(err) {
			// create error on AlreadyExists
			// update error on Conflict
			// retry
			return reconcile.Result{RequeueAfter: 1 * time.Second}, nil
		}
		return reconcile.Result{}, err
	}

	if servingCerts == nil {
		return reconcile.Result{}, fmt.Errorf("got empty serving certs from secret")
	}

	// got valid serving certs in secret
	// 1. write certs to fs
	changed, err := s.fs.Overwrite(servingCerts)
	if err != nil {
		return reconcile.Result{}, err
	}
	if changed {
		s.Logger.Info("write certs to files successfully")
	}

	// 2. update caBundle in webhook configurations
	err = s.ensureWebhookConfiguration(ctx, servingCerts.CACert)
	if err != nil {
		return reconcile.Result{}, err
	}

	return reconcile.Result{}, nil
}

func (s *WebhookCertSelfSigner) ensureWebhookConfiguration(ctx context.Context, caBundle []byte) error {
	var errList []error
	mutatingCfg := &admissionregistrationv1.MutatingWebhookConfiguration{}
	for _, name := range s.MutatingWebhookNames {
		var changed bool
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			changed = false
			if err := s.APIReader.Get(ctx, types.NamespacedName{Name: name}, mutatingCfg); err != nil {
				return err
			}

			for i := range mutatingCfg.Webhooks {
				if string(mutatingCfg.Webhooks[i].ClientConfig.CABundle) != string(caBundle) {
					changed = true
					mutatingCfg.Webhooks[i].ClientConfig.CABundle = caBundle
				}
			}

			if !changed {
				return nil
			}

			return s.Client.Update(ctx, mutatingCfg)
		}); err != nil {
			s.Logger.Info("failed to update ca in mutating webhook", "name", name, "error", err.Error())
			if !errors.IsNotFound(err) {
				errList = append(errList, fmt.Errorf("failed to update ca in mutating webhook %s: %s", name, err))
			}
			continue
		}

		if changed {
			s.Logger.Info("ensure ca in mutating webhook", "name", name)
		}
	}

	validatingCfg := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	for _, name := range s.ValidatingWebhookNames {
		var changed bool
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			changed = false
			if err := s.APIReader.Get(ctx, types.NamespacedName{Name: name}, validatingCfg); err != nil {
				return err
			}

			for i := range validatingCfg.Webhooks {
				if string(validatingCfg.Webhooks[i].ClientConfig.CABundle) != string(caBundle) {
					changed = true
					validatingCfg.Webhooks[i].ClientConfig.CABundle = caBundle
				}
			}

			if !changed {
				return nil
			}

			return s.Client.Update(ctx, validatingCfg)
		}); err != nil {
			s.Logger.Info("failed to update ca in validating webhook", "name", name, "error", err.Error())
			if !errors.IsNotFound(err) {
				errList = append(errList, fmt.Errorf("failed to update ca in validating webhook %s: %s", name, err))
			}
			continue
		}

		if changed {
			s.Logger.Info("ensure ca in validating webhook", "name", name)
		}
	}

	if len(errList) == 0 {
		return nil
	}

	return errutil.NewAggregate(errList)
}

type nonLeaderElectionController struct {
	controller.Controller
}

func (c *nonLeaderElectionController) NeedLeaderElection() bool {
	return false
}

var _ SecretClient = &secretClient{}

type secretClient struct {
	reader client.Reader
	writer client.Writer
	logger logr.Logger
}

// Create implements SecretClient.
func (s *secretClient) Create(ctx context.Context, secret *corev1.Secret) error {
	err := s.writer.Create(ctx, secret)
	if err == nil {
		s.logger.Info("create secret successfully", "namespace", secret.Namespace, "name", secret.Name)
	}
	return err
}

// Get implements SecretClient.
func (s *secretClient) Get(ctx context.Context, namespace string, name string) (*corev1.Secret, error) {
	var secret corev1.Secret
	err := s.reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &secret)
	if err != nil {
		return nil, err
	}
	return &secret, nil
}

// Update implements SecretClient.
func (s *secretClient) Update(ctx context.Context, secret *corev1.Secret) error {
	err := s.writer.Update(ctx, secret)
	if err == nil {
		s.logger.Info("update secret successfully", "namespace", secret.Namespace, "name", secret.Name)
	}
	return err
}
