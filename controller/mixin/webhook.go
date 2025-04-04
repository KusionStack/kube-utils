/**
 * Copyright 2023 KusionStack Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mixin

import (
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/runtime/inject"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

var (
	_ inject.Client             = &WebhookAdmissionHandlerMixin{}
	_ inject.Logger             = &WebhookAdmissionHandlerMixin{}
	_ admission.DecoderInjector = &WebhookAdmissionHandlerMixin{}
)

type WebhookAdmissionHandlerMixin struct {
	Client  client.Client
	Decoder *admission.Decoder
	Logger  logr.Logger
}

func NewWebhookHandlerMixin() *WebhookAdmissionHandlerMixin {
	return &WebhookAdmissionHandlerMixin{}
}

// InjectDecoder implements admission.DecoderInjector.
func (m *WebhookAdmissionHandlerMixin) InjectDecoder(d *admission.Decoder) error {
	m.Decoder = d
	return nil
}

// InjectLogger implements inject.Logger.
func (m *WebhookAdmissionHandlerMixin) InjectLogger(l logr.Logger) error {
	m.Logger = l
	return nil
}

// InjectClient implements inject.Client.
func (m *WebhookAdmissionHandlerMixin) InjectClient(c client.Client) error {
	m.Client = c
	return nil
}
