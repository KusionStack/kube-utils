// Copyright 2023 The KusionStack Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mixin

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func TestNewWebhookHandlerMixin(t *testing.T) {
	mgr, err := manager.New(&rest.Config{}, manager.Options{
		MapperProvider: func(c *rest.Config) (meta.RESTMapper, error) {
			return apiutil.NewDynamicRESTMapper(c, apiutil.WithLazyDiscovery)
		},
	})
	assert.Nil(t, err)

	server := mgr.GetWebhookServer()
	handler := newWebhookTestHandler()
	server.Register("/test", &webhook.Admission{Handler: handler})

	assert.NotNil(t, handler.Client)
	assert.NotNil(t, handler.Decoder)
	assert.NotNil(t, handler.Logger)
}

type webhookTestHandler struct {
	*WebhookAdmissionHandlerMixin
}

func newWebhookTestHandler() *webhookTestHandler {
	return &webhookTestHandler{
		WebhookAdmissionHandlerMixin: NewWebhookHandlerMixin(),
	}
}

func (h *webhookTestHandler) Handle(ctx context.Context, req admission.Request) (resp admission.Response) {
	return admission.Allowed("")
}
