/*
Copyright 2023 The KusionStack Authors.

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

package certmanager

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

var (
	env *envtest.Environment
	mgr manager.Manager

	ctx    context.Context
	cancel context.CancelFunc
	c      client.Client

	certPath              string
	host                  = "test.com"
	alernativeHost        = "test.alternative.com"
	namespace             = "test-ns"
	secretName            = "test-secret"
	mutatingWebhookName   = "test-mutating-webhook"
	validatingWebhookName = "test-validating-webhook"
)

var _ = Describe("Self signer", func() {
	It("sign cert for webhooks reconcile", func() {
		none := admissionregistrationv1.SideEffectClassNone
		mutatingWebhook := &admissionregistrationv1.MutatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{
				Name: mutatingWebhookName,
			},
			Webhooks: []admissionregistrationv1.MutatingWebhook{
				{
					Name: "hook-1.test.io",
					ClientConfig: admissionregistrationv1.WebhookClientConfig{
						URL: ptr.To("https://test.io"),
					},
					SideEffects:             &none,
					AdmissionReviewVersions: []string{"v1beta1"},
				},
				{
					Name: "hook-2.test.io",
					ClientConfig: admissionregistrationv1.WebhookClientConfig{
						URL: ptr.To("https://test.io"),
					},
					SideEffects:             &none,
					AdmissionReviewVersions: []string{"v1beta1"},
				},
			},
		}

		validtingWebhook := &admissionregistrationv1.ValidatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{
				Name: validatingWebhookName,
			},
			Webhooks: []admissionregistrationv1.ValidatingWebhook{
				{
					Name: "hook-1.test.io",
					ClientConfig: admissionregistrationv1.WebhookClientConfig{
						URL: ptr.To("https://test.io"),
					},
					SideEffects:             &none,
					AdmissionReviewVersions: []string{"v1beta1"},
				},
				{
					Name: "hook-2.test.io",
					ClientConfig: admissionregistrationv1.WebhookClientConfig{
						URL: ptr.To("https://test.io"),
					},
					SideEffects:             &none,
					AdmissionReviewVersions: []string{"v1beta1"},
				},
			},
		}

		Expect(c.Create(context.TODO(), mutatingWebhook)).Should(Succeed())
		Expect(c.Create(context.TODO(), validtingWebhook)).Should(Succeed())

		Eventually(func() error {
			if err := c.Get(context.TODO(), types.NamespacedName{Namespace: namespace, Name: mutatingWebhookName}, mutatingWebhook); err != nil {
				return err
			}
			for _, hook := range mutatingWebhook.Webhooks {
				if len(hook.ClientConfig.CABundle) == 0 {
					return fmt.Errorf("ca bundle is empty")
				}
			}

			return nil
		}, 1*time.Second).Should(Succeed())

		Eventually(func() error {
			if err := c.Get(context.TODO(), types.NamespacedName{Namespace: namespace, Name: validatingWebhookName}, validtingWebhook); err != nil {
				return err
			}
			for _, hook := range validtingWebhook.Webhooks {
				if len(hook.ClientConfig.CABundle) == 0 {
					return fmt.Errorf("ca bundle is empty")
				}
			}

			return nil
		}, 1*time.Second).Should(Succeed())
	})
})

func TestResourceContextController(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ResourceContext Test Suite")
}

var _ = BeforeSuite(func() {
	By("bootstrapping test environment")

	ctx, cancel = context.WithCancel(context.TODO())
	logf.SetLogger(zap.New(zap.WriteTo(os.Stdout), zap.UseDevMode(true)))

	env = &envtest.Environment{}

	config, err := env.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(config).NotTo(BeNil())

	certPath, err = os.MkdirTemp(os.TempDir(), "cert-self-sign-test-*")
	Expect(err).ShouldNot(HaveOccurred())

	mgr, err = manager.New(config, manager.Options{
		MetricsBindAddress: "0",
		Scheme:             scheme.Scheme,
		CertDir:            certPath,
	})
	Expect(err).NotTo(HaveOccurred())

	c = mgr.GetClient()
	Expect(createNamespace(c, namespace)).Should(Succeed())

	signer := New(mgr, CertConfig{
		Host:                   host,
		AlternateHosts:         []string{alernativeHost},
		Namespace:              namespace,
		SecretName:             secretName,
		MutatingWebhookNames:   []string{mutatingWebhookName},
		ValidatingWebhookNames: []string{validatingWebhookName},
	})
	Expect(signer.SetupWithManager(mgr)).Should(Succeed())
	// ignore PodDecoration SharedStrategyController
	go func() {
		err = mgr.Start(ctx)
		Expect(err).NotTo(HaveOccurred())
	}()
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")

	cancel()
	err := os.RemoveAll(certPath)
	Expect(err).ShouldNot(HaveOccurred())

	err = env.Stop()
	Expect(err).ShouldNot(HaveOccurred())
})

func createNamespace(c client.Client, namespaceName string) error {
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespaceName,
		},
	}

	return c.Create(context.TODO(), ns)
}
