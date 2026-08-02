/*
Copyright 2026.

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

package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	shophubv1 "github.com/DevOps-siit-master/shop-operator/api/v1"
)

type fakeDiscordClient struct {
	createChannelID  string
	createChannelErr error

	webhookID    string
	webhookToken string
	webhookErr   error

	deleteChannelErr error
	deleteCalledWith string
}

func (f *fakeDiscordClient) CreateChannel(ctx context.Context, token, serverID, name string) (string, error) {
	return f.createChannelID, f.createChannelErr
}

func (f *fakeDiscordClient) CreateWebhook(ctx context.Context, token, channelID, name string) (string, string, error) {
	return f.webhookID, f.webhookToken, f.webhookErr
}

func (f *fakeDiscordClient) DeleteChannel(ctx context.Context, token, channelID string) error {
	f.deleteCalledWith = channelID
	return f.deleteChannelErr
}

func (f *fakeDiscordClient) TransformIntoUrl(id, token string) string {
	return "https://discord.com/api/webhooks/" + id + "/" + token
}

var _ = Describe("DiscordChannel Controller", func() {
	Context("When reconciling a resource", func() {
		const (
			resourceName      = "test-resource"
			resourceNamespace = "default"
			botSecretName     = "discord-bot-credentials"
			botSecretNS       = "default"
		)

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: resourceNamespace,
		}
		discordchannel := &shophubv1.DiscordChannel{}
		fakeClient := &fakeDiscordClient{}

		BeforeEach(func() {
			fakeClient = &fakeDiscordClient{
				createChannelID: "channel-abc",
				webhookID:       "webhook-xyz",
				webhookToken:    "webhook-token-xyz",
			}

			By("creating the bot credentials secret")
			botSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      botSecretName,
					Namespace: botSecretNS,
				},
				StringData: map[string]string{
					"token": "fake-bot-token",
				},
			}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: botSecretName, Namespace: botSecretNS}, &corev1.Secret{})
			if err != nil && errors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, botSecret)).To(Succeed())
			}

			By("creating the custom resource for the Kind DiscordChannel")
			err = k8sClient.Get(ctx, typeNamespacedName, discordchannel)
			if err != nil && errors.IsNotFound(err) {
				resource := &shophubv1.DiscordChannel{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: resourceNamespace,
					},
					Spec: shophubv1.DiscordChannelSpec{
						ChannelName: "test-channel",
						ServerID:    "guild-123",
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &shophubv1.DiscordChannel{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance DiscordChannel")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
			controllerReconciler := &DiscordChannelReconciler{
				Client:                 k8sClient,
				Scheme:                 k8sClient.Scheme(),
				DiscordSecretName:      botSecretName,
				DiscordSecretNamespace: botSecretNS,
				DiscordAPIClient:       fakeClient,
			}
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			botSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: botSecretName, Namespace: botSecretNS},
			}
			_ = k8sClient.Delete(ctx, botSecret)

		})
		It("should successfully reconcile the resource and create channel + webhook", func() {
			By("Reconciling the created resource")
			controllerReconciler := &DiscordChannelReconciler{
				Client:                 k8sClient,
				Scheme:                 k8sClient.Scheme(),
				DiscordSecretName:      botSecretName,
				DiscordSecretNamespace: botSecretNS,
				DiscordAPIClient:       fakeClient,
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("verifying the DiscordChannel status was updated")
			updated := &shophubv1.DiscordChannel{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, updated)).To(Succeed())
			Expect(updated.Status.ChannelID).To(Equal("channel-abc"))
			Expect(updated.Status.WebhookID).To(Equal("webhook-xyz"))

			By("verifying the webhook secret was created with an owner reference")
			webhookSecret := &corev1.Secret{}
			secretName := types.NamespacedName{
				Name:      resourceName + "-webhook",
				Namespace: resourceNamespace,
			}
			Expect(k8sClient.Get(ctx, secretName, webhookSecret)).To(Succeed())
			Expect(string(webhookSecret.Data["url"])).To(Equal("https://discord.com/api/webhooks/webhook-xyz/webhook-token-xyz"))
			Expect(webhookSecret.OwnerReferences).To(HaveLen(1))
			Expect(webhookSecret.OwnerReferences[0].Name).To(Equal(resourceName))
		})

		It("should set a failure condition when channel creation fails", func() {
			fakeClient.createChannelErr = errors.NewBadRequest("simulated discord failure")

			controllerReconciler := &DiscordChannelReconciler{
				Client:                 k8sClient,
				Scheme:                 k8sClient.Scheme(),
				DiscordSecretName:      botSecretName,
				DiscordSecretNamespace: botSecretNS,
				DiscordAPIClient:       fakeClient,
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).To(HaveOccurred())

			updated := &shophubv1.DiscordChannel{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, updated)).To(Succeed())
			Expect(updated.Status.ChannelID).To(BeEmpty())

			found := false
			for _, cond := range updated.Status.Conditions {
				if cond.Type == "Ready" && cond.Status == metav1.ConditionFalse && cond.Reason == "ChannelCreationFailed" {
					found = true
				}
			}
			Expect(found).To(BeTrue(), "expected a Ready=False/ChannelCreationFailed condition")
		})
	})
})
