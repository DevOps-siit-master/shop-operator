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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	shophubv1 "github.com/DevOps-siit-master/shop-operator/api/v1"
)

var _ = Describe("Shop Controller", func() {
	Context("When reconciling a resource", func() {
		const (
			resourceName      = "test-resource"
			resourceNamespace = "default"
		)

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: resourceNamespace,
		}
		childName := types.NamespacedName{
			Name:      "shop-" + resourceName,
			Namespace: resourceNamespace,
		}
		shop := &shophubv1.Shop{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind Shop")
			err := k8sClient.Get(ctx, typeNamespacedName, shop)
			if err != nil && errors.IsNotFound(err) {
				resource := &shophubv1.Shop{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: resourceNamespace,
					},
					Spec: shophubv1.ShopSpec{
						Availability: shophubv1.AvailabilityHigh,
						DatabaseType: shophubv1.DatabaseStandard,
						Wallet: shophubv1.WalletConfig{
							Address: "0xtest",
						},
						DiscordChannel: shophubv1.DiscordChannelConfig{
							ChannelName: "test-channel",
							ServerID:    "test-server",
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &shophubv1.Shop{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance Shop")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})

		It("should create a Deployment scaled to the availability tier", func() {
			By("Reconciling the created resource")
			controllerReconciler := &ShopReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("checking the Deployment exists with 3 replicas (high availability)")
			deployment := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, childName, deployment)).To(Succeed())
			Expect(deployment.Spec.Replicas).NotTo(BeNil())
			Expect(*deployment.Spec.Replicas).To(Equal(int32(3)))

			By("checking the app container carries the wallet/discord references")
			container := deployment.Spec.Template.Spec.Containers[0]
			Expect(envValue(container.Env, "WALLET_REF")).To(Equal("shop-" + resourceName + "-wallet"))
			Expect(envValue(container.Env, "WALLET_ADDRESS")).To(Equal("0xtest"))
			Expect(envValue(container.Env, "DISCORD_CHANNEL_REF")).To(Equal("shop-" + resourceName + "-discord"))

			By("checking the operator created the owned Wallet from the Shop's inline config")
			wallet := &shophubv1.Wallet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      "shop-" + resourceName + "-wallet",
				Namespace: resourceNamespace,
			}, wallet)).To(Succeed())
			Expect(wallet.Spec.Address).To(Equal("0xtest"))
			Expect(wallet.OwnerReferences).NotTo(BeEmpty())

			By("checking the operator created the owned DiscordChannel from the Shop's inline config")
			channel := &shophubv1.DiscordChannel{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      "shop-" + resourceName + "-discord",
				Namespace: resourceNamespace,
			}, channel)).To(Succeed())
			Expect(channel.Spec.ChannelName).To(Equal("test-channel"))
			Expect(channel.Spec.ServerID).To(Equal("test-server"))
			Expect(channel.OwnerReferences).NotTo(BeEmpty())

			By("checking the Service exists")
			service := &corev1.Service{}
			Expect(k8sClient.Get(ctx, childName, service)).To(Succeed())

			By("checking the Shop status reports the replica count")
			updated := &shophubv1.Shop{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, updated)).To(Succeed())
			Expect(updated.Status.Replicas).To(Equal(int32(3)))
		})

		It("should scale a standard-availability Shop to 2 replicas", func() {
			By("switching the Shop to standard availability")
			resource := &shophubv1.Shop{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, resource)).To(Succeed())
			resource.Spec.Availability = shophubv1.AvailabilityStandard
			Expect(k8sClient.Update(ctx, resource)).To(Succeed())

			controllerReconciler := &ShopReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			deployment := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, childName, deployment)).To(Succeed())
			Expect(*deployment.Spec.Replicas).To(Equal(int32(2)))
		})
	})
})

// envValue returns the plain value of the named env var, or "" if not found.
func envValue(env []corev1.EnvVar, name string) string {
	for _, e := range env {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}
