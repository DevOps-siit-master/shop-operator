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
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	shophubv1 "github.com/DevOps-siit-master/shop-operator/api/v1"
	"github.com/DevOps-siit-master/shop-operator/internal/discord"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
)

// DiscordChannelReconciler reconciles a DiscordChannel object
type DiscordChannelReconciler struct {
	client.Client
	Scheme                 *runtime.Scheme
	DiscordSecretName      string
	DiscordSecretNamespace string
	DiscordAPIClient       discord.Client
}

const discordChannelFinalizer = "shophub.devops-siit.io/discordchannel-finalizer"

// +kubebuilder:rbac:groups=shophub.devops-siit.io,resources=discordchannels,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=shophub.devops-siit.io,resources=discordchannels/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=shophub.devops-siit.io,resources=discordchannels/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the DiscordChannel object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/reconcile
func (r *DiscordChannelReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var discordChannel shophubv1.DiscordChannel
	if err := r.Get(ctx, req.NamespacedName, &discordChannel); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	discordBotSecret := corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: r.DiscordSecretNamespace, Name: r.DiscordSecretName}, &discordBotSecret); err != nil {
		// Handle exception, this will fail just tell why
		if apierrors.IsNotFound(err) {
			log.Error(err, "discord bot credentials secret not found")

			meta.SetStatusCondition(&discordChannel.Status.Conditions, metav1.Condition{
				Type:    "Ready",
				Status:  metav1.ConditionFalse,
				Reason:  "SecretNotFound",
				Message: fmt.Sprintf("secret %s/%s not found", r.DiscordSecretNamespace, r.DiscordSecretName),
			})

			if statusErr := r.Status().Update(ctx, &discordChannel); statusErr != nil {
				log.Error(statusErr, "failed to update status")
			}

			return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil

		}
		log.Error(err, "failed to fetch discord bot credentials")
		return ctrl.Result{}, err
	}

	token, ok := discordBotSecret.Data["token"]
	if !ok {
		err := fmt.Errorf("secret %s/%s not found", r.DiscordSecretNamespace, r.DiscordSecretName)
		log.Error(err, "malformed discord bot credentials secret")

		meta.SetStatusCondition(&discordChannel.Status.Conditions, metav1.Condition{
			Type:    "Ready",
			Status:  metav1.ConditionFalse,
			Reason:  "SecretMissingKey",
			Message: err.Error(),
		})

		if statusErr := r.Status().Update(ctx, &discordChannel); statusErr != nil {
			log.Error(statusErr, "failed to update status")
		}

		return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil

	}

	if !discordChannel.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&discordChannel, discordChannelFinalizer) {
			if discordChannel.Status.ChannelID != "" {
				if err := r.DiscordAPIClient.DeleteChannel(ctx, string(token), discordChannel.Status.ChannelID); err != nil {
					log.Error(err, "failed to delete discord channel during cleanup")
					return ctrl.Result{}, err
				}
			}

			controllerutil.RemoveFinalizer(&discordChannel, discordChannelFinalizer)
			if err := r.Update(ctx, &discordChannel); err != nil {
				log.Error(err, "failed to remove finalizer")
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&discordChannel, discordChannelFinalizer) {
		controllerutil.AddFinalizer(&discordChannel, discordChannelFinalizer)
		if err := r.Update(ctx, &discordChannel); err != nil {
			log.Error(err, "failed to add finalizer")
			return ctrl.Result{}, err
		}
	}

	if discordChannel.Status.WebhookID != "" {
		return ctrl.Result{}, nil
	}

	if discordChannel.Status.ChannelID == "" {
		channelId, err := r.DiscordAPIClient.CreateChannel(ctx, string(token), discordChannel.Spec.ServerID, discordChannel.Spec.ChannelName)
		if err != nil {
			log.Error(err, "failed to create discord channel")
			meta.SetStatusCondition(&discordChannel.Status.Conditions, metav1.Condition{
				Type:    "Ready",
				Status:  metav1.ConditionFalse,
				Reason:  "ChannelCreationFailed",
				Message: err.Error(),
			})
			if statusErr := r.Status().Update(ctx, &discordChannel); statusErr != nil {
				log.Error(statusErr, "failed to update status")
			}
			return ctrl.Result{}, err
		}

		discordChannel.Status.ChannelID = channelId

		if statusErr := r.Status().Update(ctx, &discordChannel); statusErr != nil {
			log.Error(statusErr, "failed to update status")
			return ctrl.Result{}, statusErr
		}
	}

	if discordChannel.Status.WebhookID == "" {
		// TODO: santize the name, create something for discord channel name see how to save secret
		webhookId, webhookToken, err := r.DiscordAPIClient.CreateWebhook(ctx, string(token), discordChannel.Status.ChannelID, discordChannel.Spec.ChannelName)
		if err != nil {
			log.Error(err, "failed to create discord webhook")
			meta.SetStatusCondition(&discordChannel.Status.Conditions, metav1.Condition{
				Type:    "Ready",
				Status:  metav1.ConditionFalse,
				Reason:  "WebhookCreationFailed",
				Message: err.Error(),
			})
			if statusErr := r.Status().Update(ctx, &discordChannel); statusErr != nil {
				log.Error(statusErr, "failed to update status")
			}
			return ctrl.Result{}, err
		}

		webhookSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      discordChannel.Name + "-webhook",
				Namespace: discordChannel.Namespace,
			},
			StringData: map[string]string{
				"url": r.DiscordAPIClient.TransformIntoUrl(webhookId, webhookToken),
			},
		}

		if err := ctrl.SetControllerReference(&discordChannel, webhookSecret, r.Scheme); err != nil {
			log.Error(err, "failed to set owner reference on webhook secret")
			return ctrl.Result{}, err
		}

		if err := r.Create(ctx, webhookSecret); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				log.Error(err, "failed to create webhook secret")
				return ctrl.Result{}, err
			}
		}

		discordChannel.Status.WebhookID = webhookId
		meta.SetStatusCondition(&discordChannel.Status.Conditions, metav1.Condition{
			Type:   "Ready",
			Status: metav1.ConditionTrue,
			Reason: "ChannelAndWebhookReady",
		})

		if statusErr := r.Status().Update(ctx, &discordChannel); statusErr != nil {
			log.Error(statusErr, "failed to update status")
			return ctrl.Result{}, statusErr
		}

	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *DiscordChannelReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&shophubv1.DiscordChannel{}).
		Named("discordchannel").
		Complete(r)
}
