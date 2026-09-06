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

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	shophubv1 "github.com/DevOps-siit-master/shop-operator/api/v1"
	"github.com/DevOps-siit-master/shop-operator/internal/wallet"
)

// WalletReconciler reconciles a Wallet object
type WalletReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=shophub.devops-siit.io,resources=wallets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=shophub.devops-siit.io,resources=wallets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=shophub.devops-siit.io,resources=wallets/finalizers,verbs=update

// Reconcile validates the payout address a Shop declared and publishes it on
// the Wallet's status, which is the one place the rest of the platform reads it
// from. The address is supplied by the shop owner (spec 1.2); the operator
// deliberately does not generate a key pair, because the private key of an
// account that receives the owner's money should not live in the cluster.
func (r *WalletReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var w shophubv1.Wallet
	if err := r.Get(ctx, req.NamespacedName, &w); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	switch {
	case w.Spec.Address == "":
		return ctrl.Result{}, r.setStatus(ctx, &w, "", false, "AddressMissing",
			"The shop has no payout address configured")

	case !wallet.IsValidAddress(w.Spec.Address):
		log.Info("Rejecting malformed payout address", "wallet", w.Name)
		return ctrl.Result{}, r.setStatus(ctx, &w, "", false, "InvalidAddress",
			fmt.Sprintf("%q is not a valid EVM address", w.Spec.Address))

	default:
		return ctrl.Result{}, r.setStatus(ctx, &w, w.Spec.Address, true, "AddressAccepted",
			"Payout address accepted")
	}
}

// setStatus writes the observed address and readiness back onto the Wallet, so
// `kubectl get wallet` and the ShopHub panel can show whether payments to this
// shop can be verified at all.
func (r *WalletReconciler) setStatus(
	ctx context.Context, w *shophubv1.Wallet,
	address string, ready bool, reason, message string,
) error {
	w.Status.Address = address
	w.Status.Ready = ready

	status := metav1.ConditionFalse
	if ready {
		status = metav1.ConditionTrue
	}
	apimeta.SetStatusCondition(&w.Status.Conditions, metav1.Condition{
		Type:               conditionReadyType,
		Status:             status,
		ObservedGeneration: w.Generation,
		Reason:             reason,
		Message:            message,
	})

	return r.Status().Update(ctx, w)
}

// SetupWithManager sets up the controller with the Manager.
func (r *WalletReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&shophubv1.Wallet{}).
		Named("wallet").
		Complete(r)
}
