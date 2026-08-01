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
	"os"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	shophubv1 "github.com/DevOps-siit-master/shop-operator/api/v1"
)

const (
	// replicaCountStandard / replicaCountHigh are the instance counts the spec
	// mandates for each availability tier (spec 1.2 / 3.1).
	replicaCountStandard = 2
	replicaCountHigh     = 3

	// defaultShopAppImage / defaultShopAppPort are used when the operator is not
	// configured via the SHOP_APP_IMAGE / SHOP_APP_PORT env vars. The image is an
	// operator-level concern (all shops run the same Shop app build), so it lives
	// here rather than on the CRD.
	defaultShopAppImage = "shophub/shop-app:latest"
	defaultShopAppPort  = 8080

	// Condition type surfaced on Shop.status.conditions.
	conditionAvailable = "Available"

	// databaseReadyRequeue is how long we wait before re-checking when the
	// database's operator (CNPG / Redis Enterprise) is not installed in the
	// cluster yet, so we don't hot-loop.
	databaseReadyRequeue = 30 * time.Second
	// deploymentPendingRequeue is a shorter poll used to refresh status while the
	// app Deployment is still rolling out.
	deploymentPendingRequeue = 15 * time.Second
)

// databaseState describes how far along the Shop's backing database is. It lets
// us tell apart "the DB operator isn't installed" from "the DB resource exists
// but hasn't finished provisioning" — two situations that look identical if you
// only track whether the DB custom resource was created.
type databaseState int

const (
	// dbStateOperatorMissing: the database operator's CRD is not installed, so
	// the DB custom resource could not be created at all.
	dbStateOperatorMissing databaseState = iota
	// dbStateProvisioning: the DB custom resource exists but is not yet active
	// (e.g. a REDB with no bound REC, or a CNPG Cluster still bootstrapping).
	dbStateProvisioning
	// dbStateReady: the database reports itself active and usable.
	dbStateReady
)

// ShopReconciler reconciles a Shop object
type ShopReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=shophub.devops-siit.io,resources=shops,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=shophub.devops-siit.io,resources=shops/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=shophub.devops-siit.io,resources=shops/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=clusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=app.redislabs.com,resources=redisenterprisedatabases,verbs=get;list;watch;create;update;patch;delete

// Reconcile drives a Shop CR towards its desired state: a database (via the
// CNPG or Redis Enterprise operator), a Deployment of the Shop app scaled to the
// requested availability tier, and a Service in front of it. Owned child objects
// are garbage-collected automatically when the Shop is deleted (via owner
// references), so no finalizer is needed.
func (r *ShopReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// 1. Fetch the Shop. If it's gone, there is nothing to do — Kubernetes GC
	//    will remove the children we own.
	var shop shophubv1.Shop
	if err := r.Get(ctx, req.NamespacedName, &shop); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 2. Translate the availability tier into a concrete replica count.
	replicas := desiredReplicas(shop.Spec.Availability)

	// 3. Ensure the database exists and collect the env vars the app needs to
	//    connect to it. We still proceed to create the app even when the DB isn't
	//    ready (the pod simply can't start until its credentials secret exists),
	//    and requeue until the DB becomes active.
	dbEnv, dbState, err := r.reconcileDatabase(ctx, &shop)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 4. Reconcile the Shop app Deployment (create or converge) with the
	//    computed replicas and DB wiring.
	deployment, err := r.reconcileDeployment(ctx, &shop, replicas, dbEnv)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 5. Reconcile the Service that fronts the Deployment.
	if err := r.reconcileService(ctx, &shop); err != nil {
		return ctrl.Result{}, err
	}

	// 6. Reflect observed state back onto the Shop status.
	ready := dbState == dbStateReady && deployment.Status.ReadyReplicas == replicas
	if err := r.updateStatus(ctx, &shop, replicas, ready, dbState); err != nil {
		return ctrl.Result{}, err
	}

	switch {
	case dbState != dbStateReady:
		// DB operator missing or still provisioning; check back later.
		log.Info("Database not ready yet, requeueing", "shop", shop.Name, "state", dbState)
		return ctrl.Result{RequeueAfter: databaseReadyRequeue}, nil
	case !ready:
		// App still rolling out; poll status until replicas are ready.
		return ctrl.Result{RequeueAfter: deploymentPendingRequeue}, nil
	default:
		return ctrl.Result{}, nil
	}
}

// desiredReplicas maps the availability tier to a replica count. Anything other
// than "high" (including the empty/defaulted value) is treated as standard.
func desiredReplicas(a shophubv1.Availability) int32 {
	if a == shophubv1.AvailabilityHigh {
		return replicaCountHigh
	}
	return replicaCountStandard
}

// resourceName builds a stable, DNS-safe name for a Shop's child objects.
func resourceName(shop *shophubv1.Shop) string {
	return "shop-" + shop.Name
}

// labelsFor returns the recommended Kubernetes labels shared by all objects a
// Shop owns, so they can be selected together.
func labelsFor(shop *shophubv1.Shop) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "shop",
		"app.kubernetes.io/instance":   shop.Name,
		"app.kubernetes.io/managed-by": "shop-operator",
	}
}

// shopAppImage / shopAppPort read operator-level configuration with sane
// defaults, so the image/port can be overridden per-environment without touching
// the CRD or every Shop CR.
func shopAppImage() string {
	if img := os.Getenv("SHOP_APP_IMAGE"); img != "" {
		return img
	}
	return defaultShopAppImage
}

func shopAppPort() int32 {
	if p := os.Getenv("SHOP_APP_PORT"); p != "" {
		if port, err := strconv.Atoi(p); err == nil {
			return int32(port)
		}
	}
	return defaultShopAppPort
}

// reconcileDatabase provisions the backing store for the Shop and returns the
// env vars the app needs to reach it plus the database's current state. We never
// hard-fail on a missing DB operator: the Shop app can be created and will wait
// for its database.
func (r *ShopReconciler) reconcileDatabase(
	ctx context.Context, shop *shophubv1.Shop,
) (env []corev1.EnvVar, state databaseState, err error) {
	if shop.Spec.DatabaseType == shophubv1.DatabaseLight {
		return r.reconcileRedis(ctx, shop)
	}
	return r.reconcilePostgres(ctx, shop)
}

// reconcilePostgres ensures a CloudNativePG Cluster exists for the Shop and
// wires the app to the "-app" secret CNPG generates (host/port/dbname/user/pass).
func (r *ShopReconciler) reconcilePostgres(
	ctx context.Context, shop *shophubv1.Shop,
) ([]corev1.EnvVar, databaseState, error) {
	clusterName := resourceName(shop) + "-db"
	appSecret := clusterName + "-app" // CNPG convention: <cluster>-app holds credentials.

	cluster := &unstructured.Unstructured{}
	cluster.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "postgresql.cnpg.io",
		Version: "v1",
		Kind:    "Cluster",
	})
	cluster.SetName(clusterName)
	cluster.SetNamespace(shop.Namespace)

	exists, err := r.createOrIgnoreMissingCRD(ctx, shop, cluster, func() error {
		return unstructured.SetNestedField(cluster.Object, map[string]any{
			"instances": int64(1),
			"storage": map[string]any{
				"size": "1Gi",
			},
		}, "spec")
	})
	if err != nil {
		return nil, dbStateProvisioning, err
	}

	// Env is emitted regardless of the DB's readiness: the Deployment references
	// the secret by name, and the pod stays pending until the DB (and thus the
	// secret) exists.
	env := []corev1.EnvVar{
		{Name: "DATABASE_HOST", ValueFrom: secretKeyRef(appSecret, "host")},
		{Name: "DATABASE_PORT", ValueFrom: secretKeyRef(appSecret, "port")},
		{Name: "DATABASE_NAME", ValueFrom: secretKeyRef(appSecret, "dbname")},
		{Name: "DATABASE_USER", ValueFrom: secretKeyRef(appSecret, "username")},
		{Name: "DATABASE_PASSWORD", ValueFrom: secretKeyRef(appSecret, "password")},
	}
	return env, databaseStateFrom(exists, cluster, cnpgClusterReady), nil
}

// reconcileRedis ensures a Redis Enterprise database (REDB) exists for the Shop
// and wires the app to the secret the REDB controller generates.
func (r *ShopReconciler) reconcileRedis(
	ctx context.Context, shop *shophubv1.Shop,
) ([]corev1.EnvVar, databaseState, error) {
	redbName := resourceName(shop) + "-redis"
	// The Redis Enterprise operator publishes credentials in redb-<name>.
	redbSecret := "redb-" + redbName

	redb := &unstructured.Unstructured{}
	redb.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "app.redislabs.com",
		Version: "v1alpha1",
		Kind:    "RedisEnterpriseDatabase",
	})
	redb.SetName(redbName)
	redb.SetNamespace(shop.Namespace)

	exists, err := r.createOrIgnoreMissingCRD(ctx, shop, redb, func() error {
		return unstructured.SetNestedField(redb.Object, map[string]any{
			"memorySize": "100MB",
		}, "spec")
	})
	if err != nil {
		return nil, dbStateProvisioning, err
	}

	env := []corev1.EnvVar{
		{Name: "REDIS_HOST", Value: redbName},
		{Name: "REDIS_PORT", ValueFrom: secretKeyRef(redbSecret, "port")},
		{Name: "REDIS_PASSWORD", ValueFrom: secretKeyRef(redbSecret, "password")},
	}
	return env, databaseStateFrom(exists, redb, redbActive), nil
}

// databaseStateFrom derives the DB state from whether the CR exists and, when it
// does, a resource-specific readiness predicate.
func databaseStateFrom(
	exists bool, obj *unstructured.Unstructured, isReady func(*unstructured.Unstructured) bool,
) databaseState {
	switch {
	case !exists:
		return dbStateOperatorMissing
	case isReady(obj):
		return dbStateReady
	default:
		return dbStateProvisioning
	}
}

// cnpgClusterReady reports whether a CloudNativePG Cluster has at least one ready
// instance (status.readyInstances >= 1).
func cnpgClusterReady(obj *unstructured.Unstructured) bool {
	ready, found, err := unstructured.NestedInt64(obj.Object, "status", "readyInstances")
	return err == nil && found && ready >= 1
}

// redbActive reports whether a Redis Enterprise database has reached "active"
// (status.status == "active").
func redbActive(obj *unstructured.Unstructured) bool {
	status, found, err := unstructured.NestedString(obj.Object, "status", "status")
	return err == nil && found && status == "active"
}

// createOrIgnoreMissingCRD creates obj (owned by shop) if absent. It returns
// (true, nil) when the object exists after the call, and (false, nil) when the
// object's CRD is not installed in the cluster — so the caller can degrade
// gracefully instead of erroring. Any other error is propagated.
func (r *ShopReconciler) createOrIgnoreMissingCRD(
	ctx context.Context, shop *shophubv1.Shop, obj client.Object, mutate func() error,
) (bool, error) {
	log := logf.FromContext(ctx)

	key := client.ObjectKeyFromObject(obj)
	err := r.Get(ctx, key, obj)
	switch {
	case err == nil:
		// Already exists — leave the database's own spec alone (it may have been
		// tuned out-of-band) and just report it present.
		return true, nil
	case apimeta.IsNoMatchError(err):
		// The backing operator's CRD isn't installed. Degrade gracefully.
		log.Info("Database CRD not installed; skipping DB creation",
			"gvk", obj.GetObjectKind().GroupVersionKind().String())
		return false, nil
	case !apierrors.IsNotFound(err):
		return false, err
	}

	// NotFound: build and create it, owned by the Shop.
	if err := mutate(); err != nil {
		return false, err
	}
	if err := controllerutil.SetControllerReference(shop, obj, r.Scheme); err != nil {
		return false, err
	}
	if err := r.Create(ctx, obj); err != nil {
		if apimeta.IsNoMatchError(err) {
			log.Info("Database CRD not installed; skipping DB creation",
				"gvk", obj.GetObjectKind().GroupVersionKind().String())
			return false, nil
		}
		return false, err
	}
	log.Info("Created database resource", "name", key.Name,
		"kind", obj.GetObjectKind().GroupVersionKind().Kind)
	return true, nil
}

// reconcileDeployment creates or converges the Shop app Deployment. It uses
// CreateOrUpdate so repeated reconciles are idempotent and drift (e.g. a manual
// replica change) is corrected back to the desired state.
func (r *ShopReconciler) reconcileDeployment(
	ctx context.Context, shop *shophubv1.Shop, replicas int32, dbEnv []corev1.EnvVar,
) (*appsv1.Deployment, error) {
	log := logf.FromContext(ctx)

	labels := labelsFor(shop)
	port := shopAppPort()

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName(shop),
			Namespace: shop.Namespace,
		},
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, deployment, func() error {
		deployment.Labels = labels
		deployment.Spec.Replicas = &replicas
		deployment.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
		deployment.Spec.Template.Labels = labels
		deployment.Spec.Template.Spec.Containers = []corev1.Container{{
			Name:  "shop",
			Image: shopAppImage(),
			Ports: []corev1.ContainerPort{{
				Name:          "http",
				ContainerPort: port,
				Protocol:      corev1.ProtocolTCP,
			}},
			Env: shopAppEnv(shop, dbEnv),
		}}
		// Own the Deployment so it is deleted with the Shop.
		return controllerutil.SetControllerReference(shop, deployment, r.Scheme)
	})
	if err != nil {
		return nil, err
	}
	if op != controllerutil.OperationResultNone {
		log.Info("Reconciled Deployment", "operation", op, "replicas", replicas)
	}
	return deployment, nil
}

// shopAppEnv assembles the container env: identity/config from the Shop spec
// plus the database connection vars.
func shopAppEnv(shop *shophubv1.Shop, dbEnv []corev1.EnvVar) []corev1.EnvVar {
	env := make([]corev1.EnvVar, 0, 5+len(dbEnv))
	env = append(env,
		corev1.EnvVar{Name: "SHOP_NAME", Value: shop.Name},
		corev1.EnvVar{Name: "SHOP_DISPLAY_NAME", Value: shop.Spec.DisplayName},
		corev1.EnvVar{Name: "DATABASE_TYPE", Value: string(shop.Spec.DatabaseType)},
		corev1.EnvVar{Name: "WALLET_REF", Value: shop.Spec.WalletRef},
		corev1.EnvVar{Name: "DISCORD_CHANNEL_REF", Value: shop.Spec.DiscordChannelRef},
	)
	return append(env, dbEnv...)
}

// reconcileService creates or converges a ClusterIP Service targeting the app.
func (r *ShopReconciler) reconcileService(ctx context.Context, shop *shophubv1.Shop) error {
	labels := labelsFor(shop)
	port := shopAppPort()

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName(shop),
			Namespace: shop.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
		service.Labels = labels
		service.Spec.Selector = labels
		service.Spec.Type = corev1.ServiceTypeClusterIP
		service.Spec.Ports = []corev1.ServicePort{{
			Name:       "http",
			Port:       port,
			TargetPort: intstr.FromString("http"),
			Protocol:   corev1.ProtocolTCP,
		}}
		return controllerutil.SetControllerReference(shop, service, r.Scheme)
	})
	return err
}

// updateStatus writes observed state back onto the Shop, so `kubectl get shop`
// and the ShopHub UI can show progress.
func (r *ShopReconciler) updateStatus(
	ctx context.Context, shop *shophubv1.Shop, replicas int32, ready bool, dbState databaseState,
) error {
	shop.Status.Replicas = replicas
	shop.Status.Ready = ready

	cond := metav1.Condition{
		Type:               conditionAvailable,
		ObservedGeneration: shop.Generation,
	}
	switch {
	case ready:
		cond.Status = metav1.ConditionTrue
		cond.Reason = "ShopReady"
		cond.Message = fmt.Sprintf("Shop app is running with %d replicas", replicas)
	case dbState == dbStateOperatorMissing:
		cond.Status = metav1.ConditionFalse
		cond.Reason = "DatabaseOperatorMissing"
		cond.Message = "The database operator (CNPG/REDB) is not installed in the cluster"
	case dbState == dbStateProvisioning:
		cond.Status = metav1.ConditionFalse
		cond.Reason = "DatabaseProvisioning"
		cond.Message = "Waiting for the database to become active (check the DB resource, e.g. a REDB with no bound REC)"
	default:
		cond.Status = metav1.ConditionFalse
		cond.Reason = "DeploymentProgressing"
		cond.Message = "Shop app Deployment is rolling out"
	}
	apimeta.SetStatusCondition(&shop.Status.Conditions, cond)

	return r.Status().Update(ctx, shop)
}

// secretKeyRef is a small helper for building an env var sourced from a Secret.
func secretKeyRef(secretName, key string) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{
		SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
			Key:                  key,
		},
	}
}

// SetupWithManager sets up the controller with the Manager. Owns() tells
// controller-runtime to also watch the Deployment/Service we create and
// re-reconcile the Shop when they change (e.g. someone edits the replica count).
func (r *ShopReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&shophubv1.Shop{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Named("shop").
		Complete(r)
}
