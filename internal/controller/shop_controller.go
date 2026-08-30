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
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
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

type microservice struct {
	name        string
	envVar      string
	image       string
	port        int32
	ingressPath string
}

var (
	msAuth     = microservice{name: "auth", envVar: "SHOP_AUTH_IMAGE", image: "ghcr.io/devops-siit-master/shophub-auth-service:dev", port: 3000, ingressPath: ""}
	msOrder    = microservice{name: "order", envVar: "SHOP_ORDER_IMAGE", image: "ghcr.io/devops-siit-master/shophub-order-service:dev", port: 3000, ingressPath: "/order-api"}
	msPayment  = microservice{name: "payment", envVar: "SHOP_PAYMENT_IMAGE", image: "ghcr.io/devops-siit-master/shophub-payment-service:dev", port: 3000, ingressPath: "/payment-api"}
	msFrontend = microservice{name: "frontend", envVar: "SHOP_FRONTEND_IMAGE", image: "ghcr.io/devops-siit-master/shophub-frontend:dev", port: 8080, ingressPath: "/"}

	shopMicroservices = []microservice{msAuth, msOrder, msPayment, msFrontend}
)

const (
	// replicaCountStandard / replicaCountHigh are the instance counts the spec
	// mandates for each availability tier (spec 1.2 / 3.1).
	replicaCountStandard = 2
	replicaCountHigh     = 3

	defaultImagePullPolicyEnv = "SHOP_IMAGE_PULL_POLICY"

	shopDomainEnv     = "SHOP_INGRESS_DOMAIN"
	defaultShopDomain = "localtest.me"

	ingressClassEnv     = "SHOP_INGRESS_CLASS"
	defaultIngressClass = "nginx"

	usdtAddressEnv       = "USDT_ADDRESS"
	sepoliaRPCURLEnv     = "SEPOLIA_RPC_URL"
	usdtAddressDefault   = ""
	sepoliaRPCURLDefault = "https://sepolia.drpc.org"

	// defaultRedisImage is the Redis image used for the light (Redis) tier via the
	// OT-Container-Kit operator, overridable with REDIS_IMAGE.
	defaultRedisImage = "quay.io/opstree/redis:v7.0.15"

	// Condition type surfaced on Shop.status.conditions.
	conditionAvailable = "Available"

	// databaseReadyRequeue is how long we wait before re-checking when the
	// database's operator (CNPG / Redis) is not installed in the cluster yet, or
	// its database is still provisioning, so we don't hot-loop.
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
	// (e.g. a Redis StatefulSet still starting, or a CNPG Cluster bootstrapping).
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
// +kubebuilder:rbac:groups=shophub.devops-siit.io,resources=wallets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=shophub.devops-siit.io,resources=discordchannels,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=clusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=redis.redis.opstreelabs.in,resources=redis,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete

// Reconcile drives a Shop CR towards its desired state: a database (via the
// CNPG or OSS Redis operator), a Deployment of the Shop app scaled to the
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

	// 3. Ensure the Wallet and DiscordChannel child resources exist. They are
	//    owned by the Shop (owner reference), so they cascade-delete with it and
	//    need no rollback on the caller's side. Their own controllers own their
	//    behaviour (blockchain account, Discord webhook); we only materialise the
	//    CR from the Shop's inline config.
	if err := r.reconcileWallet(ctx, &shop); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileDiscordChannel(ctx, &shop); err != nil {
		return ctrl.Result{}, err
	}

	// 4. Ensure the database exists and collect the env vars the app needs to
	//    connect to it. We still proceed to create the app even when the DB isn't
	//    ready (the pod simply can't start until its credentials secret exists),
	//    and requeue until the DB becomes active.
	dbEnv, dbState, err := r.reconcileDatabase(ctx, &shop)
	if err != nil {
		return ctrl.Result{}, err
	}

	authSecretName, err := r.ensureAuthSecret(ctx, &shop)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 5. Reconcile the Shop app Deployment (create or converge) with the
	//    computed replicas and DB wiring.
	allReady := dbState == dbStateReady
	for _, ms := range shopMicroservices {
		deployment, err := r.reconcileDeployment(ctx, &shop, ms, replicas, dbEnv, authSecretName)
		if err != nil {
			return ctrl.Result{}, err
		}

		// 6. Reconcile the Service that fronts the Deployment.
		if err := r.reconcileService(ctx, &shop, ms); err != nil {
			return ctrl.Result{}, err
		}

		// 7. Reflect observed state back onto the Shop status.
		if deployment.Status.ReadyReplicas != replicas {
			allReady = false
		}
	}

	if err := r.updateStatus(ctx, &shop, replicas, allReady, dbState); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.reconcileIngress(ctx, &shop); err != nil {
		return ctrl.Result{}, err
	}

	switch {
	case dbState != dbStateReady:
		// DB operator missing or still provisioning; check back later.
		log.Info("Database not ready yet, requeueing", "shop", shop.Name, "state", dbState)
		return ctrl.Result{RequeueAfter: databaseReadyRequeue}, nil
	case !allReady:
		// App still rolling out; poll status until replicas are ready.
		return ctrl.Result{RequeueAfter: deploymentPendingRequeue}, nil
	default:
		return ctrl.Result{}, nil
	}
}

func serviceResourceName(shop *shophubv1.Shop, m microservice) string {
	return resourceName(shop) + "-" + m.name
}

func labelsForComponent(shop *shophubv1.Shop, componentName string) map[string]string {
	l := labelsFor(shop)
	l["app.kubernetes.io/component"] = componentName
	return l
}

func microserviceImage(m microservice) string {
	if img := os.Getenv(m.envVar); img != "" {
		return img
	}
	return m.image
}

func imagePullPolicy() corev1.PullPolicy {
	if p := os.Getenv(defaultImagePullPolicyEnv); p != "" {
		return corev1.PullPolicy(p)
	}

	return corev1.PullIfNotPresent
}

func shopHost(shop *shophubv1.Shop) string {
	domain := os.Getenv(shopDomainEnv)
	if domain == "" {
		domain = defaultShopDomain
	}
	return shop.Name + "." + domain
}

func ingressClassName() string {
	if c := os.Getenv(ingressClassEnv); c != "" {
		return c
	}
	return defaultIngressClass
}

func ingressResourceName(shop *shophubv1.Shop) string {
	return resourceName(shop) + "-ingress"
}

// desiredReplicas maps the availability tier to a replica count. Anything other
// than "high" (including the empty/defaulted value) is treated as standard.
func desiredReplicas(a shophubv1.Availability) int32 {
	if a == shophubv1.AvailabilityHigh {
		return replicaCountHigh
	}
	return replicaCountStandard
}

func serviceURL(shop *shophubv1.Shop, name string, port int32) string {
	return fmt.Sprintf("http://%s-%s:%d", resourceName(shop), name, port)
}

// resourceName builds a stable, DNS-safe name for a Shop's child objects.
func resourceName(shop *shophubv1.Shop) string {
	return "shop-" + shop.Name
}

// walletResourceName / discordResourceName derive the names of the Wallet and
// DiscordChannel resources the Shop owns, from the Shop's name.
func walletResourceName(shop *shophubv1.Shop) string {
	return resourceName(shop) + "-wallet"
}

func discordResourceName(shop *shophubv1.Shop) string {
	return resourceName(shop) + "-discord"
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

func (r *ShopReconciler) ensureAuthSecret(ctx context.Context, shop *shophubv1.Shop) (string, error) {
	name := resourceName(shop) + "-auth-jwt"
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: shop.Namespace},
	}

	err := r.Get(ctx, client.ObjectKeyFromObject(secret), secret)
	if err == nil {
		return name, nil
	}

	if !apierrors.IsNotFound(err) {
		return "", err
	}

	accessSecret, err := randomPassword()
	if err != nil {
		return "", err
	}

	refreshSecret, err := randomPassword()
	if err != nil {
		return "", err
	}

	secret.StringData = map[string]string{
		"access":  accessSecret,
		"refresh": refreshSecret,
	}

	if err := controllerutil.SetControllerReference(shop, secret, r.Scheme); err != nil {
		return "", err
	}

	return name, r.Create(ctx, secret)
}

func (r *ShopReconciler) reconcileIngress(ctx context.Context, shop *shophubv1.Shop) error {
	log := logf.FromContext(ctx)

	ingress := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ingressResourceName(shop),
			Namespace: shop.Namespace,
		},
	}

	prefixType := networkingv1.PathTypePrefix
	implSpecific := networkingv1.PathTypeImplementationSpecific
	class := ingressClassName()

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, ingress, func() error {
		ingress.Labels = labelsFor(shop)

		if ingress.Annotations == nil {
			ingress.Annotations = map[string]string{}
		}

		ingress.Annotations["nginx.ingress.kubernetes.io/rewrite-target"] = "/$2"
		ingress.Annotations["nginx.ingress.kubernetes.io/use-regex"] = "true"

		ingress.Spec.IngressClassName = &class

		var paths []networkingv1.HTTPIngressPath
		for _, m := range shopMicroservices {
			if m.ingressPath == "" {
				continue
			}

			p := m.ingressPath
			pt := &prefixType
			if p != "/" {
				p = p + "(/|$)(.*)"
				pt = &implSpecific
			} else {
				p = "/()(.*)"
				pt = &implSpecific
			}

			paths = append(paths, networkingv1.HTTPIngressPath{
				Path:     p,
				PathType: pt,
				Backend: networkingv1.IngressBackend{
					Service: &networkingv1.IngressServiceBackend{
						Name: serviceResourceName(shop, m),
						Port: networkingv1.ServiceBackendPort{Number: m.port},
					},
				},
			})
		}

		ingress.Spec.Rules = []networkingv1.IngressRule{{
			Host: shopHost(shop),
			IngressRuleValue: networkingv1.IngressRuleValue{
				HTTP: &networkingv1.HTTPIngressRuleValue{Paths: paths},
			},
		}}

		return controllerutil.SetControllerReference(shop, ingress, r.Scheme)
	})
	if err != nil {
		return err
	}
	if op != controllerutil.OperationResultNone {
		log.Info("Reconciled Ingress", "operation", op, "host", shopHost(shop))
	}
	return nil

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

// reconcileRedis ensures an open-source Redis (OT-Container-Kit operator) exists
// for the Shop. Unlike a managed DB, the OSS operator does not mint credentials,
// so we generate a password Secret ourselves, reference it from the Redis CR, and
// wire the app to the operator-created "<name>" Service on port 6379.
func (r *ShopReconciler) reconcileRedis(
	ctx context.Context, shop *shophubv1.Shop,
) ([]corev1.EnvVar, databaseState, error) {
	redisName := resourceName(shop) + "-redis"
	authSecret := redisName + "-auth"

	// 1. Ensure a password Secret exists (generated once; never rotated here).
	if err := r.ensureRedisAuthSecret(ctx, shop, authSecret); err != nil {
		return nil, dbStateProvisioning, err
	}

	// 2. Ensure the Redis CR exists, protected by that Secret.
	redis := &unstructured.Unstructured{}
	redis.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "redis.redis.opstreelabs.in",
		Version: "v1beta2",
		Kind:    "Redis",
	})
	redis.SetName(redisName)
	redis.SetNamespace(shop.Namespace)

	exists, err := r.createOrIgnoreMissingCRD(ctx, shop, redis, func() error {
		return unstructured.SetNestedField(redis.Object, map[string]any{
			"kubernetesConfig": map[string]any{
				"image":           redisImage(),
				"imagePullPolicy": "IfNotPresent",
				"redisSecret": map[string]any{
					"name": authSecret,
					"key":  "password",
				},
			},
		}, "spec")
	})
	if err != nil {
		return nil, dbStateProvisioning, err
	}

	env := []corev1.EnvVar{
		{Name: "REDIS_HOST", Value: redisName},
		{Name: "REDIS_PORT", Value: "6379"},
		{Name: "REDIS_PASSWORD", ValueFrom: secretKeyRef(authSecret, "password")},
	}
	if !exists {
		return env, dbStateOperatorMissing, nil
	}

	// The Redis CR exposes no usable status, so readiness is taken from the
	// StatefulSet the operator creates (same name as the CR).
	ready, err := r.statefulSetReady(ctx, shop.Namespace, redisName)
	if err != nil {
		return nil, dbStateProvisioning, err
	}
	if ready {
		return env, dbStateReady, nil
	}
	return env, dbStateProvisioning, nil
}

// ensureRedisAuthSecret creates a Shop-owned Secret holding a generated Redis
// password if one does not already exist. It never overwrites an existing secret,
// so the password stays stable across reconciles.
func (r *ShopReconciler) ensureRedisAuthSecret(
	ctx context.Context, shop *shophubv1.Shop, name string,
) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: shop.Namespace},
	}
	err := r.Get(ctx, client.ObjectKeyFromObject(secret), secret)
	if err == nil {
		return nil // already exists; keep the current password
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	password, err := randomPassword()
	if err != nil {
		return err
	}
	secret.StringData = map[string]string{"password": password}
	if err := controllerutil.SetControllerReference(shop, secret, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, secret)
}

// statefulSetReady reports whether the named StatefulSet has at least one ready
// replica. A missing StatefulSet is treated as not-ready (not an error).
func (r *ShopReconciler) statefulSetReady(
	ctx context.Context, namespace, name string,
) (bool, error) {
	var sts appsv1.StatefulSet
	err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &sts)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return sts.Status.ReadyReplicas >= 1, nil
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

// randomPassword returns a URL-safe 32-char random secret.
func randomPassword() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// redisImage returns the Redis image for the light tier, overridable via REDIS_IMAGE.
func redisImage() string {
	if img := os.Getenv("REDIS_IMAGE"); img != "" {
		return img
	}
	return defaultRedisImage
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
	ctx context.Context, shop *shophubv1.Shop, m microservice, replicas int32, dbEnv []corev1.EnvVar, authSecretName string,
) (*appsv1.Deployment, error) {
	log := logf.FromContext(ctx)

	labels := labelsForComponent(shop, m.name)

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceResourceName(shop, m),
			Namespace: shop.Namespace,
		},
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, deployment, func() error {
		deployment.Labels = labels
		deployment.Spec.Replicas = &replicas
		deployment.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
		deployment.Spec.Template.Labels = labels

		if len(deployment.Spec.Template.Spec.Containers) == 0 {
			deployment.Spec.Template.Spec.Containers = []corev1.Container{{}}
		}

		c := &deployment.Spec.Template.Spec.Containers[0]
		c.Name = m.name
		c.Image = microserviceImage(m)
		c.ImagePullPolicy = imagePullPolicy()
		c.Ports = []corev1.ContainerPort{{
			Name:          "http",
			ContainerPort: m.port,
			Protocol:      corev1.ProtocolTCP,
		}}
		c.Env = shopAppEnv(shop, m, dbEnv, authSecretName)

		return controllerutil.SetControllerReference(shop, deployment, r.Scheme)
	})
	if err != nil {
		return nil, err
	}
	if op != controllerutil.OperationResultNone {
		log.Info("Reconciled Deployment", "operation", op, "service", m.name, "replicas", replicas)
	}
	return deployment, nil
}

func sepoliaRPCURL() string {
	if sepoliaRPCURL := os.Getenv(sepoliaRPCURLEnv); sepoliaRPCURL != "" {
		return sepoliaRPCURL
	}
	return sepoliaRPCURLDefault
}

func usdtAddress() string {
	if usdtAdress := os.Getenv(usdtAddressEnv); usdtAdress != "" {
		return usdtAdress
	}
	return usdtAddressDefault
}

// shopAppEnv assembles the container env: identity/config from the Shop spec
// plus the database connection vars.
func shopAppEnv(shop *shophubv1.Shop, m microservice, dbEnv []corev1.EnvVar, authSecretName string) []corev1.EnvVar {
	env := []corev1.EnvVar{
		{Name: "SHOP_NAME", Value: shop.Name},
		{Name: "SERVICE_NAME", Value: m.name},
		{Name: "SHOP_DISPLAY_NAME", Value: shop.Spec.DisplayName},
		{Name: "DATABASE_TYPE", Value: string(shop.Spec.DatabaseType)},
		{Name: "DISCORD_CHANNEL_REF", Value: discordResourceName(shop)},
		{Name: "PORT", Value: fmt.Sprintf("%d", m.port)},
	}

	switch m.name {
	case "frontend":
		env = append(env,
			corev1.EnvVar{Name: "AUTH_API_URL", Value: "http://" + resourceName(shop) + "-auth"},
			corev1.EnvVar{Name: "VITE_ORDER_API", Value: "http://" + resourceName(shop) + "-order"},
			corev1.EnvVar{Name: "VITE_PAYMENT_API", Value: "http://" + resourceName(shop) + "-payment"},
		)
	case "payment":
		env = append(env,
			corev1.EnvVar{Name: "WALLET_REF", Value: walletResourceName(shop)},
			corev1.EnvVar{Name: "SHOP_WALLET_ADDRESS", Value: shop.Spec.Wallet.Address},
			corev1.EnvVar{Name: "ORDER_API_URL", Value: serviceURL(shop, "order", msOrder.port)},
			corev1.EnvVar{Name: "USDT_ADDRESS", Value: usdtAddress()},
			corev1.EnvVar{Name: "SEPOLIA_RPC_URL", Value: sepoliaRPCURL()},
		)
		env = append(env, dbEnv...)
	case "auth":
		env = append(env,
			corev1.EnvVar{Name: "JWT_ACCESS_SECRET", ValueFrom: secretKeyRef(authSecretName, "access")},
			corev1.EnvVar{Name: "JWT_REFRESH_SECRET", ValueFrom: secretKeyRef(authSecretName, "refresh")},
		)
		env = append(env, dbEnv...)
	case "order":
		env = append(env, dbEnv...)
	}

	return env
}

// reconcileService creates or converges a ClusterIP Service targeting the app.
func (r *ShopReconciler) reconcileService(ctx context.Context, shop *shophubv1.Shop, m microservice) error {
	labels := labelsForComponent(shop, m.name)

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceResourceName(shop, m),
			Namespace: shop.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
		service.Labels = labels
		service.Spec.Selector = labels
		service.Spec.Type = corev1.ServiceTypeClusterIP
		service.Spec.Ports = []corev1.ServicePort{{
			Name:       "http",
			Port:       m.port,
			TargetPort: intstr.FromString("http"),
			Protocol:   corev1.ProtocolTCP,
		}}
		return controllerutil.SetControllerReference(shop, service, r.Scheme)
	})
	return err
}

// reconcileWallet creates or converges the Wallet the Shop owns, propagating the
// admin-supplied payout address from the Shop's inline config. The Wallet's own
// controller acts on it independently; the owner reference makes it cascade with
// the Shop.
func (r *ShopReconciler) reconcileWallet(ctx context.Context, shop *shophubv1.Shop) error {
	wallet := &shophubv1.Wallet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      walletResourceName(shop),
			Namespace: shop.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, wallet, func() error {
		wallet.Labels = labelsFor(shop)
		wallet.Spec.ShopRef = shop.Name
		wallet.Spec.Address = shop.Spec.Wallet.Address
		return controllerutil.SetControllerReference(shop, wallet, r.Scheme)
	})
	return err
}

// reconcileDiscordChannel creates or converges the DiscordChannel the Shop owns,
// propagating the channel/server config from the Shop's inline config. The
// DiscordChannel controller manages the actual channel/webhook lifecycle.
func (r *ShopReconciler) reconcileDiscordChannel(ctx context.Context, shop *shophubv1.Shop) error {
	channel := &shophubv1.DiscordChannel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      discordResourceName(shop),
			Namespace: shop.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, channel, func() error {
		channel.Labels = labelsFor(shop)
		channel.Spec.ChannelName = shop.Spec.DiscordChannel.ChannelName
		channel.Spec.ServerID = shop.Spec.DiscordChannel.ServerID
		return controllerutil.SetControllerReference(shop, channel, r.Scheme)
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
		cond.Message = "The database operator (CNPG for standard, Redis for light) is not installed in the cluster"
	case dbState == dbStateProvisioning:
		cond.Status = metav1.ConditionFalse
		cond.Reason = "DatabaseProvisioning"
		cond.Message = "Waiting for the database to become ready"
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
		Owns(&shophubv1.Wallet{}).
		Owns(&networkingv1.Ingress{}).
		Owns(&shophubv1.DiscordChannel{}).
		Named("shop").
		Complete(r)
}
