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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	shophubv1 "github.com/DevOps-siit-master/shop-operator/api/v1"
)

// labelShop is the static label stamped on every per-Shop alert rule so
// Alertmanager can route by Shop even though every Shop's pods, and every
// Shop's AlertmanagerConfig, live in one shared namespace.
const labelShop = "shop"

func shopAlertRule(name, expr, forDuration, severity, shopName, summary, description string) map[string]any {
	return map[string]any{
		"alert": name,
		"expr":  expr,
		"for":   forDuration,
		"labels": map[string]any{
			"severity": severity,
			labelShop:  shopName,
		},
		"annotations": map[string]any{
			"summary":     summary,
			"description": description,
		},
	}
}

// reconcilePrometheusRule creates or converges the PrometheusRule carrying
// this Shop's own alert rules, scoped to its pods via shopMetricsScope and
// labeled so reconcileAlertmanagerConfig's route can pick only these alerts
// out for this Shop's Discord channel. Skipped gracefully if the
// PrometheusRule CRD isn't installed, same as reconcileServiceMonitor.
func (r *ShopReconciler) reconcilePrometheusRule(ctx context.Context, shop *shophubv1.Shop) error {
	log := logf.FromContext(ctx)
	scope := shopMetricsScope(shop)

	displayName := shop.Spec.DisplayName
	if displayName == "" {
		displayName = shop.Name
	}

	rules := []any{
		shopAlertRule(
			"ShopHighErrorRate",
			fmt.Sprintf(`sum(rate(http_requests_total{%s, status_code=~"5.."}[5m])) / sum(rate(http_requests_total{%s}[5m])) > 0.05`, scope, scope),
			"5m", "warning", shop.Name,
			fmt.Sprintf("%s: high 5xx error rate", displayName),
			fmt.Sprintf("Shop %s has had a 5xx error rate above 5%% for 5 minutes.", displayName),
		),
		shopAlertRule(
			"ShopPodCrashLooping",
			fmt.Sprintf(`increase(kube_pod_container_status_restarts_total{%s}[15m]) > 3`, scope),
			"5m", "critical", shop.Name,
			fmt.Sprintf("%s: pod is crash-looping", displayName),
			fmt.Sprintf("A pod belonging to Shop %s has restarted more than 3 times in the last 15 minutes.", displayName),
		),
	}

	pr := &unstructured.Unstructured{}
	pr.SetGroupVersionKind(schema.GroupVersionKind{
		Group: monitoringAPIGroup, Version: "v1", Kind: "PrometheusRule",
	})
	pr.SetName(resourceName(shop) + "-alerts")
	pr.SetNamespace(shop.Namespace)

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pr, func() error {
		labels := labelsFor(shop)
		// Required: kube-prometheus-stack's Prometheus only watches
		// PrometheusRules carrying this label (its ruleSelector) — same
		// requirement as reconcileServiceMonitor's ServiceMonitor.
		labels["release"] = "kube-prometheus-stack"
		pr.SetLabels(labels)

		if err := unstructured.SetNestedField(pr.Object, []any{
			map[string]any{
				keyName: "shop.rules",
				"rules": rules,
			},
		}, "spec", "groups"); err != nil {
			return err
		}
		return controllerutil.SetControllerReference(shop, pr, r.Scheme)
	})
	if err != nil {
		if apimeta.IsNoMatchError(err) {
			log.Info("PrometheusRule CRD not installed; skipping shop alert rules")
			return nil
		}
		return err
	}
	return nil
}

// reconcileAlertmanagerConfig creates or converges the AlertmanagerConfig
// that routes this Shop's own alerts (see reconcilePrometheusRule) to its
// own Discord channel, via the webhook Secret the DiscordChannel controller
// creates once the channel/webhook exist. Skipped (not an error) until that
// Secret exists — the owned DiscordChannel's status update re-triggers this
// Reconcile once it's ready, via SetupWithManager's Owns(&DiscordChannel{}).
func (r *ShopReconciler) reconcileAlertmanagerConfig(ctx context.Context, shop *shophubv1.Shop) error {
	log := logf.FromContext(ctx)

	webhookSecretName := discordResourceName(shop) + "-webhook"
	var secret corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: shop.Namespace, Name: webhookSecretName}, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("discord webhook secret not ready yet; skipping alert routing for now", "secret", webhookSecretName)
			return nil
		}
		return err
	}

	receiverName := "discord-" + resourceName(shop)

	amc := &unstructured.Unstructured{}
	amc.SetGroupVersionKind(schema.GroupVersionKind{
		Group: monitoringAPIGroup, Version: "v1alpha1", Kind: "AlertmanagerConfig",
	})
	amc.SetName(resourceName(shop) + "-alert-routing")
	amc.SetNamespace(shop.Namespace)

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, amc, func() error {
		amc.SetLabels(labelsFor(shop))

		if err := unstructured.SetNestedField(amc.Object, map[string]any{
			"receiver":       receiverName,
			"groupBy":        []any{"alertname"},
			"groupWait":      defaultInterval30s,
			"groupInterval":  "5m",
			"repeatInterval": "4h",
			// Required: every Shop's AlertmanagerConfig lives in the same
			// shared namespace, so without this matcher each Shop's route
			// would also catch every other Shop's alerts.
			"matchers": []any{
				map[string]any{keyName: labelShop, "value": shop.Name},
			},
		}, "spec", "route"); err != nil {
			return err
		}

		if err := unstructured.SetNestedField(amc.Object, []any{
			map[string]any{
				keyName: receiverName,
				"discordConfigs": []any{
					map[string]any{
						"apiURL": map[string]any{
							keyName: webhookSecretName,
							"key":   "url",
						},
					},
				},
			},
		}, "spec", "receivers"); err != nil {
			return err
		}

		return controllerutil.SetControllerReference(shop, amc, r.Scheme)
	})
	if err != nil {
		if apimeta.IsNoMatchError(err) {
			log.Info("AlertmanagerConfig CRD not installed; skipping shop alert routing")
			return nil
		}
		return err
	}
	return nil
}
