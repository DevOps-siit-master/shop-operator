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
	"encoding/json"
	"fmt"

	shophubv1 "github.com/DevOps-siit-master/shop-operator/api/v1"
)

const (
	keyType       = "type"
	keyDatasource = "datasource"
	keyTitle      = "title"
	keyGridPos    = "gridPos"
	keyTargets    = "targets"
	keyRefresh    = "refresh"
)

// grafanaDatasourceRef lets Grafana resolve whichever Prometheus datasource is
// provisioned, rather than baking in a specific datasource UID.
var grafanaDatasourceRef = map[string]any{keyType: "prometheus", "uid": "${datasource}"}

// lokiDatasourceRef / tempoDatasourceRef point at the logs and traces
// datasources provisioned on the shared Grafana with fixed uids (see
// kube-state .../kube-prometheus-stack/values.yaml). Unlike the Prometheus
// panels (which resolve via the datasource template variable), the logs and
// traces panels bind these directly by uid.
var (
	lokiDatasourceRef  = map[string]any{keyType: "loki", "uid": "loki"}
	tempoDatasourceRef = map[string]any{keyType: "tempo", "uid": "tempo"}
)

func dashboardTarget(expr, refID, legend string) map[string]any {
	t := map[string]any{keyDatasource: grafanaDatasourceRef, "expr": expr, "refId": refID}
	if legend != "" {
		t["legendFormat"] = legend
	}
	return t
}

// dashboardStat builds a single-row (y=0) stat panel — every stat panel in
// this dashboard lives in the top row.
func dashboardStat(id int, title, expr string, x int, unit string) map[string]any {
	if unit == "" {
		unit = "short"
	}
	return map[string]any{
		"id": id, keyType: "stat", keyTitle: title,
		keyGridPos:    map[string]any{"x": x, "y": 0, "w": 4, "h": 4},
		keyDatasource: grafanaDatasourceRef,
		"fieldConfig": map[string]any{"defaults": map[string]any{"unit": unit}, "overrides": []any{}},
		keyTargets:    []any{dashboardTarget(expr, "A", "")},
	}
}

func dashboardTable(id int, title, expr string, y int) map[string]any {
	target := dashboardTarget(expr, "A", "")
	target["format"] = "table"
	target["instant"] = true
	return map[string]any{
		"id": id, keyType: "table", keyTitle: title,
		keyGridPos:    map[string]any{"x": 0, "y": y, "w": 24, "h": 8},
		keyDatasource: grafanaDatasourceRef,
		keyTargets:    []any{target},
	}
}

type dashboardSeries struct {
	expr   string
	legend string
}

func dashboardTimeseries(id int, title string, series []dashboardSeries, x, y, w int, unit string) map[string]any {
	if unit == "" {
		unit = "short"
	}
	targets := make([]any, len(series))
	for i, s := range series {
		targets[i] = dashboardTarget(s.expr, string(rune('A'+i)), s.legend)
	}
	return map[string]any{
		"id": id, keyType: "timeseries", keyTitle: title,
		keyGridPos:    map[string]any{"x": x, "y": y, "w": w, "h": 8},
		keyDatasource: grafanaDatasourceRef,
		"fieldConfig": map[string]any{"defaults": map[string]any{"unit": unit}, "overrides": []any{}},
		keyTargets:    targets,
	}
}

// dashboardLogs builds a full-width Loki logs panel bound to the loki datasource.
func dashboardLogs(id int, title, expr string, y int) map[string]any {
	return map[string]any{
		"id": id, keyType: "logs", keyTitle: title,
		keyGridPos:    map[string]any{"x": 0, "y": y, "w": 24, "h": 10},
		keyDatasource: lokiDatasourceRef,
		"options": map[string]any{
			"showTime": true, "wrapLogMessage": true, "enableLogDetails": true,
			"dedupStrategy": "none", "sortOrder": "Descending",
		},
		keyTargets: []any{map[string]any{
			keyDatasource: lokiDatasourceRef, "expr": expr,
			"refId": "A", "queryType": "range",
		}},
	}
}

// dashboardTraces builds a full-width table of this Shop's recent traces, bound
// to the tempo datasource and driven by a TraceQL query.
func dashboardTraces(id int, title, query string, y int) map[string]any {
	return map[string]any{
		"id": id, keyType: "table", keyTitle: title,
		keyGridPos:    map[string]any{"x": 0, "y": y, "w": 24, "h": 8},
		keyDatasource: tempoDatasourceRef,
		keyTargets: []any{map[string]any{
			keyDatasource: tempoDatasourceRef, "queryType": "traceql",
			"query": query, "refId": "A", "limit": 20, "tableType": "traces",
		}},
	}
}

func shopMetricsScope(shop *shophubv1.Shop) string {
	return fmt.Sprintf(`namespace="%s", pod=~"^%s-(auth|order|payment|inventory)-.*"`,
		shop.Namespace, resourceName(shop))
}

func buildShopDashboardJSON(shop *shophubv1.Shop) ([]byte, error) {
	scope := shopMetricsScope(shop)

	displayName := shop.Spec.DisplayName
	if displayName == "" {
		displayName = shop.Name
	}

	panels := []any{
		dashboardStat(1, "Total requests (24h)",
			fmt.Sprintf(`sum(increase(http_requests_total{%s}[24h]))`, scope),
			0, ""),
		dashboardStat(2, "Successful (2xx/3xx, 24h)",
			fmt.Sprintf(`sum(increase(http_requests_total{%s, status_code=~"2..|3.."}[24h]))`, scope),
			4, ""),
		dashboardStat(3, "Failed (4xx/5xx, 24h)",
			fmt.Sprintf(`sum(increase(http_requests_total{%s, status_code=~"4..|5.."}[24h]))`, scope),
			8, ""),
		dashboardStat(4, "Unique visitors (24h, order+payment only)",
			fmt.Sprintf(`sum(increase(unique_visitors_total{%s}[24h]))`, scope),
			12, ""),
		dashboardStat(5, "Total traffic (GB, 24h, order+payment only)",
			fmt.Sprintf(`sum(increase(http_response_size_bytes_total{%s}[24h])) / 1024/1024/1024`, scope),
			16, "decgbytes"),
		dashboardStat(6, "Pods running",
			fmt.Sprintf(`count(kube_pod_status_phase{%s, phase="Running"})`, scope),
			20, ""),

		dashboardTable(7, "Top 404 endpoints (24h)",
			fmt.Sprintf(`topk(10, sum by (route) (increase(http_requests_total{%s, status_code="404"}[24h])))`, scope),
			4),

		dashboardTimeseries(8, "Request rate by service", []dashboardSeries{
			{fmt.Sprintf(`sum by (job) (rate(http_requests_total{%s}[5m]))`, scope), "{{job}}"},
		}, 0, 12, 12, "reqps"),
		dashboardTimeseries(9, "Error rate (5xx) by service", []dashboardSeries{
			{fmt.Sprintf(`sum by (job) (rate(http_requests_total{%s, status_code=~"5.."}[5m]))`, scope), "{{job}}"},
		}, 12, 12, 12, "reqps"),

		dashboardTimeseries(10, "CPU usage by pod", []dashboardSeries{
			{fmt.Sprintf(`sum by (pod) (rate(container_cpu_usage_seconds_total{%s, container!="", container!="POD"}[5m]))`, scope), "{{pod}}"},
		}, 0, 20, 12, "short"),
		dashboardTimeseries(11, "Memory usage by pod", []dashboardSeries{
			{fmt.Sprintf(`sum by (pod) (container_memory_working_set_bytes{%s, container!="", container!="POD"})`, scope), "{{pod}}"},
		}, 12, 20, 12, "bytes"),
		dashboardTimeseries(12, "Network I/O by pod", []dashboardSeries{
			{fmt.Sprintf(`sum by (pod) (rate(container_network_receive_bytes_total{%s}[5m]))`, scope), "{{pod}} rx"},
			{fmt.Sprintf(`sum by (pod) (rate(container_network_transmit_bytes_total{%s}[5m]))`, scope), "{{pod}} tx"},
		}, 0, 28, 24, "Bps"),
	}

	// Logs (Loki) and traces (Tempo) for this Shop, on the same dashboard as its
	// metrics (spec 4.1) so all three observability signals live in one place.
	// Scoped to this Shop's pods (logs) and per-Shop service names (traces).
	panels = append(panels,
		dashboardTraces(13, "Recent traces (all services)",
			fmt.Sprintf(`{resource.service.name=~"%s-.*"}`, resourceName(shop)), 36),
		dashboardLogs(14, "Logs (all services)",
			fmt.Sprintf(`{namespace="%s", pod=~"%s-.*"}`, shop.Namespace, resourceName(shop)), 44),
	)

	dashboard := map[string]any{
		"uid":           "shop-" + shop.Name,
		keyTitle:        "Shop: " + displayName,
		"tags":          []any{"shophub", "shop-instance"},
		"timezone":      "browser",
		"schemaVersion": 39,
		"version":       1,
		keyRefresh:      defaultInterval30s,
		"time":          map[string]any{"from": "now-24h", "to": "now"},
		"templating": map[string]any{"list": []any{
			map[string]any{
				keyName: "datasource", "label": "Data source", keyType: "datasource",
				"query": "prometheus", "current": map[string]any{}, "hide": 0,
				"includeAll": false, "multi": false, keyRefresh: 1,
				"options": []any{}, "regex": "",
			},
		}},
		"panels": panels,
	}

	return json.Marshal(dashboard)
}
