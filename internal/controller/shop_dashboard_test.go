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
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	shophubv1 "github.com/DevOps-siit-master/shop-operator/api/v1"
)

// TestBuildShopDashboardJSON checks that a Shop's dashboard is valid JSON and
// carries all three observability signals — metrics (Prometheus), logs (Loki)
// and traces (Tempo) — each scoped to that Shop.
func TestBuildShopDashboardJSON(t *testing.T) {
	shop := &shophubv1.Shop{
		ObjectMeta: metav1.ObjectMeta{Name: "acme", Namespace: "shophub-shops"},
	}

	body, err := buildShopDashboardJSON(shop)
	if err != nil {
		t.Fatalf("buildShopDashboardJSON: %v", err)
	}

	var dash struct {
		UID    string `json:"uid"`
		Panels []struct {
			Type       string `json:"type"`
			Datasource struct {
				Type string `json:"type"`
				UID  string `json:"uid"`
			} `json:"datasource"`
		} `json:"panels"`
	}
	if err := json.Unmarshal(body, &dash); err != nil {
		t.Fatalf("dashboard is not valid JSON: %v", err)
	}

	if dash.UID != "shop-acme" {
		t.Errorf("uid = %q, want shop-acme", dash.UID)
	}

	var hasMetrics, hasLogs, hasTraces bool
	for _, p := range dash.Panels {
		switch {
		case p.Datasource.Type == "prometheus":
			hasMetrics = true
		case p.Type == "logs" && p.Datasource.UID == "loki":
			hasLogs = true
		case p.Type == "table" && p.Datasource.UID == "tempo":
			hasTraces = true
		}
	}
	if !hasMetrics {
		t.Error("dashboard missing a Prometheus metrics panel")
	}
	if !hasLogs {
		t.Error("dashboard missing a Loki logs panel")
	}
	if !hasTraces {
		t.Error("dashboard missing a Tempo traces panel")
	}

	// Logs/traces queries must be scoped to this Shop's resources.
	if s := string(body); !strings.Contains(s, "shop-acme-") {
		t.Error("logs/traces panels are not scoped to the shop")
	}
}
