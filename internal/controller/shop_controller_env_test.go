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
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestShopAppPullPolicy(t *testing.T) {
	tests := []struct {
		name string
		env  string
		want corev1.PullPolicy
	}{
		{name: "unset falls back to default", env: "", want: defaultShopAppPullPolicy},
		{name: "Always is honored", env: "Always", want: corev1.PullAlways},
		{name: "IfNotPresent is honored", env: "IfNotPresent", want: corev1.PullIfNotPresent},
		{name: "Never is honored", env: "Never", want: corev1.PullNever},
		{name: "invalid value falls back to default", env: "sometimes", want: defaultShopAppPullPolicy},
		{name: "wrong case is not honored", env: "always", want: defaultShopAppPullPolicy},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.env != "" {
				t.Setenv("SHOP_APP_PULL_POLICY", tt.env)
			}
			if got := shopAppPullPolicy(); got != tt.want {
				t.Errorf("shopAppPullPolicy() with SHOP_APP_PULL_POLICY=%q = %q, want %q", tt.env, got, tt.want)
			}
		})
	}
}
