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

// Package wallet holds the rules for the payout accounts a Wallet custom resource represents.
package wallet

import "regexp"

// addressPattern matches an EVM address: 0x followed by 20 hex-encoded bytes.
var addressPattern = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)

// IsValidAddress reports whether s is a syntactically valid EVM address. The
// payout address is typed in by hand in the ShopHub panel, so a typo would
// otherwise reach the payment service unnoticed and silently reject every
// payment: no Transfer can ever match an address nobody owns.
func IsValidAddress(s string) bool {
	return addressPattern.MatchString(s)
}
