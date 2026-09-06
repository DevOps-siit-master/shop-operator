package wallet

import "testing"

func TestIsValidAddress(t *testing.T) {
	cases := map[string]bool{
		"0x71C7656EC7ab88b098defB751B7401B5f6d8976F":  true,
		"0x0000000000000000000000000000000000000000":  true,
		"71C7656EC7ab88b098defB751B7401B5f6d8976F":    false, // no 0x prefix
		"0x71C7656EC7ab88b098defB751B7401B5f6d897":    false, // too short
		"0x71C7656EC7ab88b098defB751B7401B5f6d8976FF": false, // too long
		"0xZZC7656EC7ab88b098defB751B7401B5f6d8976F":  false, // not hex
		"": false,
	}

	for input, want := range cases {
		if got := IsValidAddress(input); got != want {
			t.Errorf("IsValidAddress(%q) = %v, want %v", input, got, want)
		}
	}
}
