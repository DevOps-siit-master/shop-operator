package discord

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newTestClient(t *testing.T, handler http.HandlerFunc) (*ApiClient, func()) {
	t.Helper()
	server := httptest.NewServer(handler)
	client := &ApiClient{
		BaseURL:    server.URL,
		HTTPClient: &http.Client{Timeout: 5 * time.Second},
	}
	return client, server.Close
}

func TestCreateChannel_Success(t *testing.T) {
	client, cleanup := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		// Assert the request is shaped correctly.
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/guilds/guild-123/channels" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bot fake-token" {
			t.Errorf("unexpected Authorization header: %s", got)
		}

		var body createChannelRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("failed to decode request body: %v", err)
		}
		if body.Name != "test-channel" {
			t.Errorf("unexpected channel name: %s", body.Name)
		}
		if body.Type != 0 {
			t.Errorf("expected type 0 (text channel), got %d", body.Type)
		}

		// Discord's real endpoint returns 201 on success.
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(createChannelResponse{ID: "channel-abc"})
	})
	defer cleanup()

	id, err := client.CreateChannel(context.Background(), "fake-token", "guild-123", "test-channel")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != "channel-abc" {
		t.Errorf("expected id 'channel-abc', got %q", id)
	}
}

func TestCreateChannel_Failure(t *testing.T) {
	client, cleanup := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"message": "Missing Permissions", "code": 50013}`))
	})
	defer cleanup()

	id, err := client.CreateChannel(context.Background(), "fake-token", "guild-123", "test-channel")
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if id != "" {
		t.Errorf("expected empty id on failure, got %q", id)
	}
	if !strings.Contains(err.Error(), "50013") {
		t.Errorf("expected error to include discord error code, got: %v", err)
	}
}

func TestCreateWebhook_Success(t *testing.T) {
	client, cleanup := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/channels/channel-abc/webhooks" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}

		var body createWebhookRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("failed to decode request body: %v", err)
		}
		if body.Name != "test-channel" {
			t.Errorf("unexpected webhook name: %s", body.Name)
		}

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(createWebhookResponse{
			ID:    "webhook-xyz",
			Token: "webhook-token-xyz",
		})
	})
	defer cleanup()

	id, token, err := client.CreateWebhook(context.Background(), "fake-token", "channel-abc", "test-channel")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != "webhook-xyz" {
		t.Errorf("expected id 'webhook-xyz', got %q", id)
	}
	if token != "webhook-token-xyz" {
		t.Errorf("expected token 'webhook-token-xyz', got %q", token)
	}
}

func TestCreateWebhook_Failure(t *testing.T) {
	client, cleanup := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"message": "Maximum number of webhooks reached (15)", "code": 30007}`))
	})
	defer cleanup()

	id, token, err := client.CreateWebhook(context.Background(), "fake-token", "channel-abc", "test-channel")
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if id != "" || token != "" {
		t.Errorf("expected empty id/token on failure, got id=%q token=%q", id, token)
	}
	if !strings.Contains(err.Error(), "30007") {
		t.Errorf("expected error to include discord error code, got: %v", err)
	}
}

func TestDeleteChannel_Success(t *testing.T) {
	client, cleanup := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		if r.URL.Path != "/channels/channel-abc" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id": "channel-abc"}`))
	})
	defer cleanup()

	err := client.DeleteChannel(context.Background(), "fake-token", "channel-abc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDeleteChannel_NoContent(t *testing.T) {
	// Some REST APIs return 204 with an empty body for deletes — worth
	// confirming this code path is also treated as success.
	client, cleanup := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	defer cleanup()

	err := client.DeleteChannel(context.Background(), "fake-token", "channel-abc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TransformIntoUrl has no HTTP call at all, so it's a plain table-driven
// test rather than needing httptest.
func TestTransformIntoUrl(t *testing.T) {
	client := &ApiClient{BaseURL: "https://discord.com/api/v10"}

	got := client.TransformIntoUrl("webhook-id", "webhook-token")
	want := "https://discord.com/api/webhooks/webhook-id/webhook-token"

	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// This test exists specifically because BaseURL previously leaked into
// TransformIntoUrl's output — confirms the webhook URL is always built
// against discord.com regardless of what BaseURL is set to (e.g. a test
// server URL), since webhook execute URLs are never versioned/relative.
func TestTransformIntoUrl_IgnoresCustomBaseURL(t *testing.T) {
	client := &ApiClient{BaseURL: "http://127.0.0.1:12345"}

	got := client.TransformIntoUrl("webhook-id", "webhook-token")
	want := "https://discord.com/api/webhooks/webhook-id/webhook-token"

	if got != want {
		t.Errorf("got %q, want %q — BaseURL should never affect webhook URL construction", got, want)
	}
}
