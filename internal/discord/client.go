package discord

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"
)

var invalidChannelNameChars = regexp.MustCompile(`[^a-z0-9_-]`)

type createChannelRequest struct {
	Name string `json:"name"`
	Type int    `json:"type"`
}

type createChannelResponse struct {
	ID string `json:"id"`
}

type createWebhookRequest struct {
	Name string `json:"name"`
}

type createWebhookResponse struct {
	ID    string `json:"id"`
	URL   string `json:"url"`
	Token string `json:"token"`
}

type Client interface {
	CreateChannel(ctx context.Context, token, serverID, name string) (string, error)
	DeleteChannel(ctx context.Context, token, channelID string) error
	CreateWebhook(ctx context.Context, token, channelID, name string) (id, webhookToken string, err error)
	TransformIntoUrl(id, token string) (url string)
}

type ApiClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

func (client *ApiClient) CreateChannel(ctx context.Context, token, serverID, name string) (string, error) {
	reqBody := createChannelRequest{
		Name: sanitizeDiscordChannelName(name),
		Type: 0,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal req body: %w", err)
	}

	url := fmt.Sprintf("%s/guilds/%s/channels", client.BaseURL, serverID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", fmt.Errorf("failed to build request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bot "+token)
	resp, err := client.HTTPClient.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("failed to create discord channel: %w", err)
	}

	defer func() {
		_ = resp.Body.Close()
	}()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response discord API: %w", err)
	}

	if resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("discord API returned status %d: %s", resp.StatusCode, string(respBytes))
	}

	var createResp createChannelResponse
	if err := json.Unmarshal(respBytes, &createResp); err != nil {
		return "", fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return createResp.ID, nil
}

func (client *ApiClient) CreateWebhook(ctx context.Context, token, channelID, name string) (id, webhookToken string, err error) {
	reqBody := createWebhookRequest{
		Name: name,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return "", "", fmt.Errorf("failed to marshal req body: %w", err)
	}

	urlPath := fmt.Sprintf("%s/channels/%s/webhooks", client.BaseURL, channelID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, urlPath, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", "", fmt.Errorf("failed to build request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bot "+token)
	resp, err := client.HTTPClient.Do(httpReq)
	if err != nil {
		return "", "", fmt.Errorf("failed to create discord channel: %w", err)
	}

	defer func() {
		_ = resp.Body.Close()
	}()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", fmt.Errorf("failed to read response discord API: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("discord API returned status %d: %s", resp.StatusCode, string(respBytes))
	}

	var createResp createWebhookResponse
	if err := json.Unmarshal(respBytes, &createResp); err != nil {
		return "", "", fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return createResp.ID, createResp.Token, nil
}
func (client *ApiClient) TransformIntoUrl(id, token string) (url string) {
	webhookUrl := fmt.Sprintf("https://discord.com/api/webhooks/%s/%s", id, token)

	return webhookUrl
}

func New(discordApiUrl string) Client {
	return &ApiClient{BaseURL: discordApiUrl, HTTPClient: &http.Client{Timeout: 30 * time.Second}}
}

func (client *ApiClient) DeleteChannel(ctx context.Context, token, channelID string) error {
	url := fmt.Sprintf("%s/channels/%s", client.BaseURL, channelID)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("failed to build request: %w", err)
	}

	httpReq.Header.Set("Authorization", "Bot "+token)

	resp, err := client.HTTPClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("failed to delete discord channel: %w", err)
	}

	defer func() {
		_ = resp.Body.Close()
	}()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response discord API: %w", err)
	}

	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent, http.StatusNotFound:
		return nil
	default:
		return fmt.Errorf("discord API returned status %d: %s", resp.StatusCode, string(respBytes))
	}
}

func sanitizeDiscordChannelName(channelName string) string {
	name := strings.ToLower(channelName)
	name = strings.ReplaceAll(name, " ", "-")
	name = invalidChannelNameChars.ReplaceAllString(name, "")

	if len(name) > 100 {
		name = name[:100]
	}

	if name == "" {
		name = "unnamed-channel"
	}

	return name
}
