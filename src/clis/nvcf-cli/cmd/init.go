/*
SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0

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

package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"nvcf-cli/internal/client"
	"nvcf-cli/internal/logging"
	"nvcf-cli/internal/state"

	"github.com/spf13/cobra"
)

// initCmd represents the init command
var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Generate admin token for NVCF API",
	Long: `Initialize NVCF CLI by generating a fresh admin JWT token from the API Keys service.

This command:
- Calls the API Keys service endpoint to generate a new JWT admin token
- Clears any existing function state to start fresh
- Saves the token for subsequent commands
- Keeps each API key created by "api-key generate" while the account and
  the route the key is sent to (its URLs and Host overrides) are unchanged.
  A key whose route changed, or one created by an older release that did
  not record its route, is dropped.
- To remove saved keys on purpose, run "api-key clear". That only edits the
  local state. Do not use "api-key clear-all" for this: it revokes every key
  for the owner on the server.

The JWT token is used for admin operations such as creating, deploying, and managing functions.

Examples:
  # Initialize with default settings
  nvcf-cli init

  # Initialize with custom API Keys service URL (used for both key ops and admin token route)
  API_KEYS_SERVICE_URL=https://api-keys.shqa.stg.nvcf.nvidia.com nvcf-cli init

  # Initialize with dedicated admin token service URL
  API_KEYS_ADMIN_SERVICE_URL=https://api-keys-admin.shqa.stg.nvcf.nvidia.com nvcf-cli init

  # Initialize with debug output
  nvcf-cli --debug init`,
	RunE: runInit,
}

func init() {
	rootCmd.AddCommand(initCmd)
}

func runInit(cmd *cobra.Command, args []string) error {
	// Load current state
	if err := LoadStateForCurrentCommand(); err != nil {
		logging.Warning("Could not load existing state: %v", err)
	}

	// Load configuration without requiring existing authentication
	config, err := client.LoadConfigWithoutAuth()
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	logging.Info("Starting fresh session...")
	logging.Info("Generating admin token from API Keys service...")

	// Generate admin token via API endpoint
	token, expiration, err := generateAdminTokenViaAPI(config)
	if err != nil {
		return fmt.Errorf("failed to generate admin token: %w", err)
	}

	if token == "" {
		return fmt.Errorf("failed to extract admin token from API response")
	}

	// Re-read state after the token request so a change another command made
	// meanwhile, such as api-key clear, is not overwritten. Then start fresh,
	// but keep each API key whose recorded route still matches: the keys are
	// independent of the admin token, and dropping them makes later function
	// and task commands fall back to the admin token and fail authorization.
	// If the file is corrupt, keep no keys rather than restore an older copy,
	// and still save the new token so init can repair it. Any other read error
	// stops init before it changes the file.
	sm := GetStateManagerForCurrentCommand()
	prev := state.State{}
	if err := sm.Load(); err != nil {
		if !errors.Is(err, state.ErrInvalidStateFile) {
			return fmt.Errorf("failed to reload state: %w", err)
		}
		logging.Warning("State file is corrupt, saved API keys are dropped: %v", err)
	} else {
		prev = *sm.GetState()
	}
	keepFunctionKey := keepKey(prev.APIKey, prev.APIKeyBinding, functionKeyBinding(config))
	keepTaskKey := keepKey(prev.NVCTAPIKey, prev.NVCTAPIKeyBinding, taskKeyBinding(config))
	keepEventLedgerKey := keepKey(prev.EventLedgerAPIKey, prev.EventLedgerAPIKeyBinding, eventLedgerKeyBinding(config))
	sm.ClearFunction()
	sm.ClearTokens()

	// Save the new token
	if keepFunctionKey {
		sm.SetTokens(token, prev.APIKey, expiration, prev.APIKeyExpiration)
		sm.SetAPIKeyBindings(prev.APIKeyBinding, "", "")
	} else {
		sm.SetTokens(token, "", expiration, time.Time{})
	}
	if keepTaskKey {
		sm.SetNVCTAPIKey(prev.NVCTAPIKey, prev.NVCTAPIKeyExpiration)
		sm.SetAPIKeyBindings("", prev.NVCTAPIKeyBinding, "")
	}
	if keepEventLedgerKey {
		sm.SetEventLedgerAPIKey(prev.EventLedgerAPIKey, prev.EventLedgerAPIKeyExpiration)
		sm.SetAPIKeyBindings("", "", prev.EventLedgerAPIKeyBinding)
	}
	sm.SetConfig("", "", false) // No cluster mode needed anymore

	// Set endpoints - use configured account or default
	account := getConfigValueWithDefault("client_id", "nvcf-default")
	sm.SetEndpoints(config.BaseHTTPURL, config.BaseInvokeURL, account)

	if err := SaveStateForCurrentCommand(); err != nil {
		return fmt.Errorf("failed to save state: %w", err)
	}

	logging.Success("Admin token generated and saved")
	logging.Plain("Token: %s", token)
	if !expiration.IsZero() {
		logging.Plain("Expires: %s", expiration.Format("2006-01-02 15:04:05"))
	}

	return nil
}

// keepKey reports whether a saved key was generated for the current route.
// A key without a recorded binding predates route tracking and is dropped.
func keepKey(key, saved, current string) bool {
	return key != "" && saved != "" && saved == current
}

// TokenResponse represents the API response from the token generation endpoint
type TokenResponse struct {
	ID          string `json:"id"`
	Value       string `json:"value"` // The actual JWT token
	Status      string `json:"status"`
	Description string `json:"description"`
	CreatedAt   string `json:"created_at"`
	ExpiresAt   string `json:"expires_at"` // Timestamp in RFC3339 format
}

// generateAdminTokenViaAPI generates a new admin token via direct API call
func generateAdminTokenViaAPI(config *client.Config) (string, time.Time, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Resolve admin token route base URL with fallback to API keys service URL.
	// Priority:
	// 1) API_KEYS_ADMIN_SERVICE_URL / api_keys_admin_service_url
	// 2) API_KEYS_SERVICE_URL / api_keys_service_url
	// 3) built-in default
	adminAPIURL := getConfigValueWithDefault(
		"api_keys_admin_service_url",
		getConfigValueWithDefault("api_keys_service_url", "https://api-keys.nvcf.nvidia.com"),
	)
	endpoint := adminAPIURL + "/v1/admin/keys"

	if config.Debug {
		logging.Debug("Calling token generation endpoint: %s", endpoint)
	}

	// Create request
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader([]byte("{}")))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	// Set Host header override for hostname-based routing (self-hosted deployments)
	apiKeysHost := getConfigValueWithDefault("api_keys_host", "")
	if apiKeysHost != "" {
		req.Host = apiKeysHost
		if config.Debug {
			logging.Debug("Using Host header override: %s", apiKeysHost)
		}
	}

	// Make the request
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	if config.Debug {
		logging.Debug("Sending POST request to generate admin token...")
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to call API: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to read response: %w", err)
	}

	if config.Debug {
		logging.Debug("Response status: %d", resp.StatusCode)
		logging.Debug("Response body: %s", string(body))
	}

	// Check status code
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", time.Time{}, fmt.Errorf("API error %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var tokenResp TokenResponse
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		// Try parsing as plain text token
		token := string(body)
		if token != "" {
			return token, time.Time{}, nil
		}
		return "", time.Time{}, fmt.Errorf("failed to parse response: %w", err)
	}

	// Extract token from response
	if tokenResp.Value == "" {
		return "", time.Time{}, fmt.Errorf("token value is empty in response")
	}

	// Parse expiration time if provided
	var expiration time.Time
	if tokenResp.ExpiresAt != "" {
		parsedTime, err := time.Parse(time.RFC3339, tokenResp.ExpiresAt)
		if err != nil {
			if config.Debug {
				logging.Debug("Failed to parse expiration time '%s': %v", tokenResp.ExpiresAt, err)
			}
			// Continue without expiration if parsing fails
		} else {
			expiration = parsedTime
		}
	}

	return tokenResp.Value, expiration, nil
}
