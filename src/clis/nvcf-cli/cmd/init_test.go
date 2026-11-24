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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"nvcf-cli/internal/client"
	"nvcf-cli/internal/state"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// routeConfig is every configuration value that decides a key binding.
type routeConfig map[string]string

func baseRouteConfig() routeConfig {
	return routeConfig{
		"base_http_url":     "http://api.test.local:9999",
		"api_host":          "",
		"invoke_url":        "http://invoke.test.local:9999",
		"invoke_host":       "",
		"base_grpc_url":     "grpc.test.local:443",
		"base_nvct_url":     "http://nvct.test.local:9999",
		"nvct_host":         "",
		"event_ledger_url":  "http://events.test.local:9999",
		"event_ledger_host": "",
		"client_id":         "nvcf-default",
	}
}

func (c routeConfig) with(key, value string) routeConfig {
	out := routeConfig{}
	for k, v := range c {
		out[k] = v
	}
	out[key] = value
	return out
}

// applyRouteConfig pins each value in both Viper and the process environment,
// so neither lookup path can pick up an inherited override.
func applyRouteConfig(t *testing.T, c routeConfig) {
	t.Helper()
	for k, v := range c {
		t.Setenv(k, v)
		if v == "" {
			viper.Set(k, nil)
			continue
		}
		viper.Set(k, v)
	}
}

// setupInitTest isolates HOME, Viper, and the state manager, and points every
// API Keys URL at a fake server. The server mints admin tokens and API keys;
// onMint runs while an admin token request is in flight.
func setupInitTest(t *testing.T, onMint func()) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/admin/keys":
			if onMint != nil {
				onMint()
			}
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"value":"new-admin-token"}`))
		case "/v1/keys":
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"value":"nvapi-` + strings.ToLower(r.Header.Get("Key-Issuer-Service")) + `"}`))
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(srv.Close)

	viper.Reset()
	t.Cleanup(func() { viper.Reset() })
	t.Setenv("api_keys_service_url", srv.URL)
	t.Setenv("api_keys_admin_service_url", srv.URL)
	t.Setenv("API_KEYS_SERVICE_ID", "nvcf-test-service-id")
	t.Setenv("api_keys_nvct_service_id", "nvct-test-service-id")
	t.Setenv("API_KEYS_ISSUER_SERVICE", "nvcf-api")
	t.Setenv("api_keys_nvct_issuer_service", "nvct-api")

	orig := state.DefaultStateManager
	state.DefaultStateManager = state.NewStateManager()
	t.Cleanup(func() { state.DefaultStateManager = orig })
}

// bindingsFor returns the function, task, and Event Ledger key bindings that
// api-key generate records under configuration c.
func bindingsFor(t *testing.T, c routeConfig) (string, string, string) {
	t.Helper()
	applyRouteConfig(t, c)
	cfg, err := client.LoadConfigWithoutAuth()
	require.NoError(t, err)
	return functionKeyBinding(cfg), taskKeyBinding(cfg), eventLedgerKeyBinding(cfg)
}

// seedKeys saves an admin token, all three API keys bound to configuration
// generatedFor, and a selected function. It returns the keys' expiration.
func seedKeys(t *testing.T, generatedFor routeConfig, withBindings bool) time.Time {
	t.Helper()
	fn, task, el := bindingsFor(t, generatedFor)
	sm := state.DefaultStateManager
	exp := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	sm.SetTokens("old-admin-token", "function-key", exp, exp)
	sm.SetNVCTAPIKey("task-key", exp)
	sm.SetEventLedgerAPIKey("event-ledger-key", exp)
	if withBindings {
		sm.SetAPIKeyBindings(fn, task, el)
	}
	sm.SetFunction("fn-id", "ver-id", "fn")
	require.NoError(t, sm.Save())
	return exp
}

func generateAllKeys(t *testing.T) {
	t.Helper()
	resetApikeyFlags(t)
	apikeyFor = ""
	apikeyExpiresIn = "1h"
	require.NoError(t, runAPIKeyGenerate(nil, nil))
}

func TestInit_KeepsKeysGeneratedForTheSameRoute(t *testing.T) {
	setupInitTest(t, nil)
	applyRouteConfig(t, baseRouteConfig())
	require.NoError(t, runInit(nil, nil))
	generateAllKeys(t)
	before := *state.DefaultStateManager.GetState()
	require.NotEmpty(t, before.APIKey)
	require.NotEmpty(t, before.NVCTAPIKey)
	require.NotEmpty(t, before.EventLedgerAPIKey)

	require.NoError(t, runInit(nil, nil))

	got := state.DefaultStateManager.GetState()
	assert.Equal(t, "new-admin-token", got.Token)
	assert.Equal(t, before.APIKey, got.APIKey)
	assert.Equal(t, before.NVCTAPIKey, got.NVCTAPIKey)
	assert.Equal(t, before.EventLedgerAPIKey, got.EventLedgerAPIKey)
	assert.True(t, before.APIKeyExpiration.Equal(got.APIKeyExpiration))
	assert.True(t, before.NVCTAPIKeyExpiration.Equal(got.NVCTAPIKeyExpiration))
	assert.True(t, before.EventLedgerAPIKeyExpiration.Equal(got.EventLedgerAPIKeyExpiration))
}

// A key generated against deployment B must not be restored when init runs
// against deployment A, even though init last recorded A.
func TestInit_DropsKeysGeneratedForAnotherDeployment(t *testing.T) {
	setupInitTest(t, nil)
	a := baseRouteConfig()
	b := a.with("base_http_url", "http://api.b.test.local:9999").
		with("base_nvct_url", "http://nvct.b.test.local:9999").
		with("event_ledger_url", "http://events.b.test.local:9999")

	applyRouteConfig(t, a)
	require.NoError(t, runInit(nil, nil))
	applyRouteConfig(t, b)
	generateAllKeys(t)
	applyRouteConfig(t, a)

	require.NoError(t, runInit(nil, nil))

	got := state.DefaultStateManager.GetState()
	assert.Empty(t, got.APIKey)
	assert.Empty(t, got.NVCTAPIKey)
	assert.Empty(t, got.EventLedgerAPIKey)
}

func TestInit_KeepsSeededKeysWithUnchangedExpiration(t *testing.T) {
	setupInitTest(t, nil)
	exp := seedKeys(t, baseRouteConfig(), true)
	applyRouteConfig(t, baseRouteConfig())

	require.NoError(t, runInit(nil, nil))

	got := state.DefaultStateManager.GetState()
	assert.Equal(t, "function-key", got.APIKey)
	assert.Equal(t, "task-key", got.NVCTAPIKey)
	assert.Equal(t, "event-ledger-key", got.EventLedgerAPIKey)
	assert.True(t, exp.Equal(got.APIKeyExpiration))
	assert.True(t, exp.Equal(got.NVCTAPIKeyExpiration))
	assert.True(t, exp.Equal(got.EventLedgerAPIKeyExpiration))
	assert.Empty(t, got.FunctionID, "function context is still reset")
}

// Each case changes one value between generation and init. Only the keys
// whose route uses that value are dropped.
func TestInit_DropsOnlyKeysWhoseRouteChanged(t *testing.T) {
	cases := []struct {
		key, value                     string
		wantFunction, wantTask, wantEL bool
	}{
		{"base_http_url", "http://api.other.test.local:9999", false, true, true},
		{"api_host", "api.other.test", false, true, true},
		{"invoke_url", "http://invoke.other.test.local:9999", false, true, true},
		{"invoke_host", "invoke.other.test", false, true, true},
		{"base_grpc_url", "grpc.other.test.local:443", false, true, true},
		{"base_nvct_url", "http://nvct.other.test.local:9999", true, false, true},
		{"nvct_host", "tasks.other.test", true, false, true},
		{"event_ledger_url", "http://events.other.test.local:9999", true, true, false},
		{"event_ledger_host", "events.other.test", true, true, false},
		{"client_id", "other-account", false, false, false},
		// Clients trim trailing slashes, so these reach the same route.
		{"base_nvct_url", "http://nvct.test.local:9999/", true, true, true},
		{"event_ledger_url", "http://events.test.local:9999/", true, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			setupInitTest(t, nil)
			seedKeys(t, baseRouteConfig().with(tc.key, tc.value), true)
			applyRouteConfig(t, baseRouteConfig())

			require.NoError(t, runInit(nil, nil))

			got := state.DefaultStateManager.GetState()
			assert.Equal(t, "new-admin-token", got.Token)
			assertKey(t, tc.wantFunction, "function-key", got.APIKey, got.APIKeyExpiration)
			assertKey(t, tc.wantTask, "task-key", got.NVCTAPIKey, got.NVCTAPIKeyExpiration)
			assertKey(t, tc.wantEL, "event-ledger-key", got.EventLedgerAPIKey, got.EventLedgerAPIKeyExpiration)
		})
	}
}

// When only the task route changes, init keeps the function key and drops the
// task key. The next task command must not send the kept function key, or any
// other credential, to the new task route.
func TestInit_TaskCommandDoesNotSendFunctionKeyToChangedTaskRoute(t *testing.T) {
	setupInitTest(t, nil)
	var gotAuth []string
	taskRoute := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = append(gotAuth, r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"tasks":[]}`))
	}))
	t.Cleanup(taskRoute.Close)
	seedKeys(t, baseRouteConfig(), true)
	applyRouteConfig(t, baseRouteConfig().with("base_nvct_url", taskRoute.URL))

	require.NoError(t, runInit(nil, nil))
	got := state.DefaultStateManager.GetState()
	require.Equal(t, "function-key", got.APIKey)
	require.Empty(t, got.NVCTAPIKey)

	err := runTaskList(taskListCmd, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "api-key generate --for task")
	assert.Empty(t, gotAuth, "the changed task route received a request")
}

// Keys saved by a release that did not record bindings are dropped once.
func TestInit_DropsKeysWithoutRecordedBinding(t *testing.T) {
	setupInitTest(t, nil)
	seedKeys(t, baseRouteConfig(), false)
	applyRouteConfig(t, baseRouteConfig())

	require.NoError(t, runInit(nil, nil))

	got := state.DefaultStateManager.GetState()
	assert.Empty(t, got.APIKey)
	assert.Empty(t, got.NVCTAPIKey)
	assert.Empty(t, got.EventLedgerAPIKey)
}

// An api-key clear that saves while init waits for the admin token must not
// be undone by init's final save.
func TestInit_DoesNotRestoreKeysClearedDuringTokenRequest(t *testing.T) {
	setupInitTest(t, func() {
		other := state.NewStateManager()
		if err := other.Load(); err != nil {
			t.Errorf("load state in concurrent writer: %v", err)
			return
		}
		other.ClearTokens()
		if err := other.Save(); err != nil {
			t.Errorf("save state in concurrent writer: %v", err)
		}
	})
	seedKeys(t, baseRouteConfig(), true)
	applyRouteConfig(t, baseRouteConfig())

	require.NoError(t, runInit(nil, nil))

	got := state.DefaultStateManager.GetState()
	assert.Equal(t, "new-admin-token", got.Token)
	assert.Empty(t, got.APIKey)
	assert.Empty(t, got.NVCTAPIKey)
	assert.Empty(t, got.EventLedgerAPIKey)
}

// If the state file is corrupted during the token request, init must not
// restore the keys it read earlier. It still saves the new admin token.
func TestInit_DropsKeysWhenStateFileIsCorruptedDuringTokenRequest(t *testing.T) {
	statePath := ""
	setupInitTest(t, func() {
		if err := os.WriteFile(statePath, []byte("{not json"), 0o600); err != nil {
			t.Errorf("corrupt state file: %v", err)
		}
	})
	statePath = filepath.Join(os.Getenv("HOME"), ".nvcf-cli.state")
	seedKeys(t, baseRouteConfig(), true)
	applyRouteConfig(t, baseRouteConfig())

	require.NoError(t, runInit(nil, nil))

	saved := state.NewStateManager()
	require.NoError(t, saved.Load(), "init must leave a readable state file")
	got := saved.GetState()
	assert.Equal(t, "new-admin-token", got.Token)
	assert.Empty(t, got.APIKey)
	assert.Empty(t, got.NVCTAPIKey)
	assert.Empty(t, got.EventLedgerAPIKey)
}

// A state file that is already corrupt must not block init, which is how
// an operator recovers from it.
func TestInit_RepairsCorruptStateFile(t *testing.T) {
	setupInitTest(t, nil)
	statePath := filepath.Join(os.Getenv("HOME"), ".nvcf-cli.state")
	require.NoError(t, os.WriteFile(statePath, []byte("{not json"), 0o600))
	applyRouteConfig(t, baseRouteConfig())

	require.NoError(t, runInit(nil, nil))

	saved := state.NewStateManager()
	require.NoError(t, saved.Load())
	assert.Equal(t, "new-admin-token", saved.GetState().Token)
}

// A state path that exists but cannot be read is not a corrupt file: init must
// stop before it changes anything. A directory stands in for the unreadable
// file because CI runs tests as root, which ignores file permission bits.
func TestInit_StopsWhenStateFileCannotBeRead(t *testing.T) {
	statePath := ""
	setupInitTest(t, func() {
		if err := os.Remove(statePath); err != nil {
			t.Errorf("remove state file: %v", err)
			return
		}
		if err := os.MkdirAll(filepath.Join(statePath, "keep"), 0o700); err != nil {
			t.Errorf("replace state file with a directory: %v", err)
		}
	})
	statePath = filepath.Join(os.Getenv("HOME"), ".nvcf-cli.state")
	seedKeys(t, baseRouteConfig(), true)
	applyRouteConfig(t, baseRouteConfig())

	err := runInit(nil, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to reload state")
	info, statErr := os.Stat(filepath.Join(statePath, "keep"))
	require.NoError(t, statErr, "init must not replace the state path")
	assert.True(t, info.IsDir())
}

// A directory where Save writes its temporary file makes the save fail, even
// for root.
func TestInit_ReturnsErrorWhenSaveFails(t *testing.T) {
	setupInitTest(t, func() {
		tempPath := filepath.Join(os.Getenv("HOME"), ".nvcf-cli.state.tmp")
		if err := os.MkdirAll(filepath.Join(tempPath, "keep"), 0o700); err != nil {
			t.Errorf("block the temporary state file: %v", err)
		}
	})
	applyRouteConfig(t, baseRouteConfig())

	err := runInit(nil, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to save state")
}

func TestRefresh_ReplacesOnlyTheAdminToken(t *testing.T) {
	setupInitTest(t, nil)
	exp := seedKeys(t, baseRouteConfig(), true)
	before := *state.DefaultStateManager.GetState()
	applyRouteConfig(t, baseRouteConfig())

	require.NoError(t, runRefresh(nil, nil))

	saved := state.NewStateManager()
	require.NoError(t, saved.Load())
	got := saved.GetState()
	assert.Equal(t, "new-admin-token", got.Token)
	assert.Equal(t, "function-key", got.APIKey)
	assert.True(t, exp.Equal(got.APIKeyExpiration))
	assert.Equal(t, before.APIKeyBinding, got.APIKeyBinding)
	assert.Equal(t, "task-key", got.NVCTAPIKey)
	assert.True(t, exp.Equal(got.NVCTAPIKeyExpiration))
	assert.Equal(t, before.NVCTAPIKeyBinding, got.NVCTAPIKeyBinding)
	assert.Equal(t, "event-ledger-key", got.EventLedgerAPIKey)
	assert.True(t, exp.Equal(got.EventLedgerAPIKeyExpiration))
	assert.Equal(t, before.EventLedgerAPIKeyBinding, got.EventLedgerAPIKeyBinding)
}

// An api-key clear that saves while refresh waits for the admin token must not
// be undone by refresh's final save.
func TestRefresh_DoesNotRestoreKeysClearedDuringTokenRequest(t *testing.T) {
	setupInitTest(t, func() {
		other := state.NewStateManager()
		if err := other.Load(); err != nil {
			t.Errorf("load state in concurrent writer: %v", err)
			return
		}
		other.ClearNVCTAPIKey()
		other.SetTokens(other.GetState().Token, "", other.GetState().TokenExpiration, time.Time{})
		if err := other.Save(); err != nil {
			t.Errorf("save state in concurrent writer: %v", err)
		}
	})
	seedKeys(t, baseRouteConfig(), true)
	applyRouteConfig(t, baseRouteConfig())

	require.NoError(t, runRefresh(nil, nil))

	saved := state.NewStateManager()
	require.NoError(t, saved.Load())
	assert.Equal(t, "new-admin-token", saved.GetState().Token)
	assert.Empty(t, saved.GetState().APIKey)
	assert.Empty(t, saved.GetState().NVCTAPIKey)
}

func TestRefresh_FailsWithoutSavingWhenReloadFails(t *testing.T) {
	statePath := ""
	setupInitTest(t, func() {
		if err := os.WriteFile(statePath, []byte("{not json"), 0o600); err != nil {
			t.Errorf("corrupt state file: %v", err)
		}
	})
	statePath = filepath.Join(os.Getenv("HOME"), ".nvcf-cli.state")
	seedKeys(t, baseRouteConfig(), true)
	applyRouteConfig(t, baseRouteConfig())

	err := runRefresh(nil, nil)

	require.Error(t, err)
	data, readErr := os.ReadFile(statePath)
	require.NoError(t, readErr)
	assert.Equal(t, "{not json", string(data), "refresh must not save after a failed reload")
}

func assertKey(t *testing.T, keep bool, want, got string, exp time.Time) {
	t.Helper()
	if keep {
		assert.Equal(t, want, got)
		assert.False(t, exp.IsZero())
		return
	}
	assert.Empty(t, got)
	assert.True(t, exp.IsZero(), "expiration must be cleared with the key")
}
