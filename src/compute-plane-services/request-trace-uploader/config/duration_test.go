// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"strings"
	"testing"
	"time"
)

// Every time setting is a Go duration with a unit, such as "30s" or "1h".
// A bare number, a negative value, zero where zero is not allowed, or a value
// out of range stops strict loading with an error that names the setting.
func TestDurationSettings(t *testing.T) {
	tests := []struct {
		name  string
		get   func(Config) time.Duration
		valid map[string]time.Duration
		bad   []string
	}{
		{
			name:  EnvScanInterval,
			get:   func(c Config) time.Duration { return c.ScanInterval },
			valid: map[string]time.Duration{"45s": 45 * time.Second, "2m": 2 * time.Minute, " 1h ": time.Hour},
			bad:   []string{"30", "0", "0s", "-5s", "soon", "500ms", "25h"},
		},
		{
			name:  EnvDrainTimeout,
			get:   func(c Config) time.Duration { return c.DrainTimeout },
			valid: map[string]time.Duration{"15s": 15 * time.Second, "0s": 0, "1h": time.Hour},
			bad:   []string{"15", "0", "-1s", "soon", "61m"},
		},
		{
			name:  EnvMaxJobAge,
			get:   func(c Config) time.Duration { return c.MaxJobAge },
			valid: map[string]time.Duration{"24h": 24 * time.Hour, "1m": time.Minute, "90m": 90 * time.Minute},
			bad:   []string{"86400", "0", "0s", "-6h", "soon", "59s", "169h"},
		},
		{
			name:  EnvStatusInterval,
			get:   func(c Config) time.Duration { return c.StatusInterval },
			valid: map[string]time.Duration{"10s": 10 * time.Second, "1s": time.Second, "1h": time.Hour},
			bad:   []string{"10", "0", "0s", "-10s", "soon", "500ms", "61m"},
		},
		{
			name:  EnvQuarantineResume,
			get:   func(c Config) time.Duration { return c.QuarantineResume },
			valid: map[string]time.Duration{"1m": time.Minute, "2h": 2 * time.Hour, "24h": 24 * time.Hour},
			bad:   []string{"60", "0", "0s", "-1h", "soon", "59s", "25h"},
		},
		{
			name:  EnvQuarantineMaxAge,
			get:   func(c Config) time.Duration { return c.Spool.QuarantineMaxAge },
			valid: map[string]time.Duration{"168h": 7 * 24 * time.Hour, "1m": time.Minute, "720h": 30 * 24 * time.Hour},
			bad:   []string{"604800", "0", "0s", "-168h", "soon", "59s", "721h"},
		},
		{
			name:  EnvSpoolCheckInterval,
			get:   func(c Config) time.Duration { return c.Spool.CheckInterval },
			valid: map[string]time.Duration{"10s": 10 * time.Second, "1s": time.Second, "1h": time.Hour},
			bad:   []string{"10", "0", "0s", "-10s", "soon", "500ms", "61m"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !strings.HasPrefix(tt.name, "REQUEST_TRACE_UPLOADER_") || strings.HasSuffix(tt.name, "_SECONDS") {
				t.Fatalf("setting name %q, want a duration name without a unit suffix", tt.name)
			}
			for value, want := range tt.valid {
				cfg, err := LoadStrict(mapLookup(requiredEnv(map[string]string{tt.name: value})), nil)
				if err != nil {
					t.Fatalf("%s=%q: LoadStrict() error = %v", tt.name, value, err)
				}
				if got := tt.get(cfg); got != want {
					t.Fatalf("%s=%q = %v, want %v", tt.name, value, got, want)
				}
			}
			for _, value := range tt.bad {
				_, err := LoadStrict(mapLookup(requiredEnv(map[string]string{tt.name: value})), nil)
				if err == nil || !strings.Contains(err.Error(), tt.name) {
					t.Errorf("%s=%q: LoadStrict() error = %v, want an error naming the setting", tt.name, value, err)
				}
			}
		})
	}
}

// The old names that counted seconds are not read under another meaning: a
// deployment that still sets one fails strict loading as an unknown name.
func TestOldSecondsNamesStopStartup(t *testing.T) {
	for _, old := range []string{
		"REQUEST_TRACE_UPLOADER_SCAN_INTERVAL_SECONDS",
		"REQUEST_TRACE_UPLOADER_DRAIN_TIMEOUT_SECONDS",
		"REQUEST_TRACE_UPLOADER_MAX_JOB_AGE_SECONDS",
		"REQUEST_TRACE_UPLOADER_STATUS_INTERVAL_SECONDS",
		"REQUEST_TRACE_UPLOADER_QUARANTINE_RESUME_SECONDS",
	} {
		_, err := LoadStrict(mapLookup(requiredEnv(map[string]string{old: "30"})), []string{old + "=30"})
		if err == nil || !strings.Contains(err.Error(), old) {
			t.Errorf("%s: LoadStrict() error = %v, want it rejected by name", old, err)
		}
	}
}
