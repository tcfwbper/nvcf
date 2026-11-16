// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadDefaultsForSpoolSettings(t *testing.T) {
	cfg, warnings, err := Load(mapLookup(requiredEnv(nil)))
	if err != nil || len(warnings) != 0 {
		t.Fatalf("Load() error = %v warnings = %v", err, warnings)
	}
	// Deletion is a near-full backstop behind an earlier alert: it starts at
	// 85% and frees down to 75%.
	want := SpoolPolicy{
		HighWatermark:         0.85,
		LowWatermark:          0.75,
		Eviction:              EvictQuarantineThenOldest,
		MinFreeBytes:          512 << 20,
		QuarantineMaxAge:      7 * 24 * time.Hour,
		QuarantineMaxFraction: 0.2,
		CheckInterval:         10 * time.Second,
	}
	if cfg.Spool != want {
		t.Fatalf("Spool = %+v, want %+v", cfg.Spool, want)
	}
}

func TestLoadParsesSpoolSettings(t *testing.T) {
	cfg, warnings, err := Load(mapLookup(requiredEnv(map[string]string{
		EnvSpoolHighWatermark:    "0.95",
		EnvSpoolLowWatermark:     " 0.5 ",
		EnvSpoolEvictionPolicy:   "oldest",
		EnvSpoolMinFreeBytes:     "0",
		EnvQuarantineMaxAge:      "1h",
		EnvQuarantineMaxFraction: "1",
		EnvSpoolCheckInterval:    "1s",
	})))
	if err != nil || len(warnings) != 0 {
		t.Fatalf("Load() error = %v warnings = %v", err, warnings)
	}
	want := SpoolPolicy{
		HighWatermark:         0.95,
		LowWatermark:          0.5,
		Eviction:              EvictOldest,
		MinFreeBytes:          0,
		QuarantineMaxAge:      time.Hour,
		QuarantineMaxFraction: 1,
		CheckInterval:         time.Second,
	}
	if cfg.Spool != want {
		t.Fatalf("Spool = %+v, want %+v", cfg.Spool, want)
	}
	for _, policy := range []EvictionPolicy{EvictQuarantineThenOldest, EvictOldest, EvictNone} {
		cfg, _, err := Load(mapLookup(requiredEnv(map[string]string{EnvSpoolEvictionPolicy: string(policy)})))
		if err != nil || cfg.Spool.Eviction != policy {
			t.Fatalf("policy %q: error = %v, got %q", policy, err, cfg.Spool.Eviction)
		}
	}
}

func TestLoadRejectsInvalidSpoolSettings(t *testing.T) {
	tests := map[string]struct {
		env  map[string]string
		name string
	}{
		"high watermark zero":            {map[string]string{EnvSpoolHighWatermark: "0"}, EnvSpoolHighWatermark},
		"high watermark one":             {map[string]string{EnvSpoolHighWatermark: "1"}, EnvSpoolHighWatermark},
		"high watermark a percentage":    {map[string]string{EnvSpoolHighWatermark: "90"}, EnvSpoolHighWatermark},
		"high watermark negative":        {map[string]string{EnvSpoolHighWatermark: "-0.5"}, EnvSpoolHighWatermark},
		"high watermark not a number":    {map[string]string{EnvSpoolHighWatermark: "ninety"}, EnvSpoolHighWatermark},
		"high watermark NaN":             {map[string]string{EnvSpoolHighWatermark: "NaN"}, EnvSpoolHighWatermark},
		"high watermark infinite":        {map[string]string{EnvSpoolHighWatermark: "Inf"}, EnvSpoolHighWatermark},
		"low watermark zero":             {map[string]string{EnvSpoolLowWatermark: "0"}, EnvSpoolLowWatermark},
		"low watermark not a number":     {map[string]string{EnvSpoolLowWatermark: "most"}, EnvSpoolLowWatermark},
		"low watermark equals high":      {map[string]string{EnvSpoolHighWatermark: "0.8", EnvSpoolLowWatermark: "0.8"}, EnvSpoolLowWatermark},
		"low watermark above high":       {map[string]string{EnvSpoolHighWatermark: "0.7", EnvSpoolLowWatermark: "0.75"}, EnvSpoolLowWatermark},
		"high lowered below default low": {map[string]string{EnvSpoolHighWatermark: "0.75"}, EnvSpoolLowWatermark},
		"unknown policy":                 {map[string]string{EnvSpoolEvictionPolicy: "newest"}, EnvSpoolEvictionPolicy},
		"policy in capitals":             {map[string]string{EnvSpoolEvictionPolicy: "OLDEST"}, EnvSpoolEvictionPolicy},
		"min free negative":              {map[string]string{EnvSpoolMinFreeBytes: "-1"}, EnvSpoolMinFreeBytes},
		"min free with a unit":           {map[string]string{EnvSpoolMinFreeBytes: "512Mi"}, EnvSpoolMinFreeBytes},
		"min free fractional":            {map[string]string{EnvSpoolMinFreeBytes: "1.5"}, EnvSpoolMinFreeBytes},
		"min free above 1 TiB":           {map[string]string{EnvSpoolMinFreeBytes: "1099511627777"}, EnvSpoolMinFreeBytes},
		"quarantine fraction zero":       {map[string]string{EnvQuarantineMaxFraction: "0"}, EnvQuarantineMaxFraction},
		"quarantine fraction above one":  {map[string]string{EnvQuarantineMaxFraction: "1.01"}, EnvQuarantineMaxFraction},
		"quarantine fraction not a number": {
			map[string]string{EnvQuarantineMaxFraction: "a fifth"}, EnvQuarantineMaxFraction,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, _, err := Load(mapLookup(requiredEnv(tc.env)))
			if err == nil || !strings.Contains(err.Error(), tc.name) {
				t.Fatalf("Load() error = %v, want a startup error naming %s", err, tc.name)
			}
		})
	}
}

func TestLoadStrictRejectsABadSpoolSettingByName(t *testing.T) {
	for _, name := range []string{
		EnvSpoolHighWatermark, EnvSpoolLowWatermark, EnvSpoolEvictionPolicy,
		EnvSpoolMinFreeBytes, EnvQuarantineMaxAge, EnvQuarantineMaxFraction,
		EnvSpoolCheckInterval,
	} {
		_, err := LoadStrict(mapLookup(requiredEnv(map[string]string{name: "seven"})), nil)
		if err == nil || !strings.Contains(err.Error(), name) {
			t.Fatalf("LoadStrict() error = %v, want it to name %s", err, name)
		}
		if strings.Contains(err.Error(), "seven") {
			t.Fatalf("LoadStrict() error = %q, echoes the value", err)
		}
	}
}

// The spool's time settings once counted whole seconds under a _SECONDS
// name. There is no alias: a deployment that still sets an old name fails
// strict loading by that name, even when its value is a valid duration.
func TestOldSpoolSecondsNamesStopStartup(t *testing.T) {
	for _, old := range []string{
		"REQUEST_TRACE_UPLOADER_SPOOL_CHECK_INTERVAL_SECONDS",
		"REQUEST_TRACE_UPLOADER_QUARANTINE_MAX_AGE_SECONDS",
	} {
		_, err := LoadStrict(mapLookup(requiredEnv(map[string]string{old: "1h"})), []string{old + "=1h"})
		if err == nil || !strings.Contains(err.Error(), old) {
			t.Errorf("%s: LoadStrict() error = %v, want it rejected by name", old, err)
		}
	}
}
