// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"strings"
	"testing"
	"time"
)

func TestTuningSettingsDefaultToTodaysValues(t *testing.T) {
	cfg, warnings, err := Load(mapLookup(requiredEnv(nil)))
	if err != nil || len(warnings) != 0 {
		t.Fatalf("Load() error = %v warnings = %v", err, warnings)
	}
	if cfg.Batch != (BatchPolicy{}) {
		t.Fatalf("batch = %+v, want no batch setting, so the defaults of 16 segments and 1 GiB lowered to the backend's limits apply", cfg.Batch)
	}
	wantFaults := FaultPolicy{
		CreatesPerMinute:     30,
		CreateBurst:          1,
		SegmentScopeFailures: 5,
		AuthMaxBackoff:       30 * time.Minute,
		RetryAfterMax:        30 * time.Minute,
	}
	if cfg.SpoolBudgetBytes != 0 {
		t.Fatalf("spool budget = %d, want none, so fullness is measured on the filesystem only", cfg.SpoolBudgetBytes)
	}
	if cfg.Faults != wantFaults {
		t.Fatalf("faults = %+v, want %+v; an unset resend count is nil so the fault handling default applies", cfg.Faults, wantFaults)
	}
}

// unconfirmedResends reads the resend count, or -1 when it is unset.
func unconfirmedResends(c Config) any {
	if c.Faults.UnconfirmedResends == nil {
		return -1
	}
	return *c.Faults.UnconfirmedResends
}

func TestTuningSettingsApply(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		got  func(Config) any
		want any
	}{
		{"creates per minute", map[string]string{EnvCreatesPerMinute: " 120 "}, func(c Config) any { return c.Faults.CreatesPerMinute }, 120},
		{"creates per minute lowest", map[string]string{EnvCreatesPerMinute: "1"}, func(c Config) any { return c.Faults.CreatesPerMinute }, 1},
		{"creates per minute highest", map[string]string{EnvCreatesPerMinute: "600"}, func(c Config) any { return c.Faults.CreatesPerMinute }, 600},
		{"create burst", map[string]string{EnvCreateBurst: "4"}, func(c Config) any { return c.Faults.CreateBurst }, 4},
		{"create burst highest", map[string]string{EnvCreateBurst: "100"}, func(c Config) any { return c.Faults.CreateBurst }, 100},
		{"segment scope failures lowest", map[string]string{EnvSegmentScopeFailures: "2"}, func(c Config) any { return c.Faults.SegmentScopeFailures }, 2},
		{"segment scope failures highest", map[string]string{EnvSegmentScopeFailures: "100"}, func(c Config) any { return c.Faults.SegmentScopeFailures }, 100},
		{"auth max backoff", map[string]string{EnvAuthMaxBackoff: "2h"}, func(c Config) any { return c.Faults.AuthMaxBackoff }, 2 * time.Hour},
		{"auth max backoff lowest", map[string]string{EnvAuthMaxBackoff: "1m"}, func(c Config) any { return c.Faults.AuthMaxBackoff }, time.Minute},
		{"auth max backoff highest", map[string]string{EnvAuthMaxBackoff: "24h"}, func(c Config) any { return c.Faults.AuthMaxBackoff }, 24 * time.Hour},
		{"retry-after cap", map[string]string{EnvRetryAfterMax: "5m"}, func(c Config) any { return c.Faults.RetryAfterMax }, 5 * time.Minute},
		{"retry-after cap highest", map[string]string{EnvRetryAfterMax: "24h"}, func(c Config) any { return c.Faults.RetryAfterMax }, 24 * time.Hour},
		{"batch max segments", map[string]string{EnvBatchMaxSegments: "4"}, func(c Config) any { return c.Batch.MaxSegments }, 4},
		{"batch max segments highest", map[string]string{EnvBatchMaxSegments: "500"}, func(c Config) any { return c.Batch.MaxSegments }, 500},
		{"batch max bytes", map[string]string{EnvBatchMaxBytes: "536870912"}, func(c Config) any { return c.Batch.MaxBytes }, int64(512) << 20},
		{"batch max bytes lowest", map[string]string{EnvBatchMaxBytes: "1048576"}, func(c Config) any { return c.Batch.MaxBytes }, int64(1) << 20},
		{"batch max bytes highest", map[string]string{EnvBatchMaxBytes: "17179869184"}, func(c Config) any { return c.Batch.MaxBytes }, int64(16) << 30},
		{"unconfirmed resends none", map[string]string{EnvUnconfirmedResends: "0"}, unconfirmedResends, 0},
		{"unconfirmed resends", map[string]string{EnvUnconfirmedResends: "3"}, unconfirmedResends, 3},
		{"unconfirmed resends highest", map[string]string{EnvUnconfirmedResends: "10"}, unconfirmedResends, 10},
		{"spool budget", map[string]string{EnvSpoolBudgetBytes: " 2147483648 "}, func(c Config) any { return c.SpoolBudgetBytes }, int64(2) << 30},
		{"spool budget lowest", map[string]string{EnvSpoolBudgetBytes: "1073741824"}, func(c Config) any { return c.SpoolBudgetBytes }, int64(1) << 30},
		{"spool budget highest", map[string]string{EnvSpoolBudgetBytes: "1125899906842624"}, func(c Config) any { return c.SpoolBudgetBytes }, int64(1) << 50},
		{"spool budget zero is none", map[string]string{EnvSpoolBudgetBytes: "0"}, func(c Config) any { return c.SpoolBudgetBytes }, int64(0)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := LoadStrict(mapLookup(requiredEnv(tc.env)), nil)
			if err != nil {
				t.Fatalf("LoadStrict() error = %v", err)
			}
			if got := tc.got(cfg); got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBadTuningSettingsStopStartupByName(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{EnvCreatesPerMinute, "0"},
		{EnvCreatesPerMinute, "-1"},
		{EnvCreatesPerMinute, "601"},
		{EnvCreatesPerMinute, "thirty"},
		{EnvCreatesPerMinute, "2.5"},
		{EnvCreateBurst, "0"},
		{EnvCreateBurst, "101"},
		{EnvCreateBurst, "1e2"},
		{EnvSegmentScopeFailures, "0"},
		// One failure would quarantine before correlation can see the
		// backend failing; see minSegmentScopeFailures.
		{EnvSegmentScopeFailures, "1"},
		{EnvSegmentScopeFailures, "101"},
		{EnvSegmentScopeFailures, "five"},
		{EnvAuthMaxBackoff, "59s"},
		{EnvAuthMaxBackoff, "25h"},
		{EnvAuthMaxBackoff, "1800"},
		{EnvAuthMaxBackoff, "-30m"},
		{EnvAuthMaxBackoff, "soon"},
		{EnvRetryAfterMax, "59s"},
		{EnvRetryAfterMax, "0s"},
		{EnvRetryAfterMax, "25h"},
		{EnvRetryAfterMax, "1800"},
		{EnvBatchMaxSegments, "0"},
		{EnvBatchMaxSegments, "501"},
		{EnvBatchMaxSegments, "sixteen"},
		{EnvBatchMaxBytes, "0"},
		{EnvBatchMaxBytes, "1048575"},
		{EnvBatchMaxBytes, "17179869185"},
		{EnvBatchMaxBytes, "-1"},
		{EnvBatchMaxBytes, "512MiB"},
		{EnvBatchMaxBytes, "1GiB"},
		{EnvBatchMaxBytes, "1e9"},
		{EnvBatchMaxBytes, "99999999999999999999"},
		{EnvUnconfirmedResends, "-1"},
		{EnvUnconfirmedResends, "11"},
		{EnvUnconfirmedResends, "two"},
		{EnvUnconfirmedResends, "1.5"},
		{EnvSpoolBudgetBytes, "1073741823"},
		{EnvSpoolBudgetBytes, "1125899906842625"},
		{EnvSpoolBudgetBytes, "-1"},
		{EnvSpoolBudgetBytes, "2Gi"},
		{EnvSpoolBudgetBytes, "2GiB"},
		{EnvSpoolBudgetBytes, "2e9"},
		{EnvSpoolBudgetBytes, "two"},
	}
	for _, tc := range tests {
		t.Run(tc.name+"="+tc.value, func(t *testing.T) {
			_, err := LoadStrict(mapLookup(requiredEnv(map[string]string{tc.name: tc.value})), nil)
			if err == nil || !strings.Contains(err.Error(), tc.name) {
				t.Fatalf("LoadStrict() error = %v, want a startup error that names %s", err, tc.name)
			}
			// A one- or two-character value can appear inside the range
			// the message states, so only longer values are checked.
			if len(tc.value) > 2 && strings.Contains(err.Error(), tc.value) {
				t.Fatalf("LoadStrict() error = %q, echoes the value", err)
			}
		})
	}
}
