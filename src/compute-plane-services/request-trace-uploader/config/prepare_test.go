// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"strings"
	"testing"
)

func TestPrepareSettingsDefaultToOneWorker(t *testing.T) {
	cfg, err := LoadStrict(mapLookup(requiredEnv(nil)), nil)
	if err != nil {
		t.Fatalf("LoadStrict() error = %v", err)
	}
	want := PreparePolicy{Workers: 1, MemoryBytes: 1 << 30}
	if cfg.Prepare != want {
		t.Fatalf("Prepare = %+v, want %+v", cfg.Prepare, want)
	}
	if DefaultPrepareWorkers != want.Workers || DefaultPrepareMemoryBytes != want.MemoryBytes {
		t.Fatalf("defaults = %d workers and %d bytes, want %+v", DefaultPrepareWorkers, DefaultPrepareMemoryBytes, want)
	}
}

func TestPrepareSettingsAreRead(t *testing.T) {
	tests := []struct {
		workers, memory string
		want            PreparePolicy
	}{
		{"4", "2147483648", PreparePolicy{Workers: 4, MemoryBytes: 2 << 30}},
		{" 1 ", "67108864", PreparePolicy{Workers: 1, MemoryBytes: 64 << 20}},
		{"16", "68719476736", PreparePolicy{Workers: 16, MemoryBytes: 64 << 30}},
	}
	for _, tt := range tests {
		cfg, err := LoadStrict(mapLookup(requiredEnv(map[string]string{
			EnvPrepareWorkers:     tt.workers,
			EnvPrepareMemoryBytes: tt.memory,
		})), nil)
		if err != nil {
			t.Fatalf("LoadStrict(%q, %q) error = %v", tt.workers, tt.memory, err)
		}
		if cfg.Prepare != tt.want {
			t.Errorf("LoadStrict(%q, %q) Prepare = %+v, want %+v", tt.workers, tt.memory, cfg.Prepare, tt.want)
		}
	}
}

func TestPrepareSettingsRejectBadValues(t *testing.T) {
	tests := map[string]map[string]string{
		"no workers":              {EnvPrepareWorkers: "0"},
		"negative workers":        {EnvPrepareWorkers: "-2"},
		"too many workers":        {EnvPrepareWorkers: "17"},
		"workers not a number":    {EnvPrepareWorkers: "auto"},
		"workers fractional":      {EnvPrepareWorkers: "1.5"},
		"memory below 64 MiB":     {EnvPrepareMemoryBytes: "67108863"},
		"memory above 64 GiB":     {EnvPrepareMemoryBytes: "68719476737"},
		"memory with a unit":      {EnvPrepareMemoryBytes: "1Gi"},
		"memory negative":         {EnvPrepareMemoryBytes: "-1"},
		"memory beyond int64":     {EnvPrepareMemoryBytes: "99999999999999999999"},
		"memory not a number":     {EnvPrepareMemoryBytes: "lots"},
		"memory fractional bytes": {EnvPrepareMemoryBytes: "1e9"},
	}
	for name, env := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := LoadStrict(mapLookup(requiredEnv(env)), nil)
			if err == nil {
				t.Fatal("LoadStrict() error = nil, want a startup error")
			}
			for setting, value := range env {
				if !strings.Contains(err.Error(), setting) {
					t.Errorf("LoadStrict() error = %q, want it to name %s", err, setting)
				}
				if strings.Contains(err.Error(), value) {
					t.Errorf("LoadStrict() error = %q, echoes the value", err)
				}
			}
		})
	}
}
