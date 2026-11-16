// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func TestLoadDefaultsForPipelineSettings(t *testing.T) {
	cfg, warnings, err := Load(mapLookup(requiredEnv(nil)))
	if err != nil || len(warnings) != 0 {
		t.Fatalf("Load() error = %v warnings = %v", err, warnings)
	}
	if cfg.AuditPrefix != "" || cfg.SkipRedaction || cfg.DrainTimeout != DefaultDrainTimeout || cfg.MaxJobAge != DefaultMaxJobAge {
		t.Fatalf("got audit %q skip redaction %v drain %v max job age %v", cfg.AuditPrefix, cfg.SkipRedaction, cfg.DrainTimeout, cfg.MaxJobAge)
	}
	if DefaultDrainTimeout != 60*time.Second {
		t.Fatalf("DefaultDrainTimeout = %v, want the shell exporter's 60 s", DefaultDrainTimeout)
	}
	if cfg.MaxJobAge != 6*time.Hour {
		t.Fatalf("max job age = %v, want the design's 6 h", cfg.MaxJobAge)
	}
	if cfg.QuarantineTrip != 5 || cfg.QuarantineResume != time.Hour {
		t.Fatalf("quarantine trip %d resume %v, want 5 and 1 h", cfg.QuarantineTrip, cfg.QuarantineResume)
	}
	if cfg.RecordMaxBytes != 256<<20 {
		t.Fatalf("record max bytes = %d, want the design's 256 MiB", cfg.RecordMaxBytes)
	}
	if cfg.ObjectStore.ResponseTimeout != 5*time.Minute {
		t.Fatalf("objectstore response timeout = %v, want 5m", cfg.ObjectStore.ResponseTimeout)
	}
	if got := cfg.Prefixes(); !slices.Equal(got, []string{DefaultSegmentPrefix}) {
		t.Fatalf("Prefixes() = %v, want only the segment prefix", got)
	}
}

func TestLoadParsesPipelineSettings(t *testing.T) {
	cfg, warnings, err := Load(mapLookup(requiredEnv(map[string]string{
		EnvAuditPrefix:      "request-audit",
		EnvRedactMultimodal: "False",
		EnvDrainTimeout:     "15s",
		EnvMaxJobAge:        "24h",
		EnvQuarantineTrip:   " 2 ",
		EnvQuarantineResume: "1m",
		EnvRecordMaxBytes:   " 1048576 ",

		EnvObjectStoreResponseTimeout: "90s",
	})))
	if err != nil || len(warnings) != 0 {
		t.Fatalf("Load() error = %v warnings = %v", err, warnings)
	}
	if cfg.AuditPrefix != "request-audit" || !cfg.SkipRedaction || cfg.DrainTimeout != 15*time.Second || cfg.MaxJobAge != 24*time.Hour {
		t.Fatalf("got audit %q skip redaction %v drain %v max job age %v", cfg.AuditPrefix, cfg.SkipRedaction, cfg.DrainTimeout, cfg.MaxJobAge)
	}
	if cfg.QuarantineTrip != 2 || cfg.QuarantineResume != time.Minute {
		t.Fatalf("got quarantine trip %d resume %v, want 2 and 1 m", cfg.QuarantineTrip, cfg.QuarantineResume)
	}
	if cfg.RecordMaxBytes != 1<<20 {
		t.Fatalf("record max bytes = %d, want 1 MiB", cfg.RecordMaxBytes)
	}
	if cfg.ObjectStore.ResponseTimeout != 90*time.Second {
		t.Fatalf("objectstore response timeout = %v, want 90s", cfg.ObjectStore.ResponseTimeout)
	}
	if got := cfg.Prefixes(); !slices.Equal(got, []string{DefaultSegmentPrefix, "request-audit"}) {
		t.Fatalf("Prefixes() = %v, want both families", got)
	}
	cfg, _, err = Load(mapLookup(requiredEnv(map[string]string{EnvDrainTimeout: "0s"})))
	if err != nil || cfg.DrainTimeout != 0 {
		t.Fatalf("drain 0: error = %v drain = %v, want no drain wait", err, cfg.DrainTimeout)
	}
}

func TestLoadRejectsInvalidPipelineSettings(t *testing.T) {
	tests := map[string]map[string]string{
		"audit prefix with a separator":  {EnvAuditPrefix: "audit/x"},
		"audit prefix equals the prefix": {EnvAuditPrefix: DefaultSegmentPrefix},
		"audit prefix extends the other": {EnvAuditPrefix: DefaultSegmentPrefix + ".audit"},
		"redaction not a boolean":        {EnvRedactMultimodal: "sometimes"},
		"drain negative":                 {EnvDrainTimeout: "-1s"},
		"drain without a unit":           {EnvDrainTimeout: "60"},
		"drain too long":                 {EnvDrainTimeout: "61m"},
		"max job age zero":               {EnvMaxJobAge: "0s"},
		"max job age without a unit":     {EnvMaxJobAge: "21600"},
		"quarantine trip zero":           {EnvQuarantineTrip: "0"},
		"quarantine trip negative":       {EnvQuarantineTrip: "-1"},
		"quarantine trip not a number":   {EnvQuarantineTrip: "five"},
		"quarantine trip fractional":     {EnvQuarantineTrip: "2.5"},
		"quarantine trip too large":      {EnvQuarantineTrip: "1001"},
		"quarantine resume too short":    {EnvQuarantineResume: "59s"},
		"quarantine resume zero":         {EnvQuarantineResume: "0s"},
		"quarantine resume without unit": {EnvQuarantineResume: "3600"},
		"quarantine resume too long":     {EnvQuarantineResume: "25h"},
		"record max zero":                {EnvRecordMaxBytes: "0"},
		"record max negative":            {EnvRecordMaxBytes: "-1"},
		"record max with a unit":         {EnvRecordMaxBytes: "256MiB"},
		"record max fractional":          {EnvRecordMaxBytes: "1048576.5"},
		"record max not a number":        {EnvRecordMaxBytes: "large"},
		"record max under 1 MiB":         {EnvRecordMaxBytes: "1048575"},
		"record max over 512 MiB":        {EnvRecordMaxBytes: "536870913"},
		"response timeout without unit":  {EnvObjectStoreResponseTimeout: "300"},
		"response timeout zero":          {EnvObjectStoreResponseTimeout: "0s"},
		"response timeout negative":      {EnvObjectStoreResponseTimeout: "-1m"},
		"response timeout under 10s":     {EnvObjectStoreResponseTimeout: "9s"},
		"response timeout over 1h":       {EnvObjectStoreResponseTimeout: "61m"},
		"response timeout not a time":    {EnvObjectStoreResponseTimeout: "soon"},
	}
	for name, env := range tests {
		t.Run(name, func(t *testing.T) {
			if _, _, err := Load(mapLookup(requiredEnv(env))); err == nil {
				t.Fatal("Load() error = nil, want a startup error")
			}
		})
	}
}

func mapLookup(values map[string]string) LookupFunc {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}

// bashNames are the names the Bash uploader's chart sets for settings Go also
// has. Go reads only its own names, so none of them may change a setting.
var bashNames = map[string]string{
	"TRACE_DIR":               "/bash-spool",
	"TRACE_FILE_PREFIX":       "bash-trace",
	"UPLOAD_INTERVAL_SECONDS": "60",
	"SHUTDOWN_DRAIN_SECONDS":  "15",
}

func TestLoadStrictIgnoresBashEnvironmentNames(t *testing.T) {
	env := requiredEnv(nil)
	environ := []string{EnvSourceDir + "=" + env[EnvSourceDir], EnvBackend + "=" + env[EnvBackend]}
	for name, value := range bashNames {
		env[name] = value
		environ = append(environ, name+"="+value)
	}
	cfg, err := LoadStrict(mapLookup(env), environ)
	if err != nil {
		t.Fatalf("LoadStrict() error = %v", err)
	}
	if cfg.SourceDir != env[EnvSourceDir] || cfg.SegmentPrefix != DefaultSegmentPrefix ||
		cfg.ScanInterval != DefaultScanInterval || cfg.DrainTimeout != DefaultDrainTimeout {
		t.Fatalf("got source %q prefix %q scan %v drain %v, want the Go settings and defaults only",
			cfg.SourceDir, cfg.SegmentPrefix, cfg.ScanInterval, cfg.DrainTimeout)
	}
}

func TestLoadStrictNeedsTheGoSourceDirEvenWhenTheBashOneIsSet(t *testing.T) {
	env := map[string]string{EnvBackend: string(BackendDebug), "TRACE_DIR": "/bash-spool"}
	_, err := LoadStrict(mapLookup(env), []string{"TRACE_DIR=/bash-spool"})
	if err == nil || !strings.Contains(err.Error(), EnvSourceDir) {
		t.Fatalf("LoadStrict() error = %v, want %s required", err, EnvSourceDir)
	}
}

func TestLoadStrictTurnsEveryFallbackIntoAnError(t *testing.T) {
	_, err := LoadStrict(mapLookup(requiredEnv(map[string]string{EnvScanInterval: "soon"})), nil)
	if err == nil || !strings.Contains(err.Error(), EnvScanInterval) {
		t.Fatalf("LoadStrict() error = %v, want it to name %s", err, EnvScanInterval)
	}
	if strings.Contains(err.Error(), "soon") {
		t.Fatalf("LoadStrict() error = %q, echoes the value", err)
	}
}

func TestLoadStrictRejectsABadQuarantineSettingByName(t *testing.T) {
	for _, name := range []string{EnvQuarantineTrip, EnvQuarantineResume} {
		_, err := LoadStrict(mapLookup(requiredEnv(map[string]string{name: "seven"})), nil)
		if err == nil || !strings.Contains(err.Error(), name) {
			t.Fatalf("LoadStrict() error = %v, want it to name %s", err, name)
		}
		if strings.Contains(err.Error(), "seven") {
			t.Fatalf("LoadStrict() error = %q, echoes the value", err)
		}
	}
}

func TestLoadStrictRejectsUnknownSettings(t *testing.T) {
	environ := []string{"PATH=/bin", "OTHER_SERVICE_SETTING=x", EnvSourceDir + "=/records", "REQUEST_TRACE_UPLOADER_SCAN_INTERVL=30s"}
	_, err := LoadStrict(mapLookup(requiredEnv(nil)), environ)
	if err == nil || !strings.Contains(err.Error(), "REQUEST_TRACE_UPLOADER_SCAN_INTERVL") {
		t.Fatalf("LoadStrict() error = %v, want the misspelled setting named", err)
	}
}

// The uploader reads only these settings, all of them generic or for a
// backend this module links. README.md lists the same names; a setting
// added or removed here must be added or removed there too.
func TestStrictModeAcceptsExactlyTheDocumentedSettings(t *testing.T) {
	want := []string{
		EnvAttemptTimeout, EnvAuditPrefix, EnvAuthMaxBackoff, EnvBackend,
		EnvBatchMaxBytes, EnvBatchMaxSegments, EnvCreateBurst,
		EnvCreatesPerMinute, EnvDebugVerbosity, EnvDrainTimeout,
		EnvHealthAddr, EnvMaxJobAge, EnvMaxRetries,
		EnvObjectStoreBucket, EnvObjectStoreDryRun, EnvObjectStoreEndpoint,
		EnvObjectStoreKeyPrefix, EnvObjectStorePathStyle, EnvObjectStoreRegion,
		EnvObjectStoreResponseTimeout,
		EnvOperationTimeout, EnvPrepareMemoryBytes, EnvPrepareWorkers,
		EnvQuarantineDir, EnvQuarantineMaxAge, EnvQuarantineMaxFraction,
		EnvQuarantineResume, EnvQuarantineTrip, EnvRecordMaxBytes,
		EnvRedactMultimodal,
		EnvRetryAfterMax, EnvRetryInitialBackoff, EnvRetryMaximumBackoff,
		EnvRetryMultiplier, EnvScanInterval, EnvSecretsFile,
		EnvSegmentPrefix, EnvSegmentScopeFailures, EnvSourceDir,
		EnvSpoolBudgetBytes, EnvSpoolCheckInterval, EnvSpoolEvictionPolicy,
		EnvSpoolHighWatermark, EnvSpoolLowWatermark, EnvSpoolMinFreeBytes,
		EnvStateDir, EnvStatusInterval, EnvSuppressHeaderless,
		EnvSuppressionRules, EnvSuppressionStrict, EnvSuppressNCAHeaders,
		EnvSuppressNCAIDs, EnvUnconfirmedResends,
	}
	var got []string
	for name := range knownSettings() {
		got = append(got, name)
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("settings Load reads:\n got  %v\n want %v", got, want)
	}
}

// Errors name the setting, never its value.
func TestAuditPrefixErrorsDoNotQuoteTheValue(t *testing.T) {
	for name, value := range map[string]string{
		"separator": "secret-audit/x",
		"equal":     DefaultSegmentPrefix,
		"extends":   DefaultSegmentPrefix + ".secret-audit",
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := Load(mapLookup(requiredEnv(map[string]string{EnvAuditPrefix: value})))
			if err == nil {
				t.Fatal("Load() error = nil, want a startup error")
			}
			if !strings.Contains(err.Error(), EnvAuditPrefix) || strings.Contains(err.Error(), value) {
				t.Fatalf("Load() error = %q, want it to name %s and not quote the value", err, EnvAuditPrefix)
			}
		})
	}
}
