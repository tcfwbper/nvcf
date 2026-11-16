// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/segment"
)

const (
	// EnvAuditPrefix names a second segment family on the same spool, for a
	// producer configured to write one. Empty uploads one family.
	EnvAuditPrefix = "REQUEST_TRACE_UPLOADER_AUDIT_PREFIX"
	// EnvRedactMultimodal turns redaction of inline media on or off.
	EnvRedactMultimodal = "REQUEST_TRACE_UPLOADER_REDACT_MULTIMODAL"
	// EnvDrainTimeout bounds the shutdown drain after SIGTERM, as a duration
	// such as "60s". A deployment sets it to fit the pod's grace period.
	EnvDrainTimeout = "REQUEST_TRACE_UPLOADER_DRAIN_TIMEOUT"
	// EnvMaxJobAge is how long a submitted job may stay unfinished before it
	// is reported stranded, as a duration such as "6h".
	EnvMaxJobAge = "REQUEST_TRACE_UPLOADER_MAX_JOB_AGE"
	// EnvStatusInterval is how often the uploader polls the status of every
	// open job, as a duration such as "30s".
	EnvStatusInterval = "REQUEST_TRACE_UPLOADER_STATUS_INTERVAL"
	// DefaultPollInterval is the default for EnvStatusInterval.
	DefaultPollInterval = 30 * time.Second
	// EnvQuarantineTrip is how many segments quarantined in a row stop
	// intake.
	EnvQuarantineTrip = "REQUEST_TRACE_UPLOADER_QUARANTINE_TRIP"
	// EnvQuarantineResume is how long intake stays stopped after a
	// quarantine trip before it reads new segments again, as a duration such
	// as "1h".
	EnvQuarantineResume = "REQUEST_TRACE_UPLOADER_QUARANTINE_RESUME"
	// EnvRecordMaxBytes is the most bytes one source record, one line of a
	// segment, may have, as whole bytes. A longer record is not parsed: its
	// raw bytes go to the local reject file and it is counted.
	EnvRecordMaxBytes = "REQUEST_TRACE_UPLOADER_RECORD_MAX_BYTES"
	// DefaultRecordMaxBytes is the default for EnvRecordMaxBytes, 256 MiB.
	DefaultRecordMaxBytes = int64(256) << 20
	// EnvObjectStoreResponseTimeout bounds how long the objectstore backend
	// waits for a store's answer once a request, body included, is sent, as
	// a duration such as "5m". It never cuts an upload on a slow link.
	EnvObjectStoreResponseTimeout = "REQUEST_TRACE_UPLOADER_OBJECTSTORE_RESPONSE_TIMEOUT"
	// DefaultObjectStoreResponseTimeout is the default for
	// EnvObjectStoreResponseTimeout.
	DefaultObjectStoreResponseTimeout = 5 * time.Minute

	DefaultDrainTimeout     = 60 * time.Second
	DefaultMaxJobAge        = 6 * time.Hour
	DefaultQuarantineTrip   = 5
	DefaultQuarantineResume = time.Hour
	// maxQuarantineTrip bounds EnvQuarantineTrip. A larger run is no longer
	// a useful stop.
	maxQuarantineTrip = 1000
	// minRecordMaxBytes keeps a unit mistake, such as 256 meant as MiB,
	// from rejecting every record.
	minRecordMaxBytes = int64(1) << 20
	// maxRecordMaxBytes is the reader's own bound on one line; a larger
	// record would not fit the memory the design plans for.
	maxRecordMaxBytes = int64(512) << 20

	// settingPrefix starts every setting name this package reads, except
	// HEALTH_ADDR. LoadStrict rejects any other name with this prefix.
	settingPrefix = "REQUEST_TRACE_UPLOADER_"
)

// Prefixes lists the segment families to upload: the segment prefix, then
// the audit prefix when one is set.
func (c Config) Prefixes() []string {
	if c.AuditPrefix == "" {
		return []string{c.SegmentPrefix}
	}
	return []string{c.SegmentPrefix, c.AuditPrefix}
}

// LoadStrict reads Config the way a deployed uploader must: only from this
// package's own setting names, with every value that Load would replace by a
// default treated as an error, and with any unknown name that carries this
// package's prefix in environ treated as an error too, so a misspelled
// setting cannot be ignored. environ is the process environment in
// os.Environ form. Errors name settings, never their values.
func LoadStrict(lookup LookupFunc, environ []string) (Config, error) {
	cfg, warnings, err := Load(lookup)
	if err != nil {
		return Config{}, err
	}
	if len(warnings) > 0 {
		return Config{}, fmt.Errorf("invalid or incompatible values for %s", strings.Join(warnings, ", "))
	}
	if unknown := unknownSettings(environ); len(unknown) > 0 {
		return Config{}, fmt.Errorf("unknown settings %s", strings.Join(unknown, ", "))
	}
	return cfg, nil
}

// knownSettings is the set of names Load reads, found by running Load once
// with a recording lookup, so it cannot drift from Load.
var knownSettings = sync.OnceValue(func() map[string]bool {
	names := map[string]bool{}
	_, _, _ = Load(func(name string) (string, bool) {
		names[name] = true
		switch name {
		case EnvSourceDir:
			return "/", true
		case EnvBackend:
			return string(BackendDebug), true
		}
		return "", false
	})
	return names
})

func unknownSettings(environ []string) []string {
	known := knownSettings()
	var unknown []string
	for _, entry := range environ {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, settingPrefix) && !known[name] {
			unknown = append(unknown, name)
		}
	}
	sort.Strings(unknown)
	return unknown
}

func auditPrefixValue(lookup LookupFunc, segmentPrefix string) (string, error) {
	value := strings.TrimSpace(valueOrDefault(lookup, EnvAuditPrefix, ""))
	if value == "" {
		return "", nil
	}
	if err := segment.ValidatePrefixes([]string{segmentPrefix, value}); err != nil {
		return "", fmt.Errorf("%s: %w", EnvAuditPrefix, err)
	}
	return value, nil
}

// strictBool parses a new boolean setting. Unlike the older settings, a bad
// value is an error and never a fallback.
func strictBool(lookup LookupFunc, name string, fallback bool) (bool, error) {
	value := strings.TrimSpace(valueOrDefault(lookup, name, ""))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false", name)
	}
	return parsed, nil
}

// strictInt parses a new whole-number setting within bounds. A bad value is
// an error and never a fallback.
func strictInt(lookup LookupFunc, name string, fallback, minimum, maximum int) (int, error) {
	value := strings.TrimSpace(valueOrDefault(lookup, name, ""))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < minimum || parsed > maximum {
		return 0, fmt.Errorf("%s must be a whole number from %d to %d", name, minimum, maximum)
	}
	return parsed, nil
}

// strictBytes parses a whole-bytes setting, such as 268435456 for 256 MiB,
// within bounds. A unit suffix or any other bad value is an error and never
// a fallback.
func strictBytes(lookup LookupFunc, name string, fallback, minimum, maximum int64) (int64, error) {
	value := strings.TrimSpace(valueOrDefault(lookup, name, ""))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < minimum || parsed > maximum {
		return 0, fmt.Errorf("%s must be whole bytes, with no unit, from %d to %d", name, minimum, maximum)
	}
	return parsed, nil
}

// strictDuration parses a duration setting, such as "30s" or "1h", within
// bounds. A bare number has no unit and is refused, as is any other bad
// value: it is an error and never a fallback.
func strictDuration(lookup LookupFunc, name string, fallback, minimum, maximum time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(valueOrDefault(lookup, name, ""))
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || strings.Trim(value, "+-.0123456789") == "" || parsed < minimum || parsed > maximum {
		return 0, fmt.Errorf("%s must be a duration with a unit, such as 30s or 1h, from %s to %s", name, minimum, maximum)
	}
	return parsed, nil
}
