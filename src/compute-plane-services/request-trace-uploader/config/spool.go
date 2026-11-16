// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/diskguard"
)

const (
	// EnvSpoolHighWatermark is the used fraction of the spool above which
	// preparing pauses and the eviction policy deletes data.
	EnvSpoolHighWatermark = "REQUEST_TRACE_UPLOADER_SPOOL_HIGH_WATERMARK"
	// EnvSpoolLowWatermark is the used fraction the eviction policy frees the
	// spool down to once it is above the high watermark.
	EnvSpoolLowWatermark = "REQUEST_TRACE_UPLOADER_SPOOL_LOW_WATERMARK"
	// EnvSpoolEvictionPolicy is what is deleted above the high watermark.
	EnvSpoolEvictionPolicy = "REQUEST_TRACE_UPLOADER_SPOOL_EVICTION_POLICY"
	// EnvSpoolMinFreeBytes is the least free space, in bytes, preparing
	// leaves for the writer.
	EnvSpoolMinFreeBytes = "REQUEST_TRACE_UPLOADER_SPOOL_MIN_FREE_BYTES"
	// EnvQuarantineMaxAge is how long a quarantined entry is kept, as a
	// duration such as "168h".
	EnvQuarantineMaxAge = "REQUEST_TRACE_UPLOADER_QUARANTINE_MAX_AGE"
	// EnvQuarantineMaxFraction bounds quarantine to a fraction of the spool.
	EnvQuarantineMaxFraction = "REQUEST_TRACE_UPLOADER_QUARANTINE_MAX_FRACTION"
	// EnvSpoolCheckInterval is how often the spool is measured and
	// quarantine bounded, apart from scans, as a duration such as "10s".
	EnvSpoolCheckInterval = "REQUEST_TRACE_UPLOADER_SPOOL_CHECK_INTERVAL"

	DefaultSpoolHighWatermark    = diskguard.DefaultHighWatermark
	DefaultSpoolLowWatermark     = diskguard.DefaultLowWatermark
	DefaultSpoolEvictionPolicy   = EvictQuarantineThenOldest
	DefaultSpoolMinFreeBytes     = diskguard.DefaultMinFreeBytes
	DefaultQuarantineMaxAge      = diskguard.DefaultQuarantineMaxAge
	DefaultQuarantineMaxFraction = diskguard.DefaultQuarantineMaxFraction
	DefaultSpoolCheckInterval    = 10 * time.Second
	maxSpoolMinFreeBytes         = int64(1) << 40
	minQuarantineMaxAge          = time.Minute
	maxQuarantineMaxAge          = 30 * 24 * time.Hour
	maxSpoolCheckInterval        = time.Hour
)

// EvictionPolicy is what the uploader deletes when the spool is above the
// high watermark. Every deletion is counted as loss.
type EvictionPolicy string

const (
	// EvictQuarantineThenOldest deletes quarantined entries first, then the
	// oldest closed segments that are in no batch.
	EvictQuarantineThenOldest = EvictionPolicy(diskguard.PolicyQuarantineThenOldest)
	// EvictOldest deletes the oldest closed segments that are in no batch and
	// keeps quarantine.
	EvictOldest = EvictionPolicy(diskguard.PolicyOldest)
	// EvictNone deletes nothing; preparing pauses until space frees up.
	EvictNone = EvictionPolicy(diskguard.PolicyNone)
)

// SpoolPolicy bounds spool use. A zero SpoolPolicy, as in a Config built by
// hand, uses the defaults; one that is only partly set fails startup.
type SpoolPolicy struct {
	// HighWatermark is the used fraction of the spool above which preparing
	// pauses and Eviction deletes data.
	HighWatermark float64
	// LowWatermark is the used fraction Eviction frees the spool down to once
	// it is above HighWatermark. It is below HighWatermark.
	LowWatermark float64
	Eviction     EvictionPolicy
	// MinFreeBytes is the least free space preparing leaves for the writer.
	MinFreeBytes int64
	// QuarantineMaxAge bounds how long a quarantined entry is kept.
	QuarantineMaxAge time.Duration
	// QuarantineMaxFraction bounds quarantine to a fraction of the spool.
	QuarantineMaxFraction float64
	// CheckInterval is how often the spool is measured and quarantine
	// bounded, apart from scans. Zero uses the default.
	CheckInterval time.Duration
}

// spoolPolicyValue reads the spool settings. Every bad value is a startup
// error that names the setting, never a fallback.
func spoolPolicyValue(lookup LookupFunc) (SpoolPolicy, error) {
	high, err := strictFraction(lookup, EnvSpoolHighWatermark, DefaultSpoolHighWatermark, false)
	if err != nil {
		return SpoolPolicy{}, err
	}
	low, err := strictFraction(lookup, EnvSpoolLowWatermark, DefaultSpoolLowWatermark, false)
	if err != nil {
		return SpoolPolicy{}, err
	}
	if low >= high {
		return SpoolPolicy{}, fmt.Errorf("%s must be below %s", EnvSpoolLowWatermark, EnvSpoolHighWatermark)
	}
	eviction, err := evictionPolicyValue(lookup)
	if err != nil {
		return SpoolPolicy{}, err
	}
	minFree, err := strictBytes(lookup, EnvSpoolMinFreeBytes, DefaultSpoolMinFreeBytes, 0, maxSpoolMinFreeBytes)
	if err != nil {
		return SpoolPolicy{}, err
	}
	maxAge, err := strictDuration(lookup, EnvQuarantineMaxAge, DefaultQuarantineMaxAge, minQuarantineMaxAge, maxQuarantineMaxAge)
	if err != nil {
		return SpoolPolicy{}, err
	}
	maxFraction, err := strictFraction(lookup, EnvQuarantineMaxFraction, DefaultQuarantineMaxFraction, true)
	if err != nil {
		return SpoolPolicy{}, err
	}
	checkInterval, err := strictDuration(lookup, EnvSpoolCheckInterval, DefaultSpoolCheckInterval, time.Second, maxSpoolCheckInterval)
	if err != nil {
		return SpoolPolicy{}, err
	}
	return SpoolPolicy{
		HighWatermark:         high,
		LowWatermark:          low,
		Eviction:              eviction,
		MinFreeBytes:          minFree,
		QuarantineMaxAge:      maxAge,
		QuarantineMaxFraction: maxFraction,
		CheckInterval:         checkInterval,
	}, nil
}

func evictionPolicyValue(lookup LookupFunc) (EvictionPolicy, error) {
	value := EvictionPolicy(strings.TrimSpace(valueOrDefault(lookup, EnvSpoolEvictionPolicy, "")))
	switch value {
	case "":
		return DefaultSpoolEvictionPolicy, nil
	case EvictQuarantineThenOldest, EvictOldest, EvictNone:
		return value, nil
	}
	return "", fmt.Errorf("%s must be %s, %s or %s", EnvSpoolEvictionPolicy, EvictQuarantineThenOldest, EvictOldest, EvictNone)
}

// strictFraction parses a new fraction setting: a number above 0 and below
// 1, or up to and including 1 when includeOne is set. A bad value is an
// error and never a fallback.
func strictFraction(lookup LookupFunc, name string, fallback float64, includeOne bool) (float64, error) {
	value := strings.TrimSpace(valueOrDefault(lookup, name, ""))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	inRange := parsed > 0 && (parsed < 1 || includeOne && parsed == 1)
	if err != nil || math.IsNaN(parsed) || !inRange {
		if includeOne {
			return 0, fmt.Errorf("%s must be a number above 0 and at most 1", name)
		}
		return 0, fmt.Errorf("%s must be a number above 0 and below 1", name)
	}
	return parsed, nil
}
