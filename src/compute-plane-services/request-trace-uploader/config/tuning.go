// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Tuning settings let an operator change batching, fault handling and the
// spool budget with a restart instead of a new build. Each default is the
// value the uploader used before the setting existed.
const (
	// EnvCreatesPerMinute is the per-pod budget for creating backend jobs.
	EnvCreatesPerMinute = "REQUEST_TRACE_UPLOADER_CREATES_PER_MINUTE"
	// EnvCreateBurst is how many creates may start back to back once the
	// budget has been idle.
	EnvCreateBurst = "REQUEST_TRACE_UPLOADER_CREATE_BURST"
	// EnvSegmentScopeFailures is how many failures of one segment, while
	// others succeed, quarantine it. It also bounds retries of a segment
	// fault the backend says may pass.
	EnvSegmentScopeFailures = "REQUEST_TRACE_UPLOADER_SEGMENT_SCOPE_FAILURES"
	// EnvAuthMaxBackoff is the longest hold after the destination refuses
	// the credentials, as a duration such as "30m". Holds double from one
	// minute up to it.
	EnvAuthMaxBackoff = "REQUEST_TRACE_UPLOADER_AUTH_MAX_BACKOFF"
	// EnvRetryAfterMax caps how long a destination's Retry-After may hold
	// uploads, as a duration such as "30m".
	EnvRetryAfterMax = "REQUEST_TRACE_UPLOADER_RETRY_AFTER_MAX"
	// EnvBatchMaxSegments is the most segments in one upload batch. A value
	// above the backend's file limit per job stops startup.
	EnvBatchMaxSegments = "REQUEST_TRACE_UPLOADER_BATCH_MAX_SEGMENTS"
	// EnvBatchMaxBytes is the most transport bytes in one upload batch, as
	// whole bytes. A value above the backend's batch limit stops startup. A
	// segment whose transport alone is larger is quarantined.
	EnvBatchMaxBytes = "REQUEST_TRACE_UPLOADER_BATCH_MAX_BYTES"
	// EnvUnconfirmedResends is how many times an upload the destination
	// could not confirm is sent again before its segment is quarantined,
	// on a backend that cannot take a resend safely.
	EnvUnconfirmedResends = "REQUEST_TRACE_UPLOADER_UNCONFIRMED_RESENDS"
	// EnvSpoolBudgetBytes is the spool's own size, as whole bytes, such as
	// the size limit of its emptyDir. The disk guard then measures the
	// files the spool holds against it as well as the filesystem. Unset or
	// 0 measures the filesystem only.
	EnvSpoolBudgetBytes = "REQUEST_TRACE_UPLOADER_SPOOL_BUDGET_BYTES"

	DefaultCreatesPerMinute     = 30
	DefaultCreateBurst          = 1
	DefaultSegmentScopeFailures = 5
	DefaultAuthMaxBackoff       = 30 * time.Minute
	DefaultRetryAfterMax        = 30 * time.Minute

	maxCreatesPerMinute = 600
	maxCreateBurst      = 100
	// minSegmentScopeFailures is 2 because at 1 a segment's first fault of
	// unknown scope quarantines it, and the quarantine drops it from
	// correlation, so a fault of the whole destination is never seen as
	// one and quarantines healthy segments instead of holding.
	minSegmentScopeFailures = 2
	maxSegmentScopeFailures = 100
	// minAuthMaxBackoff is the first auth hold, which the cap may not be
	// below.
	minAuthMaxBackoff = time.Minute
	minRetryAfterMax  = time.Minute
	maxFaultHold      = 24 * time.Hour
	// maxBatchMaxSegments is the most files a batch carries by default.
	maxBatchMaxSegments = 500
	// minBatchMaxBytes keeps a unit mistake, such as 512 meant as MiB, from
	// quarantining every segment.
	minBatchMaxBytes = int64(1) << 20
	maxBatchMaxBytes = int64(16) << 30
	// maxUnconfirmedResends bounds EnvUnconfirmedResends. Unset uses the
	// fault handling default of 2.
	maxUnconfirmedResends = 10
	// minSpoolBudgetBytes is twice the disk guard's 512 MiB free-space
	// floor, so a budget leaves room to prepare, and a unit mistake, such
	// as 2 meant as GiB, stops startup.
	minSpoolBudgetBytes = int64(1) << 30
	maxSpoolBudgetBytes = int64(1) << 50
)

// BatchPolicy bounds each upload batch. A zero field, also the value when
// its setting is unset, uses the batch default lowered to the backend's
// limit: 16 segments and 1 GiB. A set value above the backend's limit stops
// startup, so zero must stay apart from the default.
type BatchPolicy struct {
	// MaxSegments is the most segments in one batch.
	MaxSegments int
	// MaxBytes is the most transport bytes in one batch.
	MaxBytes int64
}

// FaultPolicy tunes fault handling. A zero field, as in a Config built by
// hand, uses the fault handling default.
type FaultPolicy struct {
	// CreatesPerMinute is the per-pod budget for creating jobs.
	CreatesPerMinute int
	// CreateBurst is how many creates may start back to back.
	CreateBurst int
	// SegmentScopeFailures is how many failures of one segment quarantine
	// it.
	SegmentScopeFailures int
	// AuthMaxBackoff is the longest hold after an auth failure.
	AuthMaxBackoff time.Duration
	// RetryAfterMax caps a destination's Retry-After.
	RetryAfterMax time.Duration
	// UnconfirmedResends is how many times an unconfirmed upload is sent
	// again. Zero is a valid count, so nil, also the value when the
	// setting is unset, uses the fault handling default.
	UnconfirmedResends *int
}

// batchPolicyValue reads the batch settings. Every bad value is a startup
// error that names the setting, never a fallback.
func batchPolicyValue(lookup LookupFunc) (BatchPolicy, error) {
	segments, err := strictInt(lookup, EnvBatchMaxSegments, 0, 1, maxBatchMaxSegments)
	if err != nil {
		return BatchPolicy{}, err
	}
	bytes, err := strictByteSize(lookup, EnvBatchMaxBytes, minBatchMaxBytes, maxBatchMaxBytes)
	if err != nil {
		return BatchPolicy{}, err
	}
	return BatchPolicy{MaxSegments: segments, MaxBytes: bytes}, nil
}

// faultPolicyValue reads the fault handling settings. Every bad value is a
// startup error that names the setting, never a fallback.
func faultPolicyValue(lookup LookupFunc) (FaultPolicy, error) {
	creates, err := strictInt(lookup, EnvCreatesPerMinute, DefaultCreatesPerMinute, 1, maxCreatesPerMinute)
	if err != nil {
		return FaultPolicy{}, err
	}
	burst, err := strictInt(lookup, EnvCreateBurst, DefaultCreateBurst, 1, maxCreateBurst)
	if err != nil {
		return FaultPolicy{}, err
	}
	failures, err := strictInt(lookup, EnvSegmentScopeFailures, DefaultSegmentScopeFailures, minSegmentScopeFailures, maxSegmentScopeFailures)
	if err != nil {
		return FaultPolicy{}, err
	}
	authMax, err := strictDuration(lookup, EnvAuthMaxBackoff, DefaultAuthMaxBackoff, minAuthMaxBackoff, maxFaultHold)
	if err != nil {
		return FaultPolicy{}, err
	}
	retryAfterMax, err := strictDuration(lookup, EnvRetryAfterMax, DefaultRetryAfterMax, minRetryAfterMax, maxFaultHold)
	if err != nil {
		return FaultPolicy{}, err
	}
	// -1 marks an unset count; the range check applies only to a set one.
	resends, err := strictInt(lookup, EnvUnconfirmedResends, -1, 0, maxUnconfirmedResends)
	if err != nil {
		return FaultPolicy{}, err
	}
	var unconfirmed *int
	if resends >= 0 {
		unconfirmed = &resends
	}
	return FaultPolicy{
		CreatesPerMinute:     creates,
		CreateBurst:          burst,
		SegmentScopeFailures: failures,
		AuthMaxBackoff:       authMax,
		RetryAfterMax:        retryAfterMax,
		UnconfirmedResends:   unconfirmed,
	}, nil
}

// strictByteSize parses a whole-bytes setting from minimum to maximum, such
// as 536870912 for 512 MiB. Unset is zero, so the caller can tell it apart
// from a set value. A unit suffix or any other bad value is an error and
// never a fallback.
func strictByteSize(lookup LookupFunc, name string, minimum, maximum int64) (int64, error) {
	value := strings.TrimSpace(valueOrDefault(lookup, name, ""))
	if value == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < minimum || parsed > maximum {
		return 0, fmt.Errorf("%s must be whole bytes, with no unit, from %d to %d", name, minimum, maximum)
	}
	return parsed, nil
}

// spoolBudgetValue reads the spool budget. Unset or 0 is no budget. Any
// other value out of range, or with a unit, is a startup error that names
// the setting, never a fallback.
func spoolBudgetValue(lookup LookupFunc) (int64, error) {
	value := strings.TrimSpace(valueOrDefault(lookup, EnvSpoolBudgetBytes, ""))
	if value == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed != 0 && (parsed < minSpoolBudgetBytes || parsed > maxSpoolBudgetBytes) {
		return 0, fmt.Errorf("%s must be 0 for no budget, or whole bytes with no unit from %d to %d", EnvSpoolBudgetBytes, minSpoolBudgetBytes, maxSpoolBudgetBytes)
	}
	return parsed, nil
}
