// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package diskguard

import (
	"errors"
	"fmt"
	"time"
)

const (
	// DefaultMinFreeBytes leaves room for Dynamo to finish its active
	// segment while the uploader stops preparing.
	DefaultMinFreeBytes = int64(512) << 20
	DefaultPrepareAhead = 16
	// Deletion is a near-full backstop: an alert well below the high
	// watermark and a runbook are the main path.
	DefaultHighWatermark         = 0.85
	DefaultLowWatermark          = 0.75
	DefaultQuarantineMaxFraction = 0.2
	DefaultQuarantineMaxAge      = 7 * 24 * time.Hour
)

// Policy is what the guard deletes when the spool is above the high
// watermark. The quarantine age and share limits delete entries under every
// policy. Every deleted item is counted as loss.
type Policy string

const (
	// PolicyQuarantineThenOldest deletes quarantined entries first, then the
	// oldest closed segments.
	PolicyQuarantineThenOldest Policy = "quarantine-then-oldest"
	// PolicyOldest deletes the oldest closed segments and leaves quarantine
	// to its own limits.
	PolicyOldest Policy = "oldest"
	// PolicyNone deletes nothing above the high watermark; preparing pauses
	// until space frees up. The quarantine limits still apply.
	PolicyNone Policy = "none"
)

// Config bounds spool use.
type Config struct {
	// MinFreeBytes is the least free space preparing leaves. Before a
	// transport is written, free space must be at least the larger of twice
	// the transport estimate and this floor.
	MinFreeBytes int64
	// PrepareAhead bounds the prepared segments waiting for a batch. It must
	// be at least the batch size, so a full batch can form.
	PrepareAhead int
	// HighWatermark is the used fraction of the spool's filesystem above
	// which preparing pauses and Policy applies. The whole filesystem
	// counts, so the spool needs a volume of its own (see the package doc).
	HighWatermark float64
	// LowWatermark is the used fraction Policy frees the spool down to once
	// it is above HighWatermark. It must be above 0 and below HighWatermark.
	LowWatermark float64
	Policy       Policy
	// QuarantineMaxFraction bounds quarantine to a fraction of the spool.
	QuarantineMaxFraction float64
	// QuarantineMaxAge bounds how long a quarantined entry is kept.
	QuarantineMaxAge time.Duration
}

// DefaultConfig returns the proposed defaults.
func DefaultConfig() Config {
	return Config{
		MinFreeBytes:          DefaultMinFreeBytes,
		PrepareAhead:          DefaultPrepareAhead,
		HighWatermark:         DefaultHighWatermark,
		LowWatermark:          DefaultLowWatermark,
		Policy:                PolicyQuarantineThenOldest,
		QuarantineMaxFraction: DefaultQuarantineMaxFraction,
		QuarantineMaxAge:      DefaultQuarantineMaxAge,
	}
}

// Validate rejects invalid or unknown values.
func (c Config) Validate() error {
	var errs []error
	if c.MinFreeBytes < 0 {
		errs = append(errs, fmt.Errorf("disk guard minimum free bytes must not be negative, got %d", c.MinFreeBytes))
	}
	if c.PrepareAhead < 1 {
		errs = append(errs, fmt.Errorf("disk guard prepare-ahead must be at least 1, got %d", c.PrepareAhead))
	}
	if !(c.HighWatermark > 0 && c.HighWatermark < 1) {
		errs = append(errs, fmt.Errorf("disk guard high watermark must be between 0 and 1, got %v", c.HighWatermark))
	}
	if !(c.LowWatermark > 0 && c.LowWatermark < c.HighWatermark) {
		errs = append(errs, fmt.Errorf("disk guard low watermark must be above 0 and below the high watermark %v, got %v", c.HighWatermark, c.LowWatermark))
	}
	switch c.Policy {
	case PolicyQuarantineThenOldest, PolicyOldest, PolicyNone:
	default:
		errs = append(errs, fmt.Errorf("disk guard policy must be %q, %q or %q, got %q", PolicyQuarantineThenOldest, PolicyOldest, PolicyNone, c.Policy))
	}
	if !(c.QuarantineMaxFraction > 0 && c.QuarantineMaxFraction <= 1) {
		errs = append(errs, fmt.Errorf("quarantine maximum fraction must be above 0 and at most 1, got %v", c.QuarantineMaxFraction))
	}
	if c.QuarantineMaxAge <= 0 {
		errs = append(errs, fmt.Errorf("quarantine maximum age must be positive, got %s", c.QuarantineMaxAge))
	}
	return errors.Join(errs...)
}

// Resolve raises PrepareAhead to batchSegments when it is lower, so a full
// batch can form, and returns a warning for the change.
func (c Config) Resolve(batchSegments int) (Config, []string) {
	if c.PrepareAhead >= batchSegments {
		return c, nil
	}
	warning := fmt.Sprintf("prepare-ahead %d is below the batch size, using %d", c.PrepareAhead, batchSegments)
	c.PrepareAhead = batchSegments
	return c, []string{warning}
}
