// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package config

const (
	// EnvPrepareWorkers is how many segments are prepared at once. One keeps
	// the uploader to one core for preparing.
	EnvPrepareWorkers = "REQUEST_TRACE_UPLOADER_PREPARE_WORKERS"
	// EnvPrepareMemoryBytes is the memory, in bytes, that the long lines
	// held by concurrent prepares share. A prepare whose line does not fit
	// waits for another to finish its line; the line that started first
	// never waits, so one line larger than the budget is still read.
	EnvPrepareMemoryBytes = "REQUEST_TRACE_UPLOADER_PREPARE_MEMORY_BYTES"

	DefaultPrepareWorkers     = 1
	DefaultPrepareMemoryBytes = int64(1) << 30

	maxPrepareWorkers     = 16
	minPrepareMemoryBytes = int64(64) << 20
	maxPrepareMemoryBytes = int64(64) << 30
)

// PreparePolicy bounds preparing segments in parallel.
type PreparePolicy struct {
	// Workers is how many segments are prepared at once.
	Workers int
	// MemoryBytes is the memory long lines share across workers.
	MemoryBytes int64
}

func preparePolicyValue(lookup LookupFunc) (PreparePolicy, error) {
	workers, err := strictInt(lookup, EnvPrepareWorkers, DefaultPrepareWorkers, 1, maxPrepareWorkers)
	if err != nil {
		return PreparePolicy{}, err
	}
	memory, err := strictBytes(lookup, EnvPrepareMemoryBytes, DefaultPrepareMemoryBytes, minPrepareMemoryBytes, maxPrepareMemoryBytes)
	if err != nil {
		return PreparePolicy{}, err
	}
	return PreparePolicy{Workers: workers, MemoryBytes: memory}, nil
}
