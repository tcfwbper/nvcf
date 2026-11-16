// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend/backendtest"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/format"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/segment"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/transform"
)

// A discovered segment can be swapped before it is fingerprinted. The first
// read must refuse what is not a regular file, without following a symlink or
// waiting on a pipe.
func TestFingerprintRefusesWhatIsNotARegularFile(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(target, []byte("not ours"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, "request-trace.000001.jsonl.gz")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	fifo := filepath.Join(directory, "request-trace.000002.jsonl.gz")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Skipf("mkfifo: %v", err)
	}
	u := &Uploader{fingerprints: map[fileIdentity]string{}}
	for _, path := range []string{link, fifo} {
		done := make(chan error, 1)
		go func() {
			_, err := u.fingerprint(context.Background(), segment.Segment{Path: path})
			done <- err
		}()
		select {
		case err := <-done:
			if err == nil {
				t.Errorf("fingerprint(%s) error = nil, want a refusal", filepath.Base(path))
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("fingerprint(%s) blocked", filepath.Base(path))
		}
	}
}

// A transport prepared under an older redaction version has a different
// record policy, so it is prepared again from its source rather than sent.
// With redaction off the version does not matter.
func TestRecordPolicyFollowsTheRedactionVersion(t *testing.T) {
	factory, err := format.For(backendtest.NewFake())
	if err != nil {
		t.Fatal(err)
	}
	real := redactionVersion
	t.Cleanup(func() { redactionVersion = real })
	policy := func(redact bool, version string) string {
		t.Helper()
		redactionVersion = version
		cfg := transform.DefaultConfig()
		cfg.Redact = redact
		p, err := recordPolicy(cfg, factory)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	if policy(true, real) == policy(true, "v2") {
		t.Fatal("a transport prepared under redaction v2 has the current record policy, so it would be sent unchanged")
	}
	if policy(false, real) != policy(false, "v2") {
		t.Fatal("with redaction off, the redaction version changed the record policy")
	}
}

// With redaction off, the record policy is the one written before it named
// the redaction version, so upgrading does not prepare every unsent
// transport again.
func TestRecordPolicyWithRedactionOffIsUnchangedByTheUpgrade(t *testing.T) {
	factory, err := format.For(backendtest.NewFake())
	if err != nil {
		t.Fatal(err)
	}
	cfg := transform.DefaultConfig()
	cfg.Redact = false
	got, err := recordPolicy(cfg, factory)
	if err != nil {
		t.Fatal(err)
	}
	writerVersion := ""
	if versioned, ok := factory.(formatVersioner); ok {
		writerVersion = versioned.FormatVersion()
	}
	// The fields and order of the policy before this release.
	raw, err := json.Marshal(struct {
		Transform     transform.Config
		RecordSchema  int
		Format        backend.Format
		WriterVersion string
	}{cfg, recordSchemaVersion, factory.Format(), writerVersion})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if want := hex.EncodeToString(sum[:16]); got != want {
		t.Fatalf("recordPolicy = %s, want %s, the policy before the redaction version", got, want)
	}
}

// A drain is bounded by its context, so hashing a large segment stops once
// the context is done, and the unfinished hash is not kept.
func TestFingerprintStopsWhenTheContextIsDone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "request-trace.000001.jsonl.gz")
	if err := os.WriteFile(path, make([]byte, 1<<20), 0o600); err != nil {
		t.Fatal(err)
	}
	u := &Uploader{fingerprints: map[fileIdentity]string{}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := u.fingerprint(ctx, segment.Segment{Path: path, Size: 1 << 20}); !errors.Is(err, context.Canceled) {
		t.Fatalf("fingerprint() error = %v, want context.Canceled", err)
	}
	if len(u.fingerprints) != 0 {
		t.Fatal("an unfinished hash was kept")
	}
}
