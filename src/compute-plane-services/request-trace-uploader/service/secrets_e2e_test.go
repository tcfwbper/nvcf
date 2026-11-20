// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service_test

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend/backendtest"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/service"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/uploadertest"
)

// A missing secrets file no longer stops startup: the uploader becomes
// ready and logs a warning that names no path. Whether uploads wait is the
// backend's: this fake ignores the file, so its segment still goes out.
func TestAMissingSecretsFileDoesNotStopStartup(t *testing.T) {
	spool, closed := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
	logs := &uptimeLogs{}
	captureInto(t, logs)
	listener, base := listen(t)
	missing := filepath.Join(t.TempDir(), "not-mounted-yet.json")
	uploader := newUploader(t, spool, backendtest.NewFake(), service.Options{Listener: listener}, map[string]string{config.EnvSecretsFile: missing})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- uploader.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Run() error = %v", err)
		}
	})

	waitFor(t, "readiness without the secrets file", func() bool { return statusOf(base+"/readyz") == http.StatusOK })
	waitFor(t, "the closed segment to be handled", func() bool { return gone(closed[0]) })
	if !strings.Contains(logs.String(), "uploader secrets file is not readable") {
		t.Fatalf("log = %s, want a warning that the secrets file is not readable", logs.String())
	}
	if strings.Contains(logs.String(), "not-mounted-yet") {
		t.Fatalf("log = %s, names the secrets file path", logs.String())
	}
}

// The point of starting without the file: while a backend refuses every
// call for want of credentials, the spool bounds still run, so the spool
// cannot fill while the frontend keeps writing to it.
func TestWithoutTheSecretsFileTheSpoolIsStillBoundedWhileUploadsWait(t *testing.T) {
	spool, closed := spoolWith(t,
		[]string{uploadertest.RequestEnd("a", 1)},
		[]string{uploadertest.RequestEnd("b", 2)},
	)
	volume := &segmentVolume{dir: spool.Dir}
	// The three segments fill 95% of the volume, above the 85% high
	// watermark; removing the oldest brings it under the 75% low watermark.
	volume.total.Store(volume.segmentBytes() * 20 / 19)
	metrics, err := telemetry.NewMemory("fake")
	if err != nil {
		t.Fatal(err)
	}
	fake := backendtest.NewFake()
	for range 8 {
		fake.Script(backendtest.SubmitStep{Outcome: backendtest.Refuse, Err: &backend.Fault{Class: backend.ClassRetryable, Scope: backend.ScopeBackend, Reason: backend.ReasonAuth}})
	}
	missing := filepath.Join(t.TempDir(), "not-mounted-yet.json")
	uploader := newUploader(t, spool, fake, service.Options{Metrics: metrics, Volume: volume}, map[string]string{config.EnvSecretsFile: missing})

	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatalf("Pass() without the secrets file error = %v, want the pass to run", err)
	}
	if err := uploader.Close(); err != nil {
		t.Fatal(err)
	}
	if !gone(closed[0]) {
		t.Fatal("the oldest closed segment was kept above the high watermark while uploads waited")
	}
	if got, _ := metrics.Value(telemetry.SegmentsEvicted, nil); got != 1 {
		t.Fatalf("segments_evicted = %v, want the oldest evicted and counted", got)
	}
	if gone(closed[1]) {
		t.Fatal("the newer segment left the spool although every upload was refused")
	}
}
