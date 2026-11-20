// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry/otlp"
)

// TestMainStaysUpWithoutStartingOnAnyInvalidOrUnknownSetting checks that the
// process is strict without exiting: a value the configuration would once
// have replaced by a default, or a misspelled setting name, stops startup,
// and the process stays up retrying, so as a sidecar it never makes its pod
// unready. SIGTERM still ends it at once, with a non-zero code.
func TestMainStaysUpWithoutStartingOnAnyInvalidOrUnknownSetting(t *testing.T) {
	tests := map[string]struct {
		env   []string
		named string
	}{
		"value that used to fall back":      {env: []string{config.EnvScanInterval + "=soon"}, named: config.EnvScanInterval},
		"misspelled setting":                {env: []string{"REQUEST_TRACE_UPLOADER_SCAN_INTERVL=30s"}, named: "REQUEST_TRACE_UPLOADER_SCAN_INTERVL"},
		"value out of range":                {env: []string{config.EnvDrainTimeout + "=-5"}, named: config.EnvDrainTimeout},
		"bare number for a duration":        {env: []string{config.EnvScanInterval + "=30"}, named: config.EnvScanInterval},
		"bare number for a strict duration": {env: []string{config.EnvDrainTimeout + "=60"}, named: config.EnvDrainTimeout},
		"bare zero for a duration":          {env: []string{config.EnvDrainTimeout + "=0"}, named: config.EnvDrainTimeout},
		"metrics endpoint without protocol": {
			env:   []string{otlp.EnvMetricsEndpoint + "=http://127.0.0.1:4318/v1/metrics"},
			named: otlp.EnvMetricsProtocol,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			// A readable secrets file, so only the configuration can stop
			// startup.
			secrets := filepath.Join(t.TempDir(), "secrets.json")
			if err := os.WriteFile(secrets, []byte("{}"), 0o600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^$")
			cmd.Env = append([]string{
				runMainEnv + "=1",
				config.EnvSourceDir + "=" + t.TempDir(),
				config.EnvBackend + "=" + string(config.BackendDebug),
				config.EnvHealthAddr + "=127.0.0.1:0",
				config.EnvSecretsFile + "=" + secrets,
			}, tt.env...)
			stderr := &syncBuffer{}
			cmd.Stderr = stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			exited := make(chan error, 1)
			go func() { exited <- cmd.Wait() }()
			deadline := time.Now().Add(5 * time.Second)
			for !strings.Contains(stderr.String(), "did not start; retrying") {
				select {
				case err := <-exited:
					t.Fatalf("process exited (%v) on a bad setting, want it to stay up\n%s", err, stderr.String())
				default:
				}
				if time.Now().After(deadline) {
					_ = cmd.Process.Kill()
					t.Fatalf("no retry logged in 5 s\n%s", stderr.String())
				}
				time.Sleep(10 * time.Millisecond)
			}
			if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-exited:
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.ExitCode() == 0 {
					t.Fatalf("process error = %v after SIGTERM, want a non-zero exit\n%s", err, stderr.String())
				}
			case <-time.After(5 * time.Second):
				_ = cmd.Process.Kill()
				t.Fatalf("process still running 5 s after SIGTERM\n%s", stderr.String())
			}
			if !strings.Contains(stderr.String(), tt.named) {
				t.Fatalf("stderr = %s, want the setting %s named", stderr.String(), tt.named)
			}
			if strings.Contains(stderr.String(), "request trace uploader ready") {
				t.Fatalf("stderr = %s, want no start", stderr.String())
			}
		})
	}
}

// syncBuffer is a bytes.Buffer the process's stderr copier may write while
// the test reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
