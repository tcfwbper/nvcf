// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend/backendtest"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/service"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/uploadertest"
)

// logSafeFake is a backend that declares its job ids safe to log.
type logSafeFake struct{ *backendtest.Fake }

func (logSafeFake) LogSafeReceipts() bool { return true }

// jobsLogged returns the job attribute of the submitted and confirmed lines.
func jobsLogged(t *testing.T, logs string) (submitted, confirmed string) {
	t.Helper()
	for _, line := range strings.Split(strings.TrimSpace(logs), "\n") {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		job, _ := entry["job"].(string)
		switch entry["msg"] {
		case "submitted request trace batch":
			submitted = job
		case "request trace batch confirmed":
			confirmed = job
		}
	}
	return submitted, confirmed
}

// The submit and confirm log lines carry the backend's job id, so an
// operator can follow a missing hour from the uploader into the backend
// without reading the journal. Only a backend that declares its ids safe to
// log gets them logged.
func TestSubmitAndConfirmLogTheBackendJobID(t *testing.T) {
	spool, closed := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
	logs := captureLogs(t)
	fake := backendtest.NewFake()
	uploader := newUploader(t, spool, logSafeFake{fake}, service.Options{}, nil)
	passUntil(t, uploader, func() bool { return gone(closed[0]) })
	if err := uploader.Close(); err != nil {
		t.Fatal(err)
	}
	jobs := fake.Jobs()
	if len(jobs) != 1 {
		t.Fatalf("jobs = %d, want 1", len(jobs))
	}
	submitted, confirmed := jobsLogged(t, logs.String())
	if submitted != jobs[0].ID || confirmed != jobs[0].ID {
		t.Fatalf("job on submit = %q, on confirm = %q; want the backend's id %q on both\n%s", submitted, confirmed, jobs[0].ID, logs.String())
	}
}

// A backend whose ids may carry configuration or paths, such as an object
// key, does not get them logged.
func TestAJobIDIsNotLoggedUnlessTheBackendSaysItIsSafe(t *testing.T) {
	spool, closed := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
	logs := captureLogs(t)
	fake := backendtest.NewFake()
	uploader := newUploader(t, spool, fake, service.Options{}, nil)
	passUntil(t, uploader, func() bool { return gone(closed[0]) })
	if err := uploader.Close(); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), `"job":`) {
		t.Fatalf("log carries a job key for a backend that does not declare its ids safe\n%s", logs.String())
	}
	if id := fake.Jobs()[0].ID; strings.Contains(logs.String(), id) {
		t.Fatalf("log carries the job id %q\n%s", id, logs.String())
	}
}
