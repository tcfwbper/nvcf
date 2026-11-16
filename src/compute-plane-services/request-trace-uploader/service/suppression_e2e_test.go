// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend/backendtest"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/format"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/journal"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/service"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/transform"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/uploadertest"
)

// ncaHeaders is the NCA id header list a deployment sets, in the order the
// earlier shell exporter read them.
const ncaHeaders = "nvcf-ncaid,nvcf-nca-id"

// The suppressed line and its stub are the first case of
// transform/suppress/testdata/python.json, which the earlier shell
// exporter's Python step produced byte for byte.
const (
	suppressedLine = `{"timestamp":101,"event":{"schema":"dynamo.request.trace.v1","event_type":"request_payload","event_time_unix_ms":1777312801000,"event_source":"frontend","payload":{"request_id":"r1","endpoint":"chat","model":"m","request":{"messages":[{"role":"user","content":"secret"}]},"response":{"choices":[]},"http_request_headers":{"nvcf-ncaid":"nca-acme-nca"},"payload_complete":true}}}`
	shellStub      = `{"timestamp":101,"event":{"schema":"dynamo.request.trace.v1","event_type":"request_payload","event_time_unix_ms":1777312801000,"event_source":"frontend","payload":{"request_id":"r1","endpoint":"chat","model":"m","payload_complete":false,"payload_drop_reason":"nca_id","payload_suppressed":true,"suppression_reason":"nca_id","nca_id":"acme","http_request_headers":{"nvcf-ncaid":"acme"}}}}`
)

func TestChartSuppressionEnvExportsTheShellStub(t *testing.T) {
	keptPayload := uploadertest.RequestPayload("r2", 1777312802000, map[string]string{"nvcf-ncaid": "nca-beta-nca"})
	keptEnd := uploadertest.RequestEnd("r1", 1777312803000)
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	closed := spool.Roll(uploadertest.NewSegment().Member(suppressedLine, keptPayload, keptEnd).Bytes())
	active := spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("r3", 1)).Bytes())

	// The chart renders suppressedNcaIds with `| quote`, so the container
	// sees the comma-separated list as it was written in the values.
	cfg := loadEnv(t, spoolEnv(t, spool.Dir, map[string]string{
		config.EnvSuppressNCAIDs:     "nca-acme-nca,other-org",
		config.EnvSuppressNCAHeaders: ncaHeaders,
	}))
	exporter := newCapture(t, backendtest.NewFake())
	metrics, err := telemetry.NewMemory("fake")
	if err != nil {
		t.Fatal(err)
	}
	uploader, err := service.NewUploader(cfg, exporter, service.Options{Metrics: metrics})
	if err != nil {
		t.Fatalf("NewUploader() error = %v", err)
	}

	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatalf("Pass() error = %v", err)
	}

	exported := exporter.exported()
	if want := []string{shellStub, keptPayload, keptEnd}; !slices.Equal(exported, want) {
		t.Fatalf("exported =\n%s\nwant\n%s", strings.Join(exported, "\n"), strings.Join(want, "\n"))
	}
	if strings.Contains(strings.Join(exported, "\n"), "secret") {
		t.Fatal("the suppressed request body was exported")
	}
	for name, labels := range map[string]telemetry.Labels{
		telemetry.RecordsSuppressed:        {"rule": config.NCAIDRuleName, "action": "strip_payload"},
		telemetry.SuppressionHeaderPresent: {"rule": config.NCAIDRuleName},
	} {
		want := map[string]float64{telemetry.RecordsSuppressed: 1, telemetry.SuppressionHeaderPresent: 2}[name]
		if got, _ := metrics.Value(name, labels); got != want {
			t.Errorf("%s%v = %v, want %v", name, labels, got, want)
		}
	}
	if got, _ := metrics.Value(telemetry.RecordsEvaluated, telemetry.Labels{"record_type": "request_payload"}); got != 2 {
		t.Errorf("records_evaluated = %v, want 2", got)
	}
	if _, err := os.Stat(closed); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("closed segment after a confirmed export: %v, want removed", err)
	}
	if _, err := os.Stat(active); err != nil {
		t.Errorf("active segment: %v, want it left for the writer", err)
	}
}

func TestNCAHeaderSettingDecidesWhichHeadersAreReadInOrder(t *testing.T) {
	// The first header carries another org, so the second is never read.
	firstWins := uploadertest.RequestPayload("r2", 1777312802000, map[string]string{"x-org-id": "other", "nvcf-ncaid": "acme"})
	// nvcf-nca-id is not in the configured list.
	unlisted := uploadertest.RequestPayload("r3", 1777312802000, map[string]string{"nvcf-nca-id": "acme"})
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	spool.Roll(uploadertest.NewSegment().Member(suppressedLine, firstWins, unlisted).Bytes())
	spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("r4", 1)).Bytes())

	cfg := loadEnv(t, spoolEnv(t, spool.Dir, map[string]string{
		config.EnvSuppressNCAIDs:     "acme",
		config.EnvSuppressNCAHeaders: "x-org-id,nvcf-ncaid",
	}))
	exporter := newCapture(t, backendtest.NewFake())
	metrics, err := telemetry.NewMemory("fake")
	if err != nil {
		t.Fatal(err)
	}
	uploader, err := service.NewUploader(cfg, exporter, service.Options{Metrics: metrics})
	if err != nil {
		t.Fatalf("NewUploader() error = %v", err)
	}
	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatalf("Pass() error = %v", err)
	}

	// suppressedLine carries only nvcf-ncaid, so the second header is read.
	if exported, want := exporter.exported(), []string{shellStub, firstWins, unlisted}; !slices.Equal(exported, want) {
		t.Fatalf("exported =\n%s\nwant\n%s", strings.Join(exported, "\n"), strings.Join(want, "\n"))
	}
	if got, _ := metrics.Value(telemetry.SuppressionHeaderPresent, telemetry.Labels{"rule": config.NCAIDRuleName}); got != 2 {
		t.Errorf("suppression_header_present = %v, want 2", got)
	}
}

// A stripPayload rule from the rules setting matches on a header that is not
// the NCA id, so its stub must not present the matched value as one.
func TestCustomStripRuleStubCarriesNoIdentifier(t *testing.T) {
	line := uploadertest.RequestPayload("r1", 1777312801000, map[string]string{"x-user-id": "alice@example.com", "nvcf-ncaid": "nca-beta-nca"})
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	spool.Roll(uploadertest.NewSegment().Member(line).Bytes())
	spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("r2", 1)).Bytes())

	cfg := loadEnv(t, spoolEnv(t, spool.Dir, map[string]string{
		config.EnvSuppressionRules: `[{"name":"users","headers":["x-user-id"],"values":["alice@example.com"],"action":"stripPayload"}]`,
	}))
	exporter := newCapture(t, backendtest.NewFake())
	uploader, err := service.NewUploader(cfg, exporter, service.Options{})
	if err != nil {
		t.Fatalf("NewUploader() error = %v", err)
	}
	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatalf("Pass() error = %v", err)
	}

	exported := strings.Join(exporter.exported(), "\n")
	for _, leak := range []string{"alice@example.com", `"nca_id"`, "nvcf-ncaid", "beta"} {
		if strings.Contains(exported, leak) {
			t.Errorf("exported stub contains %s:\n%s", leak, exported)
		}
	}
	if !strings.Contains(exported, `"suppression_reason":"users"`) {
		t.Errorf("exported = %s, want the rule name as the suppression reason", exported)
	}
}

func TestSuppressionCountersArePresetAndCarryOnlyRuleNames(t *testing.T) {
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	cfg := loadEnv(t, spoolEnv(t, spool.Dir, map[string]string{
		config.EnvSuppressNCAIDs:     "acme",
		config.EnvSuppressNCAHeaders: ncaHeaders,
		config.EnvSuppressionRules:   `[{"name":"tenants","headers":["x-tenant"],"values":["t1"],"action":"dropRecord"}]`,
	}))
	metrics, err := telemetry.NewMemory("fake")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.NewUploader(cfg, backendtest.NewFake(), service.Options{Metrics: metrics}); err != nil {
		t.Fatalf("NewUploader() error = %v", err)
	}

	for _, want := range []struct {
		name   string
		labels telemetry.Labels
	}{
		{telemetry.RecordsSuppressed, telemetry.Labels{"rule": "nca_id", "action": "strip_payload"}},
		{telemetry.RecordsSuppressed, telemetry.Labels{"rule": "tenants", "action": "drop_record"}},
		{telemetry.SuppressionHeaderPresent, telemetry.Labels{"rule": "nca_id"}},
		{telemetry.SuppressionHeaderPresent, telemetry.Labels{"rule": "tenants"}},
	} {
		if value, ok := metrics.Value(want.name, want.labels); !ok || value != 0 {
			t.Errorf("%s%v = %v, %v; want a zero series before any record", want.name, want.labels, value, ok)
		}
	}
	for _, name := range []string{telemetry.RecordsSuppressed, telemetry.SuppressionHeaderPresent} {
		for _, labels := range metrics.Series(name) {
			for key, value := range labels {
				if strings.Contains(value, "ncaid") || strings.Contains(value, "tenant-") || value == "acme" || value == "t1" || value == "x-tenant" {
					t.Errorf("%s label %s=%q exposes a header name or value", name, key, value)
				}
			}
		}
	}
}

func TestWarnsOnceWhenNoSuppressionHeaderIsSeen(t *testing.T) {
	logs := captureLogs(t)
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	cfg := loadEnv(t, spoolEnv(t, spool.Dir, map[string]string{config.EnvSuppressNCAIDs: "acme", config.EnvSuppressNCAHeaders: ncaHeaders}))
	exporter := newCapture(t, backendtest.NewFake())
	uploader, err := service.NewUploader(cfg, exporter, service.Options{HeaderCheckRecords: 3})
	if err != nil {
		t.Fatalf("NewUploader() error = %v", err)
	}
	headerless := func(id string) string { return uploadertest.RequestPayload(id, 1, nil) }
	const warning = "no payload record carried a suppression header"

	spool.Roll(uploadertest.NewSegment().Member(headerless("a"), headerless("b")).Bytes())
	spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("x", 1)).Bytes())
	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), warning) {
		t.Fatalf("warned after 2 payload records, want a warning only after 3:\n%s", logs)
	}

	for range 2 {
		spool.Roll(uploadertest.NewSegment().Member(headerless("c"), headerless("d")).Bytes())
		if err := uploader.Pass(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if got := strings.Count(logs.String(), warning); got != 1 {
		t.Fatalf("warnings = %d, want exactly 1:\n%s", got, logs)
	}
	// The condition only warns: every record of the closed segments (a, b,
	// x, c, d) is still exported.
	if got := len(exporter.exported()); got != 5 {
		t.Fatalf("exported %d records, want all 5", got)
	}
}

func TestNoHeaderWarningOnceAHeaderIsSeen(t *testing.T) {
	logs := captureLogs(t)
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	cfg := loadEnv(t, spoolEnv(t, spool.Dir, map[string]string{config.EnvSuppressNCAIDs: "acme", config.EnvSuppressNCAHeaders: ncaHeaders}))
	uploader, err := service.NewUploader(cfg, backendtest.NewFake(), service.Options{HeaderCheckRecords: 2})
	if err != nil {
		t.Fatalf("NewUploader() error = %v", err)
	}
	spool.Roll(uploadertest.NewSegment().Member(
		uploadertest.RequestPayload("a", 1, map[string]string{"nvcf-ncaid": "other"}),
		uploadertest.RequestPayload("b", 1, nil),
		uploadertest.RequestPayload("c", 1, nil),
	).Bytes())
	spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("x", 1)).Bytes())
	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "suppression header") {
		t.Fatalf("warned although a header was seen:\n%s", logs)
	}
}

func TestATransportPreparedUnderAnotherPolicyIsPreparedAgainAfterARestart(t *testing.T) {
	if uploadertest.CrashChild() {
		t.Skip("runs only in the parent")
	}
	landing := t.TempDir()
	spool, closed := spoolWith(t, []string{suppressedLine})
	// The child prepares with suppression off and is killed before it
	// submits.
	result := uploadertest.RunCrash(t, "TestCrashChildRunsThePipeline", service.PointPrepared,
		envCrashSpool+"="+spool.Dir, envCrashLanding+"="+landing)
	if !result.Killed || result.Point != service.PointPrepared {
		t.Fatalf("child was not killed at %s:\n%s", service.PointPrepared, result.Output)
	}

	logs := captureLogs(t)
	restarted := newUploader(t, spool, dirBackend{root: landing}, service.Options{}, map[string]string{config.EnvSuppressNCAIDs: "nca-acme-nca", config.EnvSuppressNCAHeaders: ncaHeaders})
	passUntil(t, restarted, func() bool { return gone(closed[0]) })
	files, err := filepath.Glob(filepath.Join(landing, "*", "*.jsonl.gz"))
	if err != nil {
		t.Fatal(err)
	}
	var exported []string
	for _, file := range files {
		exported = append(exported, readGzipLines(t, file)...)
	}
	joined := strings.Join(exported, "\n")
	if strings.Contains(joined, "secret") || !strings.Contains(joined, `"payload_suppressed":true`) {
		t.Fatalf("exported = %s, want the record suppressed under the policy now configured", joined)
	}
	if strings.Contains(logs.String(), "nca-acme-nca") {
		t.Fatalf("logs carry a configured suppression value:\n%s", logs)
	}
}

// refusingBackend refuses every submit in a way that shows nothing was
// processed, as a 503 does.
type refusingBackend struct{ dirBackend }

func (refusingBackend) Submit(context.Context, backend.SubmitRequest) (string, error) {
	return "", &backend.Fault{Class: backend.ClassRetryable, Scope: backend.ScopeUnknown, Reason: backend.ReasonUnavailable, Err: errors.New("unavailable")}
}

func TestARefusedBatchIsPreparedAgainWhenThePolicyChangesBeforeItsRetry(t *testing.T) {
	landing := t.TempDir()
	spool, closed := spoolWith(t, []string{suppressedLine})
	first := newUploader(t, spool, refusingBackend{dirBackend{root: landing}}, service.Options{}, nil)
	_ = first.Pass(context.Background())
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	batches := journalBatches(t, spool)
	if len(batches) != 1 || batches[0].State != journal.StateRetryWait {
		t.Fatalf("batches = %+v, want one refused batch waiting to retry", batches)
	}

	restarted := newUploader(t, spool, dirBackend{root: landing}, service.Options{}, map[string]string{config.EnvSuppressNCAIDs: "nca-acme-nca", config.EnvSuppressNCAHeaders: ncaHeaders})
	passUntil(t, restarted, func() bool { return gone(closed[0]) })
	files, err := filepath.Glob(filepath.Join(landing, "*", "*.jsonl.gz"))
	if err != nil {
		t.Fatal(err)
	}
	var exported []string
	for _, file := range files {
		exported = append(exported, readGzipLines(t, file)...)
	}
	joined := strings.Join(exported, "\n")
	if strings.Contains(joined, "secret") || !strings.Contains(joined, `"payload_suppressed":true`) {
		t.Fatalf("exported = %s, want the record suppressed under the policy now configured", joined)
	}
}

// dualBackend is a fake that writes every segment to two destinations,
// one gzip JSON lines transport each. It can refuse every submit to the
// secondary destination in a way that shows nothing was processed. When
// landing is set, it copies each file it is sent there, named by
// destination, so a test can read what went out.
type dualBackend struct {
	*backendtest.Fake
	refuseSecondary bool
	landing         string
	sent            *atomic.Int32
}

func newDualBackend(refuseSecondary bool, landing string) dualBackend {
	fake := backendtest.NewFake()
	caps := fake.Capabilities()
	caps.Destinations = []backend.Destination{"primary", "secondary"}
	fake.SetCapabilities(caps)
	return dualBackend{Fake: fake, refuseSecondary: refuseSecondary, landing: landing, sent: &atomic.Int32{}}
}

func (b dualBackend) Submit(ctx context.Context, request backend.SubmitRequest) (string, error) {
	if b.refuseSecondary && request.Destination == "secondary" {
		return refusingBackend{}.Submit(ctx, request)
	}
	if b.landing != "" {
		for _, file := range request.Transports() {
			name := fmt.Sprintf("%s-%d.jsonl.gz", request.Destination, b.sent.Add(1))
			if err := copyFile(file.Path, filepath.Join(b.landing, name)); err != nil {
				return "", err
			}
		}
	}
	return b.Fake.Submit(ctx, request)
}

func (dualBackend) Format() backend.Format { return backend.FormatGzipJSONL }

func (dualBackend) NewWriter(target format.Target) (format.Writer, error) {
	var lanes fanout
	for _, destination := range target.Destinations {
		one := target
		one.Destinations = []backend.Destination{destination}
		writer, err := format.GzipJSONL{}.NewWriter(one)
		if err != nil {
			return nil, errors.Join(err, lanes.Abort())
		}
		lanes = append(lanes, writer)
	}
	return lanes, nil
}

// fanout writes every record to each of its writers.
type fanout []format.Writer

func (f fanout) Write(r transform.Record) error {
	for _, w := range f {
		if err := w.Write(r); err != nil {
			return err
		}
	}
	return nil
}

func (f fanout) Close() error {
	var errs []error
	for _, w := range f {
		errs = append(errs, w.Close())
	}
	return errors.Join(errs...)
}

func (f fanout) Abort() error {
	var errs []error
	for _, w := range f {
		errs = append(errs, w.Abort())
	}
	return errors.Join(errs...)
}

func (f fanout) Outputs() []format.Output {
	var outputs []format.Output
	for _, w := range f {
		outputs = append(outputs, w.Outputs()...)
	}
	return outputs
}

// partlyConfirmed runs a two-destination uploader whose secondary refuses
// every submit, so the one closed segment, holding suppressedLine, ends with
// its primary destination confirmed and its secondary never sent. It
// returns the spool, the closed segment and the confirmed batch id.
func partlyConfirmed(t *testing.T) (*uploadertest.Spool, []string, string) {
	t.Helper()
	spool, closed := spoolWith(t, []string{suppressedLine})
	first := newUploader(t, spool, newDualBackend(true, ""), service.Options{}, nil)
	for range 3 {
		_ = first.Pass(context.Background())
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	var confirmed []string
	for _, b := range journalBatches(t, spool) {
		if b.Destination == "primary" && b.State == journal.StateConfirmed {
			confirmed = append(confirmed, b.ID)
		}
	}
	if len(confirmed) != 1 {
		t.Fatalf("confirmed primary batches = %v, want one", confirmed)
	}
	return spool, closed, confirmed[0]
}

// A segment with one destination confirmed and another never sent is
// prepared again as a whole when the record policy changes, so nothing goes
// out under the old policy. The confirmed destination is sent again and
// counted as a possible duplicate. This holds whether the confirmed batch
// file is still there or was unreadable and fenced; recovery counts the
// fenced batch as ambiguous too.
func TestAPartlyConfirmedSegmentIsPreparedAgainWhenThePolicyChanges(t *testing.T) {
	cases := []struct {
		name          string
		fence         bool
		wantAmbiguous float64
	}{
		{"batch kept", false, 1},
		{"batch fenced", true, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spool, closed, confirmed := partlyConfirmed(t)
			if tc.fence {
				batchFile := filepath.Join(spool.Dir, "request-trace-uploader-state", "batches", confirmed+".json")
				if err := os.WriteFile(batchFile, []byte("{"), 0o600); err != nil {
					t.Fatal(err)
				}
			}

			landing := t.TempDir()
			metrics, err := telemetry.NewMemory("fake")
			if err != nil {
				t.Fatal(err)
			}
			restarted := newUploader(t, spool, newDualBackend(false, landing), service.Options{Metrics: metrics}, map[string]string{config.EnvSuppressNCAIDs: "nca-acme-nca", config.EnvSuppressNCAHeaders: ncaHeaders})
			if err := restarted.Pass(context.Background()); err != nil {
				t.Fatalf("first Pass() after the restart error = %v", err)
			}
			passUntil(t, restarted, func() bool { return gone(closed[0]) })

			for _, destination := range []string{"secondary", "primary"} {
				files, err := filepath.Glob(filepath.Join(landing, destination+"-*.jsonl.gz"))
				if err != nil || len(files) != 1 {
					t.Fatalf("%s files sent after the restart = %v, %v; want one", destination, files, err)
				}
				joined := strings.Join(readGzipLines(t, files[0]), "\n")
				if strings.Contains(joined, "secret") || !strings.Contains(joined, `"payload_suppressed":true`) {
					t.Fatalf("%s sent %s, want the record suppressed under the policy now configured", destination, joined)
				}
			}
			if got, _ := metrics.Value(telemetry.SubmitsAmbiguous, nil); got != tc.wantAmbiguous {
				t.Fatalf("submits_ambiguous = %v, want %v", got, tc.wantAmbiguous)
			}
		})
	}
}

// When its source is gone, a partly confirmed segment cannot be prepared
// again, so its stale transports are kept in quarantine and never sent
// under the old policy.
func TestAPartlyConfirmedSegmentWhoseSourceIsGoneIsQuarantinedWhenThePolicyChanges(t *testing.T) {
	spool, closed, _ := partlyConfirmed(t)
	if err := os.Remove(closed[0]); err != nil {
		t.Fatal(err)
	}
	landing := t.TempDir()
	restarted := newUploader(t, spool, newDualBackend(false, landing), service.Options{}, map[string]string{config.EnvSuppressNCAIDs: "nca-acme-nca", config.EnvSuppressNCAHeaders: ncaHeaders})
	for range 2 {
		_ = restarted.Pass(context.Background())
	}
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}
	if sent, _ := filepath.Glob(filepath.Join(landing, "*")); len(sent) != 0 {
		t.Fatalf("files sent after the restart = %v, want none", sent)
	}
	if state := journalStates(t, spool)[filepath.Base(closed[0])]; state != journal.StateQuarantined {
		t.Fatalf("state = %q, want quarantined", state)
	}
}

// lostResponseBackend lands each job and then loses the response, so the
// uploader cannot tell whether the submit committed.
type lostResponseBackend struct{ dirBackend }

func (b lostResponseBackend) Submit(ctx context.Context, request backend.SubmitRequest) (string, error) {
	if _, err := b.dirBackend.Submit(ctx, request); err != nil {
		return "", err
	}
	return "", &backend.Fault{Class: backend.ClassUncertain, Scope: backend.ScopeBackend, Reason: backend.ReasonTimeout, Err: errors.New("response lost")}
}

func TestARetainedSubmitIsNeitherPreparedAgainNorResentWhenThePolicyChanges(t *testing.T) {
	landing := t.TempDir()
	spool, closed := spoolWith(t, []string{suppressedLine})
	first := newUploader(t, spool, lostResponseBackend{dirBackend{root: landing}}, service.Options{Faults: retainFaults()}, nil)
	_ = first.Pass(context.Background())
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	restarted := newUploader(t, spool, dirBackend{root: landing}, service.Options{Faults: retainFaults()}, map[string]string{config.EnvSuppressNCAIDs: "nca-acme-nca", config.EnvSuppressNCAHeaders: ncaHeaders})
	for range 3 {
		_ = restarted.Pass(context.Background())
	}
	jobs, err := filepath.Glob(filepath.Join(landing, "*", "landed"))
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || gone(closed[0]) {
		t.Fatalf("landed jobs = %d, source deleted = %t; want the retained submit left for an operator", len(jobs), gone(closed[0]))
	}
}

// A batch whose submits were only refused is released at startup when the
// record policy changed. If the journal refuses that release, startup fails
// and stays failed, so its transport, prepared under the old policy, is never
// sent, even once the journal takes writes again.
func TestAFailedStaleReleaseAtStartupSendsNothing(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	landing := t.TempDir()
	spool, closed := spoolWith(t, []string{suppressedLine})
	first := newUploader(t, spool, refusingBackend{dirBackend{root: landing}}, service.Options{}, nil)
	_ = first.Pass(context.Background())
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	batches := journalBatches(t, spool)
	if len(batches) != 1 || batches[0].State != journal.StateRetryWait || batches[0].MayHaveLanded {
		t.Fatalf("batches = %+v, want one refused batch waiting to retry", batches)
	}

	batchDir := filepath.Join(spool.Dir, "request-trace-uploader-state", "batches")
	if err := os.Chmod(batchDir, 0o500); err != nil {
		t.Fatal(err)
	}
	restarted := newUploader(t, spool, dirBackend{root: landing}, service.Options{}, map[string]string{config.EnvSuppressNCAIDs: "nca-acme-nca", config.EnvSuppressNCAHeaders: ncaHeaders})
	firstErr := restarted.Pass(context.Background())
	if err := os.Chmod(batchDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if firstErr == nil {
		t.Fatal("Pass() error = nil with the release refused, want startup to fail")
	}
	for range 3 {
		if err := restarted.Pass(context.Background()); err == nil {
			t.Fatal("Pass() error = nil after a failed startup, want it to stay failed")
		}
	}
	if sent, err := filepath.Glob(filepath.Join(landing, "*")); err != nil || len(sent) != 0 {
		t.Fatalf("sent = %v, %v; want nothing sent under the old policy", sent, err)
	}
	if gone(closed[0]) {
		t.Fatal("source deleted, want it kept for the next start")
	}
}

// lostPrimaryBackend is a two-destination fake whose primary submit lands
// and then loses its response, so it may have landed, while every secondary
// submit is refused, so nothing of it was sent.
type lostPrimaryBackend struct{ dualBackend }

func (b lostPrimaryBackend) Submit(ctx context.Context, request backend.SubmitRequest) (string, error) {
	if request.Destination == "secondary" {
		return refusingBackend{}.Submit(ctx, request)
	}
	if _, err := b.dualBackend.Submit(ctx, request); err != nil {
		return "", err
	}
	return "", &backend.Fault{Class: backend.ClassUncertain, Scope: backend.ScopeBackend, Reason: backend.ReasonTimeout, Err: errors.New("response lost")}
}

// A segment's primary is in a submit that may have landed and its secondary
// was never sent when the record policy changes. The primary's batch is
// retried as it is, but the secondary never goes out under the old policy:
// once the primary's batch is done, the segment is prepared again and the
// secondary is sent under the policy now configured.
func TestAnUnsentDestinationBesideASubmitThatMayHaveLandedWaitsForTheNewPolicy(t *testing.T) {
	spool, closed := spoolWith(t, []string{suppressedLine})
	first := newUploader(t, spool, lostPrimaryBackend{newDualBackend(false, "")}, service.Options{}, nil)
	_ = first.Pass(context.Background())
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	states := map[string]journal.State{}
	for _, b := range journalBatches(t, spool) {
		states[b.Destination] = b.State
	}
	if states["primary"] != journal.StateAmbiguous || states["secondary"] != journal.StateRetryWait {
		t.Fatalf("batch states = %v, want the primary ambiguous and the secondary refused", states)
	}

	landing := t.TempDir()
	restarted := newUploader(t, spool, newDualBackend(false, landing), service.Options{}, map[string]string{config.EnvSuppressNCAIDs: "nca-acme-nca", config.EnvSuppressNCAHeaders: ncaHeaders})
	passUntil(t, restarted, func() bool { return gone(closed[0]) })

	secondary, err := filepath.Glob(filepath.Join(landing, "secondary-*.jsonl.gz"))
	if err != nil || len(secondary) == 0 {
		t.Fatalf("secondary files sent = %v, %v; want the segment sent", secondary, err)
	}
	for _, file := range secondary {
		joined := strings.Join(readGzipLines(t, file), "\n")
		if strings.Contains(joined, "secret") || !strings.Contains(joined, `"payload_suppressed":true`) {
			t.Fatalf("secondary sent %s, want only the record suppressed under the policy now configured", joined)
		}
	}
}

// Each rule needs its own header: one rule's header being present says
// nothing about another rule that can never fire.
func TestWarnsForEachRuleWhoseHeaderIsNeverSeen(t *testing.T) {
	logs := captureLogs(t)
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	cfg := loadEnv(t, spoolEnv(t, spool.Dir, map[string]string{
		config.EnvSuppressNCAIDs:     "acme",
		config.EnvSuppressNCAHeaders: ncaHeaders,
		config.EnvSuppressionRules:   `[{"name":"tenants","headers":["x-tenant"],"values":["t1"],"action":"dropRecord"}]`,
	}))
	uploader, err := service.NewUploader(cfg, backendtest.NewFake(), service.Options{HeaderCheckRecords: 2})
	if err != nil {
		t.Fatalf("NewUploader() error = %v", err)
	}
	spool.Roll(uploadertest.NewSegment().Member(
		uploadertest.RequestPayload("a", 1, map[string]string{"nvcf-ncaid": "other"}),
		uploadertest.RequestPayload("b", 1, map[string]string{"nvcf-ncaid": "other"}),
	).Bytes())
	spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("x", 1)).Bytes())
	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	var warned []string
	for line := range strings.Lines(logs.String()) {
		if strings.Contains(line, "suppression header") {
			warned = append(warned, line)
		}
	}
	if len(warned) != 1 || !strings.Contains(warned[0], "tenants") || strings.Contains(warned[0], "nca_id") {
		t.Fatalf("warnings = %q, want one naming only the tenants rule", warned)
	}
}

// Suppression counts are recorded once per segment, when it reaches its
// final outcome, not again on every pass that retries it.
func TestSuppressionCountsAreRecordedOncePerSegment(t *testing.T) {
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	closed := spool.Roll(uploadertest.NewSegment().Member(suppressedLine).Bytes())
	spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("x", 1)).Bytes())
	fake := backendtest.NewFake()
	refused := backendtest.SubmitStep{Outcome: backendtest.Refuse}
	fake.Script(refused, refused)
	metrics, err := telemetry.NewMemory("fake")
	if err != nil {
		t.Fatal(err)
	}
	// Fast fault waits, so both retries happen within the test.
	uploader := newUploader(t, spool, fake, service.Options{Metrics: metrics}, map[string]string{config.EnvSuppressNCAIDs: "acme", config.EnvSuppressNCAHeaders: ncaHeaders})
	passUntil(t, uploader, func() bool { return gone(closed) })
	if got := fake.SubmitCalls(); got != 3 {
		t.Fatalf("submits = %d, want two refusals and a success", got)
	}
	if got, _ := metrics.Value(telemetry.RecordsSuppressed, telemetry.Labels{"rule": config.NCAIDRuleName, "action": "strip_payload"}); got != 1 {
		t.Fatalf("records_suppressed = %v after two refusals and a success, want 1", got)
	}
}

// With strict suppression on, a payload record whose rule header was not
// captured is exported only as a stub: the producer may have dropped the
// header of a suppressed customer.
func TestStrictSuppressionStripsPayloadsWhoseHeaderIsMissing(t *testing.T) {
	logs := captureLogs(t)
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestPayload("r1", 1, nil)).Bytes())
	spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestPayload("r2", 2, nil), uploadertest.RequestEnd("r2", 3)).Bytes())
	spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("active", 4)).Bytes())
	cfg := loadEnv(t, spoolEnv(t, spool.Dir, map[string]string{
		config.EnvSuppressNCAIDs:     "acme",
		config.EnvSuppressNCAHeaders: ncaHeaders,
		config.EnvSuppressionStrict:  "true",
	}))
	exporter := newCapture(t, backendtest.NewFake())
	metrics, err := telemetry.NewMemory("fake")
	if err != nil {
		t.Fatal(err)
	}
	uploader, err := service.NewUploader(cfg, exporter, service.Options{Metrics: metrics})
	if err != nil {
		t.Fatalf("NewUploader() error = %v", err)
	}
	unresolved := telemetry.Labels{"rule": config.NCAIDRuleName, "action": "strip_unresolved"}
	if _, ok := metrics.Value(telemetry.RecordsSuppressed, unresolved); !ok {
		t.Fatalf("records_suppressed%v is not preset", unresolved)
	}

	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatalf("Pass() error = %v", err)
	}

	exported := strings.Join(exporter.exported(), "\n")
	if strings.Contains(exported, "synthetic prompt") || strings.Contains(exported, "synthetic reply") {
		t.Fatalf("exported a body whose rule header was missing:\n%s", exported)
	}
	if got := strings.Count(exported, `"suppression_reason":"header_unresolved"`); got != 2 {
		t.Fatalf("exported %d strict stubs, want 2:\n%s", got, exported)
	}
	if got, _ := metrics.Value(telemetry.RecordsSuppressed, unresolved); got != 2 {
		t.Errorf("records_suppressed%v = %v, want 2", unresolved, got)
	}
	if got := strings.Count(logs.String(), "strict suppression stripped"); got != 1 {
		t.Errorf("strict warnings = %d, want one for the rule:\n%s", got, logs)
	}
}
