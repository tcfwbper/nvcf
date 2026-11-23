/*
SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"nvcf-grpc-proxy/proxy/eventledger"
	"nvcf-grpc-proxy/proxy/invocation"
	"nvcf-grpc-proxy/proxy/metrics"
	"nvcf-grpc-proxy/proxy/worker"
)

const testWorkerToken = "worker-token"

// newRecordingLedger returns a publisher wired to a fake ledger. The events
// it received are complete once the publisher is closed.
func newRecordingLedger(t *testing.T) (*eventledger.Publisher, func() []map[string]any) {
	t.Helper()
	var mu sync.Mutex
	var received []map[string]any
	ledger := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event map[string]any
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&event))
		mu.Lock()
		received = append(received, event)
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(ledger.Close)

	secrets := filepath.Join(t.TempDir(), "secrets.json")
	require.NoError(t, os.WriteFile(secrets, []byte(`{"eventLedgerToken":"test-token"}`), 0o600))
	publisher, err := eventledger.New(ledger.URL, secrets, "")
	require.NoError(t, err)
	return publisher, func() []map[string]any {
		mu.Lock()
		defer mu.Unlock()
		return received
	}
}

func ofType(events []map[string]any, eventType string) []map[string]any {
	var matching []map[string]any
	for _, event := range events {
		if event["type"] == eventType {
			matching = append(matching, event)
		}
	}
	return matching
}

func eventTypes(events []map[string]any) []string {
	types := []string{}
	for _, event := range events {
		types = append(types, event["type"].(string))
	}
	return types
}

// newEventDirector returns a director whose invoker records the caller's NCA
// ID on the worker token, as the real invoker does after authenticating the client.
func newEventDirector(t *testing.T, publisher *eventledger.Publisher) *StreamDirector {
	t.Helper()
	director := NewStreamDirector(FunctionInvokerFunc(func(_ context.Context, _ net.Conn, _, _, _ string, requestId *uuid.UUID, onWorkerAuth func(invocation.WorkerAuth)) (invocation.Result, context.CancelFunc, error) {
		result := invocation.Result{RequestId: uuid.New(), WorkerAuthorizationToken: testWorkerToken}
		if requestId != nil {
			result.RequestId = *requestId
		}
		onWorkerAuth(invocation.WorkerAuth{Token: result.WorkerAuthorizationToken, RequestId: result.RequestId, FunctionId: testFunctionId, FunctionVersionId: testFunctionVersionId, CallerNcaId: "nca-1"})
		return result, nil, nil
	}))
	director.eventLedger = publisher
	t.Cleanup(func() { _ = director.Close() })
	return director
}

// setUpSession runs the client side of session setup and returns the slot and
// the token record a worker CONNECT would be checked against.
func setUpSession(t *testing.T, director *StreamDirector, requestId *uuid.UUID) (*worker.WorkerConnection, workerAuthInfo) {
	t.Helper()
	client, _ := net.Pipe()
	wc, err := director.getAndInitWorkerConnection(t.Context(), worker.NewConnectionTrackingConn(client), "client-auth", testFunctionId, "", requestId)
	require.NoError(t, err)
	token := director.workerAuth.Get(testWorkerToken)
	require.NotNil(t, token)
	return wc, token.Value()
}

func TestRegisterWorkerPublishesAttachmentEstablished(t *testing.T) {
	existing := uuid.New()
	tests := []struct {
		name      string
		requestId *uuid.UUID
		isRejoin  bool
	}{
		{name: "new session"},
		{name: "rejoin", requestId: &existing, isRejoin: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			publisher, received := newRecordingLedger(t)
			director := newEventDirector(t, publisher)
			wc, info := setUpSession(t, director, tc.requestId)
			// Setup in a test takes under a millisecond, so backdate it to make
			// the reported latency distinguishable from zero.
			info.setupStartedAt = info.setupStartedAt.Add(-1500 * time.Millisecond)

			workerLink, _ := net.Pipe()
			require.NoError(t, director.RegisterWorker(t.Context(), testWorkerToken, info, workerLink))
			publisher.Close()

			all := received()
			dispatched := ofType(all, eventTypeWorkDispatched)
			require.Len(t, dispatched, 1)
			assert.Equal(t, map[string]any{
				"schema_version": float64(1),
				"request_id":     wc.RequestId.String(),
				"is_rejoin":      tc.isRejoin,
			}, dispatched[0]["data"])
			events := ofType(all, eventTypeAttachmentEstablished)
			require.Len(t, events, 1)
			event := events[0]
			assert.Equal(t, "GrpcProxy.Attachment.Established", event["type"])
			assert.Equal(t, "grpc-proxy", event["source"])
			assert.Equal(t, testFunctionVersionId, event["functionversionid"])
			assert.Equal(t, testFunctionId, event["functionid"])
			assert.Equal(t, wc.RequestId.String(), event["resourceid"])
			assert.Equal(t, "nca-1", event["ncaid"])
			connectedAt, err := time.Parse(time.RFC3339Nano, event["time"].(string))
			require.NoError(t, err)
			assert.Equal(t, map[string]any{
				"schema_version":   float64(1),
				"request_id":       wc.RequestId.String(),
				"attachment_id":    wc.AttachmentId.String(),
				"is_rejoin":        tc.isRejoin,
				"setup_latency_ms": float64(connectedAt.Sub(info.setupStartedAt).Milliseconds()),
			}, event["data"])
		})
	}
}

func TestRegisterWorkerPublishesOnceWhenWorkersRace(t *testing.T) {
	publisher, received := newRecordingLedger(t)
	director := newEventDirector(t, publisher)
	_, info := setUpSession(t, director, nil)

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			workerLink, _ := net.Pipe()
			errs[i] = director.RegisterWorker(t.Context(), testWorkerToken, info, workerLink)
		}()
	}
	wg.Wait()
	publisher.Close()

	assert.ElementsMatch(t, []bool{true, false}, []bool{errs[0] == nil, errs[1] == nil})
	assert.Len(t, ofType(received(), eventTypeAttachmentEstablished), 1)
}

func TestRegisterWorkerDuringShutdownPublishesNothing(t *testing.T) {
	publisher, received := newRecordingLedger(t)
	director := newEventDirector(t, publisher)
	_, info := setUpSession(t, director, nil)
	require.NoError(t, director.Close())

	workerLink, _ := net.Pipe()
	err := director.RegisterWorker(t.Context(), testWorkerToken, info, workerLink)
	publisher.Close()

	assert.ErrorIs(t, err, errShuttingDown)
	assert.Equal(t, []string{eventTypeWorkDispatched}, eventTypes(received()))
}

func TestRegisterWorkerAfterClientLeftPublishesNothing(t *testing.T) {
	publisher, received := newRecordingLedger(t)
	director := newEventDirector(t, publisher)
	client, _ := net.Pipe()
	tracking := worker.NewConnectionTrackingConn(client)
	_, err := director.getAndInitWorkerConnection(t.Context(), tracking, "client-auth", testFunctionId, "", nil)
	require.NoError(t, err)
	info := director.workerAuth.Get(testWorkerToken).Value()
	require.NoError(t, tracking.Close())

	// The token is still valid, so the worker attaches exactly as it does today.
	workerLink, _ := net.Pipe()
	require.NoError(t, director.RegisterWorker(t.Context(), testWorkerToken, info, workerLink))
	publisher.Close()

	assert.Equal(t, []string{eventTypeWorkDispatched}, eventTypes(received()))
}

func TestDispatchFailurePublishesSetupFailed(t *testing.T) {
	tests := []struct {
		name       string
		mintsToken bool
		want       []string
	}{
		{name: "dispatch fails after the token is minted", mintsToken: true, want: []string{eventTypeAttachmentSetupFailed}},
		{name: "rejected before dispatch", want: []string{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			publisher, received := newRecordingLedger(t)
			director := NewStreamDirector(FunctionInvokerFunc(func(_ context.Context, _ net.Conn, _, _, _ string, _ *uuid.UUID, onWorkerAuth func(invocation.WorkerAuth)) (invocation.Result, context.CancelFunc, error) {
				if tc.mintsToken {
					onWorkerAuth(invocation.WorkerAuth{Token: testWorkerToken, RequestId: uuid.New(), FunctionId: testFunctionId, FunctionVersionId: testFunctionVersionId, CallerNcaId: "nca-1"})
				}
				return invocation.Result{}, nil, errors.New("dispatch failed")
			}))
			director.eventLedger = publisher
			t.Cleanup(func() { _ = director.Close() })
			client, _ := net.Pipe()
			_, err := director.getAndInitWorkerConnection(t.Context(), worker.NewConnectionTrackingConn(client), "client-auth", testFunctionId, "", nil)
			require.Error(t, err)
			publisher.Close()

			events := received()
			assert.Equal(t, tc.want, eventTypes(events))
			if tc.mintsToken {
				assert.Equal(t, setupPhaseDispatch, events[0]["data"].(map[string]any)["phase"])
			}
		})
	}
}

func TestStreamingDispatchWaitsForAWorker(t *testing.T) {
	setUp := func(t *testing.T) (*StreamDirector, *eventledger.Publisher, func() []map[string]any, chan<- bool) {
		publisher, received := newRecordingLedger(t)
		answered := make(chan bool, 1)
		director := NewStreamDirector(FunctionInvokerFunc(func(_ context.Context, _ net.Conn, _, _, _ string, _ *uuid.UUID, onWorkerAuth func(invocation.WorkerAuth)) (invocation.Result, context.CancelFunc, error) {
			result := invocation.Result{RequestId: uuid.New(), WorkerAuthorizationToken: testWorkerToken, StreamingDispatch: answered}
			onWorkerAuth(invocation.WorkerAuth{Token: testWorkerToken, RequestId: result.RequestId, FunctionId: testFunctionId, FunctionVersionId: testFunctionVersionId, CallerNcaId: "nca-1"})
			return result, nil, nil
		}))
		director.eventLedger = publisher
		t.Cleanup(func() { _ = director.Close() })
		client, _ := net.Pipe()
		_, err := director.getAndInitWorkerConnection(t.Context(), worker.NewConnectionTrackingConn(client), "client-auth", testFunctionId, "", nil)
		require.NoError(t, err)
		return director, publisher, received, answered
	}

	t.Run("not before a worker answers", func(t *testing.T) {
		_, publisher, received, answered := setUp(t)
		// Close delivers everything published so far.
		publisher.Close()
		assert.Empty(t, received())
		// Abandon the request so the waiting goroutine ends.
		answered <- false
	})
	t.Run("once a worker answers", func(t *testing.T) {
		_, publisher, received, answered := setUp(t)
		answered <- true
		require.Eventually(t, func() bool {
			return len(ofType(received(), eventTypeWorkDispatched)) == 1
		}, 5*time.Second, 10*time.Millisecond)
		publisher.Close()
	})
	t.Run("a worker attaches before its answer is seen", func(t *testing.T) {
		director, publisher, received, answered := setUp(t)
		info := director.workerAuth.Get(testWorkerToken).Value()
		workerLink, _ := net.Pipe()
		require.NoError(t, director.RegisterWorker(t.Context(), testWorkerToken, info, workerLink))
		answered <- false
		require.Eventually(t, func() bool {
			return len(ofType(received(), eventTypeWorkDispatched)) == 1
		}, 5*time.Second, 10*time.Millisecond)
		publisher.Close()
	})
}

func TestAttachmentEndEvent(t *testing.T) {
	tests := []struct {
		name                   string
		established, connected bool
		cause                  string
		wantType               string // empty when no event should be published
		wantPhase              string
	}{
		{name: "established attachment closes", established: true, connected: true, cause: metrics.CloseReasonClientClosed, wantType: eventTypeAttachmentClosed},
		{name: "no worker before the TTL", cause: metrics.CloseReasonTTLExpired, wantType: eventTypeAttachmentSetupFailed, wantPhase: setupPhaseWorkerWait},
		{name: "client left before a worker", cause: metrics.CloseReasonClientClosed},
		{name: "attached but never reported", connected: true, cause: metrics.CloseReasonShutdown},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wc := worker.NewWorkerConnection(uuid.New(), testFunctionId, testFunctionVersionId, func() {}, func() {})
			t.Cleanup(func() { _ = wc.Close() })
			wc.CallerNcaId = "nca-1"
			if tc.established {
				require.True(t, wc.MarkEstablished())
			}
			if tc.connected {
				workerLink, _ := net.Pipe()
				require.NoError(t, wc.SetConnection(workerLink))
			}

			event, ok := attachmentEndEvent(wc, tc.cause, time.Now(), 1500*time.Millisecond, worker.CloseCodeEOF)
			if tc.wantType == "" {
				assert.False(t, ok)
				return
			}
			require.True(t, ok)
			assert.Equal(t, tc.wantType, event.Type())
			assert.Equal(t, "nca-1", event.Extensions()["ncaid"])
			assert.Equal(t, wc.RequestId.String(), event.Extensions()["resourceid"])
			var data map[string]any
			require.NoError(t, event.DataAs(&data))
			want := map[string]any{"schema_version": float64(1), "request_id": wc.RequestId.String(), "attachment_id": wc.AttachmentId.String()}
			if tc.wantPhase != "" {
				want["phase"] = tc.wantPhase
			} else {
				want["cause"] = tc.cause
				want["held_for_ms"] = float64(1500)
				want["close_code"] = worker.CloseCodeEOF
			}
			assert.Equal(t, want, data)
		})
	}
}

func TestClientCloseAfterAttachPublishesClosed(t *testing.T) {
	publisher, received := newRecordingLedger(t)
	director := newEventDirector(t, publisher)
	client, _ := net.Pipe()
	tracking := worker.NewConnectionTrackingConn(client)
	_, err := director.getAndInitWorkerConnection(t.Context(), tracking, "client-auth", testFunctionId, "", nil)
	require.NoError(t, err)
	info := director.workerAuth.Get(testWorkerToken).Value()
	workerLink, _ := net.Pipe()
	require.NoError(t, director.RegisterWorker(t.Context(), testWorkerToken, info, workerLink))

	require.NoError(t, tracking.Close())
	// The eviction that publishes Closed runs on its own goroutine.
	require.Eventually(t, func() bool {
		return len(ofType(received(), eventTypeAttachmentClosed)) == 1
	}, 5*time.Second, 10*time.Millisecond)
	publisher.Close()

	closed := ofType(received(), eventTypeAttachmentClosed)[0]
	assert.Equal(t, "nca-1", closed["ncaid"])
	assert.Equal(t, metrics.CloseReasonClientClosed, closed["data"].(map[string]any)["cause"])
}

// resumableInvoker answers whether a session can still be rejoined, as the
// real invoker does from the worker's registration.
type resumableInvoker struct {
	FunctionInvokerFunc
	resumable atomic.Bool
	lookups   atomic.Int64
}

func (r *resumableInvoker) SessionResumable(context.Context, uuid.UUID) (bool, error) {
	r.lookups.Add(1)
	return r.resumable.Load(), nil
}

func newWatchingDirector(t *testing.T, publisher *eventledger.Publisher) (*StreamDirector, *resumableInvoker) {
	t.Helper()
	director := newEventDirector(t, publisher)
	director.sessionEndPoll = 5 * time.Millisecond
	invoker := &resumableInvoker{FunctionInvokerFunc: director.functionInvoker.(FunctionInvokerFunc)}
	invoker.resumable.Store(true)
	director.functionInvoker = invoker
	return director, invoker
}

// attachThenLeave attaches a worker to a new session and then has its client
// leave.
func attachThenLeave(t *testing.T, director *StreamDirector) *worker.WorkerConnection {
	t.Helper()
	return attachThenClose(t, director, "")
}

// attachThenClose is attachThenLeave, with origin as the side that closed first.
func attachThenClose(t *testing.T, director *StreamDirector, origin string) *worker.WorkerConnection {
	t.Helper()
	client, _ := net.Pipe()
	tracking := worker.NewConnectionTrackingConn(client)
	wc, err := director.getAndInitWorkerConnection(t.Context(), tracking, "client-auth", testFunctionId, "", nil)
	require.NoError(t, err)
	info := director.workerAuth.Get(testWorkerToken).Value()
	workerLink, _ := net.Pipe()
	require.NoError(t, director.RegisterWorker(t.Context(), testWorkerToken, info, workerLink))
	if origin != "" {
		wc.SetCloseOrigin(origin)
	}
	require.NoError(t, tracking.Close())
	return wc
}

func TestSessionEndWatchedWhenTheWorkerLeavesFirst(t *testing.T) {
	publisher, received := newRecordingLedger(t)
	director, invoker := newWatchingDirector(t, publisher)
	wc := attachThenClose(t, director, metrics.CloseReasonWorkerClosed)

	require.Eventually(t, func() bool { return invoker.lookups.Load() > 0 }, 5*time.Second, time.Millisecond)
	invoker.resumable.Store(false)
	require.Eventually(t, func() bool {
		return len(ofType(received(), eventTypeSessionEnded)) == 1
	}, 5*time.Second, 5*time.Millisecond)
	publisher.Close()

	closed := single(t, received(), eventTypeAttachmentClosed)
	assert.Equal(t, metrics.CloseReasonWorkerClosed, closed["data"].(map[string]any)["cause"])
	assert.Equal(t, wc.RequestId.String(), single(t, received(), eventTypeSessionEnded)["resourceid"])
}

func TestSessionEndNotWatchedAfterShutdown(t *testing.T) {
	publisher, _ := newRecordingLedger(t)
	director, invoker := newWatchingDirector(t, publisher)
	attachThenClose(t, director, metrics.CloseReasonShutdown)
	assert.Never(t, func() bool { return invoker.lookups.Load() > 0 }, 100*time.Millisecond, 5*time.Millisecond)
	publisher.Close()
}

func TestSessionEndedOnceTheSessionCannotBeRejoined(t *testing.T) {
	publisher, received := newRecordingLedger(t)
	director, invoker := newWatchingDirector(t, publisher)
	wc := attachThenLeave(t, director)

	// While the worker still waits for the client, the session has not ended.
	require.Eventually(t, func() bool { return invoker.lookups.Load() >= 3 }, 5*time.Second, time.Millisecond)
	assert.Empty(t, ofType(received(), eventTypeSessionEnded))

	invoker.resumable.Store(false)
	require.Eventually(t, func() bool {
		return len(ofType(received(), eventTypeSessionEnded)) == 1
	}, 5*time.Second, 5*time.Millisecond)
	lookups := invoker.lookups.Load()
	assert.Never(t, func() bool { return invoker.lookups.Load() > lookups }, 50*time.Millisecond, 5*time.Millisecond,
		"kept watching after the session ended")
	publisher.Close()

	ended := single(t, received(), eventTypeSessionEnded)
	assert.Equal(t, wc.RequestId.String(), ended["resourceid"])
	assert.Equal(t, "nca-1", ended["ncaid"])
	assert.Equal(t, testFunctionVersionId, ended["functionversionid"])
	assert.Equal(t, map[string]any{"schema_version": float64(1), "request_id": wc.RequestId.String()}, ended["data"])
}

func TestSessionEndNotWatchedWithoutALedger(t *testing.T) {
	director, invoker := newWatchingDirector(t, nil)
	attachThenLeave(t, director)
	assert.Never(t, func() bool { return invoker.lookups.Load() > 0 }, 100*time.Millisecond, 5*time.Millisecond)
}

func TestSessionEndWatchStopsOnClose(t *testing.T) {
	publisher, received := newRecordingLedger(t)
	director, invoker := newWatchingDirector(t, publisher)
	attachThenLeave(t, director)
	require.Eventually(t, func() bool { return invoker.lookups.Load() > 0 }, 5*time.Second, time.Millisecond)

	require.NoError(t, director.Close())
	// One lookup may already be in flight.
	lookups := invoker.lookups.Load()
	assert.Never(t, func() bool { return invoker.lookups.Load() > lookups+1 }, 100*time.Millisecond, 5*time.Millisecond)
	publisher.Close()
	assert.Empty(t, ofType(received(), eventTypeSessionEnded))
}

func TestSessionEndNotReportedWhileRegistrationNotYetPublished(t *testing.T) {
	publisher, received := newRecordingLedger(t)
	director, invoker := newWatchingDirector(t, publisher)
	director.sessionRegistrationGrace = time.Hour
	invoker.resumable.Store(false)
	attachThenLeave(t, director)

	require.Eventually(t, func() bool { return invoker.lookups.Load() >= 5 }, 5*time.Second, time.Millisecond)
	assert.Empty(t, ofType(received(), eventTypeSessionEnded), "an unpublished registration is not an ended session")

	// Once seen, its removal does end the session.
	invoker.resumable.Store(true)
	lookups := invoker.lookups.Load()
	require.Eventually(t, func() bool { return invoker.lookups.Load() > lookups+1 }, 5*time.Second, time.Millisecond)
	invoker.resumable.Store(false)
	require.Eventually(t, func() bool {
		return len(ofType(received(), eventTypeSessionEnded)) == 1
	}, 5*time.Second, 5*time.Millisecond)
	publisher.Close()
}

func TestSessionEndReportedWhenRegistrationNeverAppears(t *testing.T) {
	publisher, received := newRecordingLedger(t)
	director, invoker := newWatchingDirector(t, publisher)
	director.sessionRegistrationGrace = 30 * time.Millisecond
	invoker.resumable.Store(false)
	attachThenLeave(t, director)

	require.Eventually(t, func() bool {
		return len(ofType(received(), eventTypeSessionEnded)) == 1
	}, 5*time.Second, 5*time.Millisecond)
	publisher.Close()
}

func TestSessionEndWatchRenewedByLaterDeparture(t *testing.T) {
	publisher, received := newRecordingLedger(t)
	director, invoker := newWatchingDirector(t, publisher)
	director.sessionEndWatchLimit = 150 * time.Millisecond
	wc := attachThenLeave(t, director)

	// The client keeps rejoining and leaving, each departure renewing the one
	// watcher, so it outlives the original deadline.
	for range 8 {
		time.Sleep(50 * time.Millisecond)
		director.watchSessionEnd(wc)
	}
	_, running := director.sessionWatches.Load(wc.RequestId)
	assert.True(t, running, "watch expired despite later departures")

	// The session then ends while that watcher is still the one watching.
	invoker.resumable.Store(false)
	require.Eventually(t, func() bool {
		return len(ofType(received(), eventTypeSessionEnded)) == 1
	}, 5*time.Second, 5*time.Millisecond)
	publisher.Close()
	assert.Len(t, ofType(received(), eventTypeSessionEnded), 1, "one watcher per request id")
}
