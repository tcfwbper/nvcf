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
package invocation

import (
	"context"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"net"

	"github.com/nats-io/nkeys"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"nvcf-grpc-proxy/nvcf/pb"
	"nvcf-grpc-proxy/proxy/internal/test"
	"nvcf-grpc-proxy/proxy/metrics"
)

func TestNatsErrorReason(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "expired certificate", err: x509.CertificateInvalidError{Reason: x509.Expired}, want: metrics.NatsErrorReasonCertificateExpired},
		{name: "verification failure", err: x509.UnknownAuthorityError{}, want: metrics.NatsErrorReasonTLSVerification},
		{name: "generic tls failure", err: errors.New("remote error: tls: bad certificate"), want: metrics.NatsErrorReasonTLS},
		{name: "authentication failure", err: nats.ErrAuthorization, want: metrics.NatsErrorReasonAuthentication},
		{name: "timeout", err: context.DeadlineExceeded, want: metrics.NatsErrorReasonTimeout},
		{name: "connection failure", err: io.EOF, want: metrics.NatsErrorReasonConnection},
		{name: "other failure", err: errors.New("unexpected error"), want: metrics.NatsErrorReasonOther},
		{name: "wrapped error", err: fmt.Errorf("reconnect: %w", nats.ErrAuthExpired), want: metrics.NatsErrorReasonAuthentication},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, natsErrorReason(test.err))
		})
	}
}

func TestRecordNatsDisconnect(t *testing.T) {
	before := testutil.ToFloat64(metrics.NatsDisconnectCounter)
	recordNatsDisconnect(io.EOF)
	assert.Equal(t, before+1, testutil.ToFloat64(metrics.NatsDisconnectCounter))
}

func TestRecordNatsAsyncError(t *testing.T) {
	failureCounter := metrics.NatsFailureCounter.WithLabelValues(metrics.NatsErrorReasonTimeout)
	errorBefore := testutil.ToFloat64(metrics.NatsErrorCounter)
	failureBefore := testutil.ToFloat64(failureCounter)
	recordNatsAsyncError(context.DeadlineExceeded)
	assert.Equal(t, errorBefore+1, testutil.ToFloat64(metrics.NatsErrorCounter))
	assert.Equal(t, failureBefore+1, testutil.ToFloat64(failureCounter))
}

func TestNewNkeyAuthOption(t *testing.T) {
	t.Run("valid nkey seed", func(t *testing.T) {
		// Generate a valid nkey seed for testing
		kp, err := nkeys.CreateUser()
		require.NoError(t, err)

		seed, err := kp.Seed()
		require.NoError(t, err)

		// Test the function
		option, err := newNkeyAuthOption(string(seed))
		assert.NoError(t, err)
		assert.NotNil(t, option)
	})

	t.Run("invalid nkey seed", func(t *testing.T) {
		option, err := newNkeyAuthOption("invalid-seed")
		assert.Error(t, err)
		assert.Nil(t, option)
	})

	t.Run("empty nkey seed", func(t *testing.T) {
		option, err := newNkeyAuthOption("")
		assert.Error(t, err)
		assert.Nil(t, option)
	})
}

func TestNewOAuthTokenProvider(t *testing.T) {
	t.Run("invalid secrets path", func(t *testing.T) {
		provider, err := newOAuthTokenProvider("https://ssa.example.com", "/non/existent/path")
		assert.Error(t, err)
		assert.Nil(t, provider)
	})

	t.Run("valid secrets path but invalid config", func(t *testing.T) {
		// Create a temporary secrets file with invalid config
		tmpDir := t.TempDir()
		secretsPath := filepath.Join(tmpDir, "secrets.json")

		secrets := map[string]any{
			"invalid": "config",
		}
		secretsData, _ := json.Marshal(secrets)
		err := os.WriteFile(secretsPath, secretsData, 0600)
		require.NoError(t, err)

		provider, err := newOAuthTokenProvider("https://ssa.example.com", secretsPath)
		// This should fail because the auth config is invalid
		assert.Error(t, err)
		assert.Nil(t, provider)
	})

	t.Run("valid secrets file", func(t *testing.T) {
		// Use the helper function to create a proper secrets file
		secretsPath := createTestSecretsFile(t)

		provider, err := newOAuthTokenProvider("https://ssa.example.com", secretsPath)
		// This will fail because we can't actually connect to the OAuth2
		// server, but it should not fail due to secrets file format
		assert.Error(t, err)
		assert.Nil(t, provider)
	})
}

func TestAuthCalloutPluginTokenProvider_GetToken(t *testing.T) {
	t.Run("token encoding format", func(t *testing.T) {
		// Test the token encoding format directly
		oauthToken := "test-oauth-token"
		tokenJson, err := json.Marshal(struct {
			Account    string `json:"account"`
			PluginName string `json:"pluginName"`
			Payload    string `json:"payload"`
		}{
			Account:    "Worker",
			PluginName: "ssa",
			Payload:    oauthToken,
		})
		require.NoError(t, err)

		expectedToken := base64.RawURLEncoding.EncodeToString(tokenJson)

		// Test the token structure by decoding the expected result
		decoded, err := base64.RawURLEncoding.DecodeString(expectedToken)
		require.NoError(t, err)

		var tokenData struct {
			Account    string `json:"account"`
			PluginName string `json:"pluginName"`
			Payload    string `json:"payload"`
		}

		err = json.Unmarshal(decoded, &tokenData)
		require.NoError(t, err)

		assert.Equal(t, "Worker", tokenData.Account)
		assert.Equal(t, "ssa", tokenData.PluginName)
		assert.Equal(t, oauthToken, tokenData.Payload)
	})
}

func TestNewAuthCalloutAuthOption(t *testing.T) {
	t.Run("invalid secrets path", func(t *testing.T) {
		option, err := newAuthCalloutAuthOption("https://ssa.example.com", "/non/existent/path")
		assert.Error(t, err)
		assert.Nil(t, option)
	})
}

// Integration test helper functions
func createTestSecretsFile(t *testing.T) string {
	tmpDir := t.TempDir()
	secretsPath := filepath.Join(tmpDir, "secrets.json")

	secrets := map[string]any{
		"client_id":     "test-client-id",
		"client_secret": "test-client-secret",
	}
	secretsData, _ := json.Marshal(secrets)
	err := os.WriteFile(secretsPath, secretsData, 0600)
	require.NoError(t, err)

	return secretsPath
}

func TestAuthCalloutPluginTokenProviderMarshalling(t *testing.T) {
	t.Run("json marshalling and encoding", func(t *testing.T) {
		// Test the JSON marshalling and base64 encoding logic directly
		testPayload := "test-oauth-token"

		tokenJson, err := json.Marshal(struct {
			Account    string `json:"account"`
			PluginName string `json:"pluginName"`
			Payload    string `json:"payload"`
		}{
			Account:    "Worker",
			PluginName: "ssa",
			Payload:    testPayload,
		})
		require.NoError(t, err)

		encodedToken := base64.RawURLEncoding.EncodeToString(tokenJson)
		assert.NotEmpty(t, encodedToken)

		// Verify the token can be properly decoded and parsed
		decoded, err := base64.RawURLEncoding.DecodeString(encodedToken)
		require.NoError(t, err)

		var result map[string]any
		err = json.Unmarshal(decoded, &result)
		require.NoError(t, err)

		assert.Equal(t, "Worker", result["account"])
		assert.Equal(t, "ssa", result["pluginName"])
		assert.Equal(t, testPayload, result["payload"])
	})
}

func TestRecordNatsStartupFailure(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	defer zap.ReplaceGlobals(zap.New(core))()

	failureCounter := metrics.NatsFailureCounter.WithLabelValues(metrics.NatsErrorReasonAuthentication)
	before := testutil.ToFloat64(failureCounter)

	recordNatsStartupFailure("nats initial connection failed", nats.ErrAuthorization)

	assert.Equal(t, before+1, testutil.ToFloat64(failureCounter))

	entries := logs.FilterMessage("nats initial connection failed").All()
	require.Len(t, entries, 1)
	// Startup failures are fatal to the process, so they must not be logged at warn.
	assert.Equal(t, zap.ErrorLevel, entries[0].Level)
	assert.Equal(t, metrics.NatsErrorReasonAuthentication, entries[0].ContextMap()["reason"])
}

// TestNewNatsConnectionsRecordsInitialConnectFailure covers the gap this change
// closes: nats.Connect returns the error synchronously and never invokes the
// registered ErrorHandler, so nothing was recorded for a failed initial connect.
func TestNewNatsConnectionsRecordsInitialConnectFailure(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	defer zap.ReplaceGlobals(zap.New(core))()

	// Bind and immediately release a port so the dial is refused rather than timing out.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	unreachable := listener.Addr().String()
	require.NoError(t, listener.Close())

	kp, err := nkeys.CreateUser()
	require.NoError(t, err)
	seed, err := kp.Seed()
	require.NoError(t, err)

	failureCounter := metrics.NatsFailureCounter.WithLabelValues(metrics.NatsErrorReasonConnection)
	before := testutil.ToFloat64(failureCounter)

	conns, err := NewNatsConnections("nats://"+unreachable, string(seed), "test-service", "", "")

	assert.Error(t, err)
	assert.Nil(t, conns)
	assert.Equal(t, before+1, testutil.ToFloat64(failureCounter))
	assert.Len(t, logs.FilterMessage("nats initial connection failed").All(), 1)
}

func TestNewNatsConnectionsRecordsNkeySetupFailure(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	defer zap.ReplaceGlobals(zap.New(core))()

	conns, err := NewNatsConnections("nats://127.0.0.1:4222", "invalid-seed", "test-service", "", "")

	assert.Error(t, err)
	assert.Nil(t, conns)
	assert.Len(t, logs.FilterMessage("nats nkey auth setup failed").All(), 1)
	// The connect is never attempted, so no connection failure should be logged.
	assert.Empty(t, logs.FilterMessage("nats initial connection failed").All())
}

// A reconnect rejected twice with the same auth error, as when the auth callout
// is briefly unavailable, must not close the connection: nothing replaces a
// closed connection. The control shows nats.go closing it without
// IgnoreAuthErrorAbort, so the test fails if that option is dropped.
func TestConnectOptionsReconnectAfterRepeatedAuthRejections(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		ignoreAuthErrorAbort bool
	}{
		{name: "connect options", ignoreAuthErrorAbort: true},
		{name: "control without IgnoreAuthErrorAbort", ignoreAuthErrorAbort: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, err := test.NewFakeNatsServer(test.NatsAcceptThenDrop, test.NatsRejectAuth, test.NatsRejectAuth)
			require.NoError(t, err)
			t.Cleanup(func() { _ = server.Close() })

			options := append(connectOptions("test-service", nats.Token(test.FakeNatsToken), &ConnState{}),
				nats.ReconnectWait(10*time.Millisecond),
				nats.ReconnectJitter(0, 0))
			if !tc.ignoreAuthErrorAbort {
				options = append(options, func(o *nats.Options) error {
					o.IgnoreAuthErrorAbort = false
					return nil
				})
			}
			nc, err := nats.Connect(server.URL, options...)
			require.NoError(t, err)
			t.Cleanup(nc.Close)

			if tc.ignoreAuthErrorAbort {
				// Reconnects counts each attempt that reached the server: two
				// rejections, then the accepted one.
				require.Eventually(t, func() bool {
					return nc.Status() == nats.CONNECTED && nc.Stats().Reconnects >= 3
				}, 10*time.Second, 10*time.Millisecond, "status %s after %d reconnects", nc.Status(), nc.Stats().Reconnects)
				return
			}
			require.Eventually(t, nc.IsClosed, 10*time.Second, 10*time.Millisecond, "status %s", nc.Status())
			assert.ErrorIs(t, nc.LastError(), nats.ErrAuthorization)
		})
	}
}

// connectOptions must never give up on the connection, and must keep the
// reconnect buffer so a purge queued during an outage is still sent afterwards.
// Each assertion fails if its option is changed.
func TestConnectOptionsReconnectForever(t *testing.T) {
	shared := nats.GetDefaultOptions()
	for _, option := range connectOptions("test-service", nats.Token(test.FakeNatsToken), &ConnState{}) {
		require.NoError(t, option(&shared))
	}
	assert.Equal(t, -1, shared.MaxReconnect)
	assert.True(t, shared.IgnoreAuthErrorAbort)
	assert.Equal(t, nats.DefaultReconnectBufSize, shared.ReconnectBufSize, "purges must stay buffered")
	assert.False(t, shared.UseOldRequestStyle)

	dispatch := nats.GetDefaultOptions()
	for _, option := range dispatchConnectOptions("test-service", nats.Token(test.FakeNatsToken), &ConnState{}) {
		require.NoError(t, option(&dispatch))
	}
	assert.Equal(t, -1, dispatch.MaxReconnect)
	assert.True(t, dispatch.IgnoreAuthErrorAbort)
	assert.Equal(t, -1, dispatch.ReconnectBufSize)
	assert.True(t, dispatch.UseOldRequestStyle)
	assert.Equal(t, "test-service-dispatch", dispatch.Name)
}

// testConnState is a ConnState with a gauge of its own, so tests running one
// after another do not read each other's values.
func testConnState(t *testing.T) *ConnState {
	return newConnState(t.Name())
}

// connect connects to server with connectOptions and waits for ConnState to
// report the connection up.
func connect(t *testing.T, server *test.FakeNatsServer, extra ...nats.Option) (*nats.Conn, *ConnState) {
	t.Helper()
	state := testConnState(t)
	options := append(connectOptions("test-service", nats.Token(test.FakeNatsToken), state),
		nats.ReconnectWait(10*time.Millisecond), nats.ReconnectJitter(0, 0))
	nc, err := nats.Connect(server.URL, append(options, extra...)...)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	require.Eventually(t, state.Connected, 5*time.Second, time.Millisecond)
	return nc, state
}

// connectReconnecting connects, then drops the connection and stops listening,
// so the connection stays reconnecting until the test ends.
func connectReconnecting(t *testing.T) (*nats.Conn, *ConnState) {
	t.Helper()
	server, err := test.NewFakeNatsServer(test.NatsAcceptThenDrop)
	require.NoError(t, err)
	nc, state := connect(t, server)
	require.NoError(t, server.Close())
	require.Eventually(t, func() bool { return !state.Connected() }, 5*time.Second, time.Millisecond)
	return nc, state
}

// connectBoth opens both connections to server and waits for them.
func connectBoth(t *testing.T, server *test.FakeNatsServer) *NatsConnections {
	t.Helper()
	conns, err := connectNats(server.URL, "test-service", nats.Token(test.FakeNatsToken),
		nats.ReconnectWait(10*time.Millisecond), nats.ReconnectJitter(0, 0))
	require.NoError(t, err)
	t.Cleanup(conns.Close)
	t.Cleanup(func() { metrics.SetNatsStatsConnections() })
	require.Eventually(t, conns.Connected, 5*time.Second, time.Millisecond)
	return conns
}

// outage drops both connections and keeps them reconnecting until the
// returned function ends it and they are connected again.
func outage(t *testing.T, server *test.FakeNatsServer, conns *NatsConnections) (end func()) {
	t.Helper()
	server.Outage()
	require.Eventually(t, func() bool {
		return !conns.SharedState.Connected() && !conns.DispatchState.Connected()
	}, 5*time.Second, time.Millisecond)
	return func() {
		server.EndOutage()
		require.Eventually(t, conns.Connected, 5*time.Second, time.Millisecond)
		// A flush round trip after the reconnect means the server has read
		// everything sent before it, including the reconnect buffer.
		require.NoError(t, conns.Shared.Flush())
		require.NoError(t, conns.Dispatch.Flush())
	}
}

func TestConnStateFollowsTheConnection(t *testing.T) {
	// The first connection is dropped and the reconnect is accepted.
	server, err := test.NewFakeNatsServer(test.NatsAcceptThenDrop)
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })
	nc, state := connect(t, server, nats.ReconnectWait(300*time.Millisecond))
	gauge := metrics.NatsConnected.WithLabelValues(t.Name())
	assert.Equal(t, 1.0, testutil.ToFloat64(gauge))

	// setConnected stores the state before it sets the gauge, so wait for both.
	require.Eventually(t, func() bool {
		return !state.Connected() && testutil.ToFloat64(gauge) == 0
	}, 5*time.Second, time.Millisecond)
	assert.False(t, state.Closed())

	require.Eventually(t, func() bool {
		return state.Connected() && testutil.ToFloat64(gauge) == 1
	}, 5*time.Second, time.Millisecond)

	nc.Close()
	require.Eventually(t, state.Closed, 5*time.Second, time.Millisecond)
	assert.False(t, state.Connected())
	assert.Equal(t, 0.0, testutil.ToFloat64(gauge))
}

// lockConnection holds nc's own lock until the returned function is called.
//
// It reaches nats.Conn's unexported mu field, so it is pinned to the nats.go
// version in go.mod and fails the test rather than guess if the field changes.
func lockConnection(t *testing.T, nc *nats.Conn) (unlock func()) {
	t.Helper()
	mu := connectionMutex(t, nc)
	mu.Lock()
	return mu.Unlock
}

func connectionMutex(t *testing.T, nc *nats.Conn) *sync.RWMutex {
	t.Helper()
	field := reflect.ValueOf(nc).Elem().FieldByName("mu")
	require.True(t, field.IsValid(), "nats.Conn has no mu field; revisit this helper for the new nats.go version")
	require.Equal(t, reflect.TypeOf(sync.RWMutex{}), field.Type())
	return (*sync.RWMutex)(unsafe.Pointer(field.UnsafeAddr()))
}

// pendingReplyHandlers counts the reply handlers nc keeps for default-style
// requests, read under nc's own lock. Pinned to the nats.go version like
// lockConnection.
func pendingReplyHandlers(t *testing.T, nc *nats.Conn) int {
	t.Helper()
	mu := connectionMutex(t, nc)
	mu.Lock()
	defer mu.Unlock()
	field := reflect.ValueOf(nc).Elem().FieldByName("respMap")
	require.True(t, field.IsValid(), "nats.Conn has no respMap field; revisit this helper for the new nats.go version")
	return field.Len()
}

// The callbacks must not take the connection's lock. A callback waiting on it,
// for example while a reconnect attempt holds it, would hold back every
// callback queued behind it, and ConnState would keep showing a connection that
// is already gone.
func TestCallbacksDoNotTakeTheConnectionLock(t *testing.T) {
	server, err := test.NewFakeNatsServer()
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })
	nc, _ := connect(t, server)

	state := testConnState(t)
	options := nats.GetDefaultOptions()
	for _, option := range connectOptions("test-service", nats.Token(test.FakeNatsToken), state) {
		require.NoError(t, option(&options))
	}

	unlock := lockConnection(t, nc)
	defer unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		options.ConnectedCB(nc)
		options.DisconnectedErrCB(nc, nil)
		options.ReconnectedCB(nc)
		options.ClosedCB(nc)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("a callback waited on the connection's lock")
	}
	assert.True(t, state.Closed())
}

// While a reconnect attempt holds the connection's lock, here inside the token
// handler as when the token endpoint is slow, every reader the probes, metrics
// and publishers use must still answer at once, and starting the stats sampler
// must not wait. The first assertion shows the lock really is held, so the test
// fails if nats.go stops taking it here and ConnState can be revisited.
func TestConnStateDoesNotWaitOnTheConnectionLock(t *testing.T) {
	server, err := test.NewFakeNatsServer(test.NatsAcceptThenDrop)
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })

	var tokenCalls atomic.Int32
	inHandler := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
	// Registered before anything that can block on the held lock, so a failure
	// below still lets the reconnect finish.
	t.Cleanup(releaseHandler)
	token := nats.TokenHandler(func() string {
		if tokenCalls.Add(1) == 2 {
			close(inHandler)
			<-release
		}
		return test.FakeNatsToken
	})
	state := testConnState(t)
	nc, err := nats.Connect(server.URL, append(connectOptions("test-service", token, state),
		nats.ReconnectWait(10*time.Millisecond), nats.ReconnectJitter(0, 0))...)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	// Cleanups run last first, and nc.Close waits for the lock the reconnect
	// holds, so release the reconnect again before it.
	t.Cleanup(releaseHandler)
	t.Cleanup(func() { metrics.SetNatsStatsConnections() })

	select {
	case <-inHandler:
	case <-time.After(5 * time.Second):
		t.Fatal("reconnect never reached the token handler")
	}

	isClosed := make(chan struct{})
	go func() {
		nc.IsClosed()
		close(isClosed)
	}()
	select {
	case <-isClosed:
		t.Fatal("nc.IsClosed returned while the reconnect held the lock")
	case <-time.After(200 * time.Millisecond):
	}

	// Run in a goroutine so a check that blocks on the lock fails the test
	// here instead of hanging it.
	type stateReads struct {
		connected, closed bool
		waitErr           error
	}
	read := make(chan stateReads, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		read <- stateReads{state.Connected(), state.Closed(), waitConnected(ctx, state)}
	}()
	select {
	case r := <-read:
		assert.False(t, r.connected)
		assert.False(t, r.closed)
		assert.ErrorIs(t, r.waitErr, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("reading the connection state waited on the connection's lock")
	}

	sampling := make(chan struct{})
	go func() {
		metrics.SetNatsStatsConnections(nc)
		close(sampling)
	}()
	select {
	case <-sampling:
	case <-time.After(time.Second):
		t.Fatal("starting the stats sampler waited on the connection's lock")
	}

	gathered := make(chan error, 1)
	go func() {
		_, err := prometheus.DefaultGatherer.Gather()
		gathered <- err
	}()
	select {
	case err := <-gathered:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("gathering metrics waited on the connection's lock")
	}

	releaseHandler()
	<-isClosed
	require.Eventually(t, state.Connected, 5*time.Second, time.Millisecond)
}

func TestWaitConnected(t *testing.T) {
	t.Run("returns at once when connected", func(t *testing.T) {
		server, err := test.NewFakeNatsServer()
		require.NoError(t, err)
		t.Cleanup(func() { _ = server.Close() })
		_, state := connect(t, server)

		assert.NoError(t, waitConnected(context.Background(), state))
	})

	t.Run("fails when the caller has already given up", func(t *testing.T) {
		server, err := test.NewFakeNatsServer()
		require.NoError(t, err)
		t.Cleanup(func() { _ = server.Close() })
		_, state := connect(t, server)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		assert.ErrorIs(t, waitConnected(ctx, state), context.Canceled)
	})

	t.Run("returns once reconnected", func(t *testing.T) {
		server, err := test.NewFakeNatsServer(test.NatsAcceptThenDrop)
		require.NoError(t, err)
		t.Cleanup(func() { _ = server.Close() })
		_, state := connect(t, server, nats.ReconnectWait(300*time.Millisecond))
		require.Eventually(t, func() bool { return !state.Connected() }, 5*time.Second, time.Millisecond)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, waitConnected(ctx, state))
		assert.True(t, state.Connected())
	})

	t.Run("gives up when the context ends", func(t *testing.T) {
		_, state := connectReconnecting(t)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		assert.ErrorIs(t, waitConnected(ctx, state), context.DeadlineExceeded)
	})

	t.Run("fails when the connection is closed", func(t *testing.T) {
		nc, state := connectReconnecting(t)
		nc.Close()
		require.Eventually(t, state.Closed, 5*time.Second, time.Millisecond)
		assert.ErrorIs(t, waitConnected(context.Background(), state), nats.ErrConnectionClosed)
	})
}

func TestWaitDispatchConnectedCountsFailures(t *testing.T) {
	closed := &ConnState{}
	closed.closed.Store(true)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelExpired := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancelExpired()
	for _, tc := range []struct {
		name   string
		ctx    context.Context
		state  *ConnState
		reason string
	}{
		{name: "canceled", ctx: canceled, state: &ConnState{}, reason: metrics.NatsSendReasonCanceled},
		{name: "timeout", ctx: expired, state: &ConnState{}, reason: metrics.NatsSendReasonTimeout},
		{name: "closed", ctx: context.Background(), state: closed, reason: metrics.NatsSendReasonClosed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			counter := metrics.NatsSendFailures.WithLabelValues(metrics.NatsSendSessionJoin, metrics.NatsSendStageWait, tc.reason)
			before := testutil.ToFloat64(counter)
			f := &FunctionInvoker{conns: &NatsConnections{DispatchState: tc.state}}
			assert.Error(t, f.waitDispatchConnected(tc.ctx, metrics.NatsSendSessionJoin))
			assert.Equal(t, before+1, testutil.ToFloat64(counter))
		})
	}
}

// Each connection has its own state and gauge. A pod is ready only while both
// are connected, and needs a restart once either is closed.
func TestNatsConnectionsHealth(t *testing.T) {
	shared, dispatch := newConnState(t.Name()+"-shared"), newConnState(t.Name()+"-dispatch")
	conns := &NatsConnections{SharedState: shared, DispatchState: dispatch}

	dispatch.setConnected(false)
	shared.setConnected(true)
	assert.False(t, conns.Connected())
	assert.Equal(t, 1.0, testutil.ToFloat64(metrics.NatsConnected.WithLabelValues(t.Name()+"-shared")))
	assert.Equal(t, 0.0, testutil.ToFloat64(metrics.NatsConnected.WithLabelValues(t.Name()+"-dispatch")))

	dispatch.setConnected(true)
	assert.True(t, conns.Connected())
	assert.False(t, conns.Closed())

	dispatch.closed.Store(true)
	assert.True(t, conns.Closed())
	shared.closed.Store(true)
	dispatch.closed.Store(false)
	assert.True(t, conns.Closed())
}

func TestNatsConnectionsCloseClosesBoth(t *testing.T) {
	server, err := test.NewFakeNatsServer()
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })
	conns := connectBoth(t, server)

	conns.Close()
	require.Eventually(t, conns.SharedState.Closed, 5*time.Second, time.Millisecond)
	require.Eventually(t, conns.DispatchState.Closed, 5*time.Second, time.Millisecond)
	assert.True(t, conns.Closed())
	assert.False(t, conns.Connected())
}

// If the dispatch connection cannot be opened, the shared one must not be left
// open behind the error.
func TestConnectNatsClosesSharedWhenDispatchFails(t *testing.T) {
	server, err := test.NewFakeNatsServer(test.NatsAccept, test.NatsRejectAuth)
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })
	closedNames := make(chan string, 2)

	conns, err := connectNats(server.URL, "test-service", nats.Token(test.FakeNatsToken),
		nats.ClosedHandler(func(nc *nats.Conn) { closedNames <- nc.Opts.Name }))

	assert.Error(t, err)
	assert.Nil(t, conns)
	select {
	case name := <-closedNames:
		assert.Equal(t, "test-service", name)
	case <-time.After(5 * time.Second):
		t.Fatal("the shared connection was left open")
	}
}

// While NATS is down, nats.go rejects every kind of send on the dispatch
// connection and keeps nothing to send later, and a JetStream publish and a
// request still work once it is back. The shared connection keeps its
// reconnect buffer, so a purge sent during the outage reaches the server after
// it.
func TestDispatchConnectionKeepsNothingAcrossAnOutage(t *testing.T) {
	server, err := test.NewFakeNatsServer()
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })
	conns := connectBoth(t, server)
	dispatchJS, err := jetstream.New(conns.Dispatch)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	endOutage := outage(t, server, conns)
	require.NoError(t, conns.Shared.Publish("purge.during-outage", nil))
	assert.ErrorIs(t, conns.Dispatch.Publish("join.during-outage", nil), nats.ErrReconnectBufExceeded)
	_, err = conns.Dispatch.RequestWithContext(ctx, "dispatch.during-outage", nil)
	assert.ErrorIs(t, err, nats.ErrReconnectBufExceeded)
	_, err = dispatchJS.Publish(ctx, "work.during-outage", nil)
	assert.ErrorIs(t, err, nats.ErrReconnectBufExceeded)
	endOutage()

	published := server.Published()
	assert.Contains(t, published, "purge.during-outage")
	assert.NotContains(t, published, "join.during-outage")
	assert.NotContains(t, published, "dispatch.during-outage")
	assert.NotContains(t, published, "work.during-outage")

	reply, err := conns.Dispatch.RequestWithContext(ctx, "dispatch.after-outage", nil)
	require.NoError(t, err)
	assert.Equal(t, test.FakeNatsAck, string(reply.Data))
	ack, err := dispatchJS.Publish(ctx, "work.after-outage", nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), ack.Sequence)
}

// The race the dispatch connection exists for: the send passes waitConnected,
// then the connection drops before the send reaches nats.go. Here ConnState is
// set to connected after the drop, as when the disconnect callback has not run
// yet. If NATS stays down past the caller's deadline, no send may reach the
// server once it is back.
func TestSendsRacingADisconnectAreNotSentAfterTheirDeadline(t *testing.T) {
	server, err := test.NewFakeNatsServer()
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })
	conns := connectBoth(t, server)
	f := testInvoker(t, conns)
	requestId := uuid.New()
	version := &pb.ProxyAuthResponse_FunctionVersion{FunctionVersionId: "version"}
	auth := &pb.ProxyAuthResponse{ClientNcaId: "nca"}
	before := map[string]float64{}
	for _, send := range metrics.NatsSends {
		before[send] = testutil.ToFloat64(rejectedAttempts(send))
	}

	endOutage := outage(t, server, conns)
	conns.DispatchState.setConnected(true)

	shortDeadline := func() context.Context {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		t.Cleanup(cancel)
		return ctx
	}
	_, _, _, err = f.startNewSession(shortDeadline(), nil, requestId, auth, "token", version)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.ErrorIs(t, f.joinExistingSession(shortDeadline(), requestId, auth, "token"), context.DeadlineExceeded)
	assert.ErrorIs(t, f.tryRegionForLLS(shortDeadline(), "default", requestId, version, nil), context.DeadlineExceeded)
	for _, send := range metrics.NatsSends {
		assert.Greater(t, testutil.ToFloat64(rejectedAttempts(send)), before[send], send)
	}
	endOutage()

	published := server.Published()
	assert.NotContains(t, published, f.requestStreamSubject(version.FunctionVersionId, requestId))
	assert.NotContains(t, published, reconnectSubject(requestId))
	assert.NotContains(t, published, llsRequestSubject("default", version.FunctionVersionId, requestId))
}

// The same race, but NATS is back before the deadline. A send rejected by
// nats.go waits and tries again, so a brief outage does not
// fail it, and it reaches the server exactly once.
func TestSendsRacingADisconnectAreRetriedWithinTheirDeadline(t *testing.T) {
	server, err := test.NewFakeNatsServer()
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })
	conns := connectBoth(t, server)
	f := testInvoker(t, conns)
	requestId := uuid.New()
	version := &pb.ProxyAuthResponse_FunctionVersion{FunctionVersionId: "version"}
	auth := &pb.ProxyAuthResponse{ClientNcaId: "nca"}
	workItem := rejectedAttempts(metrics.NatsSendWorkItem)
	sessionJoin := rejectedAttempts(metrics.NatsSendSessionJoin)
	dispatch := rejectedAttempts(metrics.NatsSendDispatch)
	workItemBefore, sessionJoinBefore, dispatchBefore := testutil.ToFloat64(workItem), testutil.ToFloat64(sessionJoin), testutil.ToFloat64(dispatch)

	endOutage := outage(t, server, conns)
	conns.DispatchState.setConnected(true)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	workItemErr := make(chan error, 1)
	sessionJoinErr := make(chan error, 1)
	go func() {
		_, _, _, err := f.startNewSession(ctx, nil, requestId, auth, "token", version)
		workItemErr <- err
	}()
	go func() { sessionJoinErr <- f.joinExistingSession(ctx, requestId, auth, "token") }()
	// One LLS attempt has 3 s, so its retry must succeed within that attempt
	// rather than fail into the next region's backoff.
	dispatchErr := make(chan error, 1)
	go func() { dispatchErr <- f.tryRegionForLLS(ctx, "default", requestId, version, []byte("request")) }()
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(workItem) > workItemBefore &&
			testutil.ToFloat64(sessionJoin) > sessionJoinBefore &&
			testutil.ToFloat64(dispatch) > dispatchBefore
	}, 5*time.Second, time.Millisecond, "every send should be rejected while NATS is down")
	endOutage()

	require.NoError(t, <-workItemErr)
	require.NoError(t, <-sessionJoinErr)
	require.NoError(t, <-dispatchErr)
	require.NoError(t, conns.Dispatch.Flush())
	published := server.Published()
	assert.Equal(t, 1, countOf(published, f.requestStreamSubject(version.FunctionVersionId, requestId)))
	assert.Equal(t, 1, countOf(published, reconnectSubject(requestId)))
	assert.Equal(t, 1, countOf(published, llsRequestSubject("default", version.FunctionVersionId, requestId)))
}

// A work item whose ack never comes may already be with NATS, so it must not
// be retried: it fails at its deadline and reaches the server once.
func TestWorkItemWithoutAnAckIsNotRetried(t *testing.T) {
	server, err := test.NewFakeNatsServer()
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })
	conns := connectBoth(t, server)
	f := testInvoker(t, conns)
	requestId := uuid.New()
	version := &pb.ProxyAuthResponse_FunctionVersion{FunctionVersionId: "version"}
	subject := f.requestStreamSubject(version.FunctionVersionId, requestId)
	server.WithholdReplies(subject)
	rejected := rejectedAttempts(metrics.NatsSendWorkItem)
	before := testutil.ToFloat64(rejected)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, _, _, err = f.startNewSession(ctx, nil, requestId, &pb.ProxyAuthResponse{ClientNcaId: "nca"}, "token", version)

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, conns.Dispatch.Flush())
	assert.Equal(t, 1, countOf(server.Published(), subject))
	assert.Equal(t, before, testutil.ToFloat64(rejected))
}

// Repeated rejections, as over an outage that outlasts the send, are retried
// until the one deadline set at the start, never a fresh one per attempt.
// Publishing here never waits on the client's locks; see
// TestSendBlockedByAReconnectGoesOutAfterItsCallerLeft for what happens when
// it does.
func TestPublishDispatchRetriesUntilItsOneDeadline(t *testing.T) {
	f := connectedInvoker(t)
	f.retryPause = func(ctx context.Context) { SleepWithContext(ctx, 10*time.Millisecond) }
	rejected := rejectedAttempts(metrics.NatsSendSessionJoin)
	timedOut := metrics.NatsSendFailures.WithLabelValues(metrics.NatsSendSessionJoin, metrics.NatsSendStageWait, metrics.NatsSendReasonTimeout)
	rejectedBefore, timedOutBefore := testutil.ToFloat64(rejected), testutil.ToFloat64(timedOut)
	// Bounds the test if the deadline were renewed per attempt.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const timeout = 200 * time.Millisecond
	start := time.Now()
	var firstAttempt time.Time
	var deadlines []time.Time
	err := f.publishDispatch(ctx, metrics.NatsSendSessionJoin, timeout, func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		if firstAttempt.IsZero() {
			firstAttempt = time.Now()
		}
		deadlines = append(deadlines, deadline)
		return nats.ErrReconnectBufExceeded
	})

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	require.Greater(t, len(deadlines), 1)
	assert.False(t, deadlines[0].Before(start.Add(timeout)))
	assert.False(t, deadlines[0].After(firstAttempt.Add(timeout)))
	for i, deadline := range deadlines {
		assert.True(t, deadline.Equal(deadlines[0]), "attempt %d has its own deadline", i+1)
	}
	assert.Equal(t, rejectedBefore+float64(len(deadlines)), testutil.ToFloat64(rejected))
	assert.Equal(t, timedOutBefore+1, testutil.ToFloat64(timedOut))
}

// After a rejection nothing is published until the pause ends. The pause is
// held open here, so the check does not depend on timing.
func TestPublishDispatchPausesBeforeRetrying(t *testing.T) {
	f := connectedInvoker(t)
	paused := make(chan struct{}, 1)
	release := make(chan struct{})
	f.retryPause = func(ctx context.Context) {
		paused <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
	}
	attempts := make(chan int, 10)
	done := make(chan error, 1)
	go func() {
		n := 0
		done <- f.publishDispatch(context.Background(), metrics.NatsSendWorkItem, 5*time.Second, func(context.Context) error {
			n++
			attempts <- n
			if n == 1 {
				return nats.ErrReconnectBufExceeded
			}
			return nil
		})
	}()

	require.Equal(t, 1, receive(t, attempts))
	select {
	case <-paused:
	case n := <-attempts:
		t.Fatalf("attempt %d was published without a pause", n)
	case <-time.After(5 * time.Second):
		t.Fatal("the retry never paused")
	}
	select {
	case n := <-attempts:
		t.Fatalf("attempt %d was published during the pause", n)
	default:
	}
	close(release)

	require.NoError(t, receive(t, done))
	assert.Equal(t, 2, receive(t, attempts))
}

// A caller that cancels during the pause stops the retries. The rejected
// attempt and the cancellation are each counted once.
func TestPublishDispatchStopsWhenCanceledDuringThePause(t *testing.T) {
	f := connectedInvoker(t)
	paused := make(chan struct{}, 1)
	f.retryPause = func(ctx context.Context) {
		paused <- struct{}{}
		<-ctx.Done()
	}
	rejected := rejectedAttempts(metrics.NatsSendWorkItem)
	canceled := metrics.NatsSendFailures.WithLabelValues(metrics.NatsSendWorkItem, metrics.NatsSendStageWait, metrics.NatsSendReasonCanceled)
	rejectedBefore, canceledBefore := testutil.ToFloat64(rejected), testutil.ToFloat64(canceled)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var attempts atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- f.publishDispatch(ctx, metrics.NatsSendWorkItem, 5*time.Second, func(context.Context) error {
			attempts.Add(1)
			return nats.ErrReconnectBufExceeded
		})
	}()

	receive(t, paused)
	cancel()

	assert.ErrorIs(t, receive(t, done), context.Canceled)
	assert.Equal(t, int32(1), attempts.Load())
	assert.Equal(t, rejectedBefore+1, testutil.ToFloat64(rejected))
	assert.Equal(t, canceledBefore+1, testutil.ToFloat64(canceled))
}

// Only ErrReconnectBufExceeded means the attempt never left the client. Every
// other result is returned after one attempt.
func TestPublishDispatchRetriesOnlyRejectedAttempts(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "sent", err: nil},
		{name: "no ack", err: context.DeadlineExceeded},
		{name: "no responders", err: nats.ErrNoResponders},
		{name: "closed", err: nats.ErrConnectionClosed},
		{name: "other", err: errors.New("other")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := connectedInvoker(t)
			f.retryPause = func(context.Context) { t.Error("paused to retry") }
			attempts := 0
			err := f.publishDispatch(context.Background(), metrics.NatsSendWorkItem, 5*time.Second, func(context.Context) error {
				attempts++
				return tc.err
			})
			assert.Equal(t, 1, attempts)
			if tc.err == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, tc.err)
			}
		})
	}
}

// Every send is timed once, by its final result, however many attempts it took.
func TestPublishDispatchTimesEachSendByResult(t *testing.T) {
	f := connectedInvoker(t)
	f.retryPause = func(context.Context) {}
	send := metrics.NatsSendSessionJoin
	successes, errs := sendDurationCount(t, send, metrics.NatsSendResultSuccess), sendDurationCount(t, send, metrics.NatsSendResultError)

	attempts := 0
	require.NoError(t, f.publishDispatch(context.Background(), send, 5*time.Second, func(context.Context) error {
		attempts++
		if attempts == 1 {
			return nats.ErrReconnectBufExceeded
		}
		return nil
	}))
	assert.Error(t, f.publishDispatch(context.Background(), send, 5*time.Second, func(context.Context) error {
		return nats.ErrNoResponders
	}))

	assert.Equal(t, successes+1, sendDurationCount(t, send, metrics.NatsSendResultSuccess))
	assert.Equal(t, errs+1, sendDurationCount(t, send, metrics.NatsSendResultError))
}

// sendDurationCount reads how many sends NatsSendDuration has observed for the
// labels.
func sendDurationCount(t *testing.T, send, result string) uint64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != metrics.NatsNamespace+"_send_duration_seconds" {
			continue
		}
		for _, m := range family.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["send"] == send && labels["result"] == result {
				return m.GetHistogram().GetSampleCount()
			}
		}
	}
	t.Fatalf("no send duration series for send=%s result=%s", send, result)
	return 0
}

func connectedInvoker(t *testing.T) *FunctionInvoker {
	t.Helper()
	state := testConnState(t)
	state.setConnected(true)
	return &FunctionInvoker{conns: &NatsConnections{DispatchState: state}}
}

func rejectedAttempts(send string) prometheus.Counter {
	return metrics.NatsSendFailures.WithLabelValues(send, metrics.NatsSendStagePublish, metrics.NatsSendReasonReconnecting)
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting")
		var zero T
		return zero
	}
}

func countOf(subjects []string, subject string) int {
	n := 0
	for _, s := range subjects {
		if s == subject {
			n++
		}
	}
	return n
}

func testInvoker(t *testing.T, conns *NatsConnections) *FunctionInvoker {
	t.Helper()
	js, err := jetstream.New(conns.Shared)
	require.NoError(t, err)
	dispatchJS, err := jetstream.New(conns.Dispatch)
	require.NoError(t, err)
	return &FunctionInvoker{conns: conns, js: js, dispatchJS: dispatchJS, region: "test"}
}

// A request whose publish nats.go rejects must not leave anything behind. The
// dispatch connection rejects every request while it reconnects, and it
// reconnects without limit, so anything left per request would grow without
// bound over an outage.
func TestRejectedDispatchRequestsLeaveNothingBehind(t *testing.T) {
	const requests = 100
	reject := func(t *testing.T, nc *nats.Conn) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for range requests {
			_, err := nc.RequestWithContext(ctx, "dispatch.during-outage", nil)
			require.ErrorIs(t, err, nats.ErrReconnectBufExceeded)
		}
	}

	t.Run("dispatch connection", func(t *testing.T) {
		server, err := test.NewFakeNatsServer()
		require.NoError(t, err)
		t.Cleanup(func() { _ = server.Close() })
		conns := connectBoth(t, server)
		outage(t, server, conns)
		subscriptions := conns.Dispatch.NumSubscriptions()

		reject(t, conns.Dispatch)

		assert.Equal(t, subscriptions, conns.Dispatch.NumSubscriptions())
		assert.Zero(t, pendingReplyHandlers(t, conns.Dispatch))
	})

	// The control: with nats.go's default request style, every rejected request
	// leaves its reply handler registered. This is why the dispatch connection
	// uses the old style, and the test fails if nats.go stops leaking here.
	t.Run("default request style leaks", func(t *testing.T) {
		server, err := test.NewFakeNatsServer()
		require.NoError(t, err)
		t.Cleanup(func() { _ = server.Close() })
		nc, state := connect(t, server, nats.ReconnectBufSize(-1))
		server.Outage()
		require.Eventually(t, func() bool { return !state.Connected() }, 5*time.Second, time.Millisecond)

		reject(t, nc)

		assert.Equal(t, requests, pendingReplyHandlers(t, nc))
	})
}

// firstCheckContext reports when nats.go has made its first cancellation check
// and found the context live.
type firstCheckContext struct {
	context.Context
	once   sync.Once
	passed chan struct{}
}

func (c *firstCheckContext) Err() error {
	err := c.Context.Err()
	c.once.Do(func() {
		if err == nil {
			close(c.passed)
		}
	})
	return err
}

// A known limitation, not something the dispatch connection guarantees: a send
// that nats.go has accepted waits on the connection's lock while a reconnect
// attempt holds it, and goes out once that attempt succeeds, even though its
// caller gave up in the meantime. The dispatch connection only keeps sends out
// of the reconnect buffer; it does not bound how late a send goes out. If a
// nats.go upgrade changes this, revisit the comment on NatsConnections.
func TestSendBlockedByAReconnectGoesOutAfterItsCallerLeft(t *testing.T) {
	server, err := test.NewFakeNatsServer()
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })

	var tokenCalls atomic.Int32
	inHandler := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseHandler)
	token := nats.TokenHandler(func() string {
		if tokenCalls.Add(1) == 2 {
			close(inHandler)
			<-release
		}
		return test.FakeNatsToken
	})
	state := testConnState(t)
	nc, err := nats.Connect(server.URL, append(dispatchConnectOptions("test-service", token, state),
		nats.ReconnectWait(10*time.Millisecond), nats.ReconnectJitter(0, 0))...)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	// Cleanups run last first, and nc.Close waits for the lock the reconnect
	// holds, so release the reconnect again before it.
	t.Cleanup(releaseHandler)
	require.Eventually(t, state.Connected, 5*time.Second, time.Millisecond)

	// Drop the connection; the reconnect attempt then holds the lock in the
	// token handler.
	server.Outage()
	server.EndOutage()
	select {
	case <-inHandler:
	case <-time.After(5 * time.Second):
		t.Fatal("reconnect never reached the token handler")
	}

	parent, leave := context.WithCancel(context.Background())
	ctx := &firstCheckContext{Context: parent, passed: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		_, err := nc.RequestWithContext(ctx, "dispatch.late", nil)
		result <- err
	}()
	select {
	case <-ctx.passed:
	case <-time.After(5 * time.Second):
		t.Fatal("the request never made its first cancellation check")
	}
	leave()
	assert.NotContains(t, server.Published(), "dispatch.late")

	releaseHandler()
	select {
	case err := <-result:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("the request never returned")
	}
	require.NoError(t, nc.Flush())
	assert.Contains(t, server.Published(), "dispatch.late", "the send went out after its caller left")
}

// BenchmarkDispatchRequest compares nats.go's request styles on a connection
// without a reconnect buffer, as the dispatch connection is. It runs against
// the in-process fake server, whose parsing counts toward the time and
// allocations, so it measures the client plus the fake server, not
// the client alone, and not latency through a real NATS cluster.
func BenchmarkDispatchRequest(b *testing.B) {
	payload := make([]byte, 512)
	for _, style := range []struct {
		name string
		old  bool
	}{{"default", false}, {"old", true}} {
		for _, senders := range []int{1, 16, 64} {
			b.Run(fmt.Sprintf("style=%s/senders=%d", style.name, senders), func(b *testing.B) {
				server, err := test.NewFakeNatsServer()
				require.NoError(b, err)
				b.Cleanup(func() { _ = server.Close() })
				server.DiscardPublished()
				options := []nats.Option{nats.Token(test.FakeNatsToken), nats.ReconnectBufSize(-1)}
				if style.old {
					options = append(options, nats.UseOldRequestStyle())
				}
				nc, err := nats.Connect(server.URL, options...)
				require.NoError(b, err)
				b.Cleanup(nc.Close)
				ctx := context.Background()

				var next atomic.Int64
				var wg sync.WaitGroup
				b.ReportAllocs()
				b.ResetTimer()
				for range senders {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for next.Add(1) <= int64(b.N) {
							if _, err := nc.RequestWithContext(ctx, "dispatch.bench", payload); err != nil {
								b.Error(err)
								return
							}
						}
					}()
				}
				wg.Wait()
			})
		}
	}
}
