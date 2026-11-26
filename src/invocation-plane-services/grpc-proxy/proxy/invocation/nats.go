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
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/nvkit/auth"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"google.golang.org/grpc/credentials"

	"nvcf-grpc-proxy/proxy/metrics"
)

// NatsConnections are the two NATS connections a proxy keeps.
//
// Shared carries lookups, purges and subscriptions and keeps nats.go's reconnect
// buffer: a purge queued during an outage still removes dead work when it is
// sent. Dispatch carries only the sends that must not go out late, which are LLS
// dispatches, work items and session joins. It has no reconnect buffer, so
// nats.go rejects a send made while it is reconnecting instead of keeping it and
// sending it after the outage, when nobody is waiting for it. waitConnected only
// keeps a send from trying while the connection is down; nats.go makes the final
// decision under its own lock when the send is made.
//
// This does not bound how late a send goes out. A send can wait on nats.go's
// lock while a reconnect attempt holds it and go out once that attempt
// succeeds, after its caller's deadline.
type NatsConnections struct {
	Shared        *nats.Conn
	SharedState   *ConnState
	Dispatch      *nats.Conn
	DispatchState *ConnState
}

// Connected reports whether both connections are up. A pod needs both to
// serve: the dispatch connection to send work and the shared one to look up
// and purge it.
func (c *NatsConnections) Connected() bool {
	return c.SharedState.Connected() && c.DispatchState.Connected()
}

// Closed reports whether either connection has closed. nats.go never reopens a
// closed connection, so the pod cannot serve until it restarts.
func (c *NatsConnections) Closed() bool {
	return c.SharedState.Closed() || c.DispatchState.Closed()
}

// Close closes both connections.
func (c *NatsConnections) Close() {
	c.Dispatch.Close()
	c.Shared.Close()
}

func NewNatsConnections(natsFqdn, nKeySeed, serviceName, oauth2Fqdn, secretsPath string) (*NatsConnections, error) {
	var natsAuthOption nats.Option
	if nKeySeed != "" {
		var err error
		natsAuthOption, err = newNkeyAuthOption(nKeySeed)
		if err != nil {
			recordNatsStartupFailure("nats nkey auth setup failed", err)
			return nil, err
		}
	} else {
		var err error
		natsAuthOption, err = newAuthCalloutAuthOption(oauth2Fqdn, secretsPath)
		if err != nil {
			recordNatsStartupFailure("nats auth callout setup failed", err)
			return nil, err
		}
	}
	return connectNats(natsFqdn, serviceName, natsAuthOption)
}

// connectNats opens both connections, or neither: if the dispatch connection
// fails, the shared one is closed. extra options apply to both.
func connectNats(natsFqdn, serviceName string, authOption nats.Option, extra ...nats.Option) (*NatsConnections, error) {
	sharedState := newConnState(metrics.NatsConnectionShared)
	shared, err := nats.Connect(natsFqdn, append(connectOptions(serviceName, authOption, sharedState), extra...)...)
	if err != nil {
		recordNatsStartupFailure("nats initial connection failed", err)
		return nil, err
	}
	dispatchState := newConnState(metrics.NatsConnectionDispatch)
	dispatch, err := nats.Connect(natsFqdn, append(dispatchConnectOptions(serviceName, authOption, dispatchState), extra...)...)
	if err != nil {
		shared.Close()
		recordNatsStartupFailure("nats initial dispatch connection failed", err)
		return nil, err
	}
	metrics.SetNatsStatsConnections(shared, dispatch)
	return &NatsConnections{
		Shared:        shared,
		SharedState:   sharedState,
		Dispatch:      dispatch,
		DispatchState: dispatchState,
	}, nil
}

// ConnState mirrors a connection's state from nats.go's callbacks, so reading
// it never takes the connection's lock. nats.go holds that lock through each
// reconnect attempt, including the dial, the token fetch and the wait for the
// server, so IsConnected, IsClosed and Status can block for seconds while
// reconnecting. Callbacks are delivered in order on one goroutine, so the state
// trails the connection only by that delivery delay, as long as no callback
// blocks. The callbacks in connectOptions never call a method that takes the
// lock.
type ConnState struct {
	name      string
	connected atomic.Bool
	closed    atomic.Bool
	gauge     prometheus.Gauge
}

func newConnState(name string) *ConnState {
	return &ConnState{name: name, gauge: metrics.NatsConnected.WithLabelValues(name)}
}

// Connected reports whether the connection is up.
func (s *ConnState) Connected() bool {
	return s.connected.Load()
}

// Closed reports whether the connection has closed. nats.go never reopens a
// closed connection.
func (s *ConnState) Closed() bool {
	return s.closed.Load()
}

func (s *ConnState) setConnected(connected bool) {
	s.connected.Store(connected)
	if s.gauge == nil {
		return
	}
	if connected {
		s.gauge.Set(1)
	} else {
		s.gauge.Set(0)
	}
}

// dispatchConnectOptions are connectOptions without the reconnect buffer, for
// the dispatch connection.
//
// UseOldRequestStyle gives each request its own reply subscription, which
// nats.go removes even when the publish fails. In the default style a request
// whose publish fails leaves its reply handler registered until the connection
// closes, and this connection rejects every publish made while it is
// reconnecting and never closes.
func dispatchConnectOptions(serviceName string, authOption nats.Option, state *ConnState) []nats.Option {
	return append(connectOptions(serviceName+"-dispatch", authOption, state),
		nats.ReconnectBufSize(-1),
		nats.UseOldRequestStyle())
}

// connectOptions never call a method on the connection that takes its lock, such
// as ConnectedServerName. A callback blocked on the lock would hold back the
// callbacks queued behind it, leaving ConnState showing a stale state.
func connectOptions(serviceName string, authOption nats.Option, state *ConnState) []nats.Option {
	return []nats.Option{nats.PingInterval(10 * time.Second), authOption,
		// Nothing replaces a closed connection, and readiness stays down while it is
		// closed, so keep reconnecting through long outages. Without
		// IgnoreAuthErrorAbort, nats.go closes the connection after the same
		// authentication error twice, even with unlimited reconnects.
		nats.MaxReconnects(-1),
		nats.IgnoreAuthErrorAbort(),
		nats.LameDuckModeHandler(func(conn *nats.Conn) {
			metrics.NatsLameDuckCounter.Inc()
			go func() {
				// TODO remove when the SDK natively handles reconnects on lame duck
				time.Sleep(time.Duration(rand.Int63n(int64(time.Second * 10))))
				zap.L().Info("got lame duck message, force reconnecting", zap.String("connection", state.name))
				_ = conn.ForceReconnect()
			}()
		}), nats.ReconnectHandler(func(*nats.Conn) {
			state.setConnected(true)
			zap.L().Info("reconnected to nats", zap.String("connection", state.name))
			metrics.NatsReconnectCounter.Inc()
		}), nats.Name(serviceName),
		nats.ErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
			recordNatsAsyncError(err)
		}), nats.ReconnectErrHandler(func(conn *nats.Conn, err error) {
			recordNatsFailure("nats reconnect failed", err)
		}), nats.DisconnectErrHandler(func(conn *nats.Conn, err error) {
			state.setConnected(false)
			recordNatsDisconnect(err)
		}), nats.ConnectHandler(func(*nats.Conn) {
			state.setConnected(true)
			zap.L().Info("connected to nats", zap.String("connection", state.name))
		}), nats.ClosedHandler(func(*nats.Conn) {
			state.setConnected(false)
			state.closed.Store(true)
			zap.L().Warn("nats connection closed", zap.String("connection", state.name))
		})}
}

func recordNatsDisconnect(err error) {
	if err == nil {
		zap.L().Info("disconnected from nats")
	} else {
		zap.L().Warn("disconnected from nats", zap.Error(err))
	}
	metrics.NatsDisconnectCounter.Inc()
}

func recordNatsAsyncError(err error) {
	metrics.NatsErrorCounter.Inc()
	recordNatsFailure("nats connection error", err)
}

func recordNatsFailure(message string, err error) {
	reason := natsErrorReason(err)
	zap.L().Warn(message, zap.String("reason", reason), zap.Error(err))
	metrics.NatsFailureCounter.WithLabelValues(reason).Inc()
}

// recordNatsStartupFailure records a failure that prevents the proxy from
// starting. The process exits before the metrics endpoint is served, so the log
// line, not the counter, is what leaves the pod.
func recordNatsStartupFailure(message string, err error) {
	reason := natsErrorReason(err)
	zap.L().Error(message, zap.String("reason", reason), zap.Error(err))
	metrics.NatsFailureCounter.WithLabelValues(reason).Inc()
}

func natsErrorReason(err error) string {
	var certificateInvalid x509.CertificateInvalidError
	if errors.As(err, &certificateInvalid) {
		if certificateInvalid.Reason == x509.Expired {
			return metrics.NatsErrorReasonCertificateExpired
		}
		return metrics.NatsErrorReasonTLSVerification
	}

	var unknownAuthority x509.UnknownAuthorityError
	var hostnameError x509.HostnameError
	var certificateVerification *tls.CertificateVerificationError
	if errors.As(err, &unknownAuthority) || errors.As(err, &hostnameError) || errors.As(err, &certificateVerification) {
		return metrics.NatsErrorReasonTLSVerification
	}

	errorText := strings.ToLower(errString(err))
	if strings.Contains(errorText, "certificate has expired") || strings.Contains(errorText, "expired certificate") {
		return metrics.NatsErrorReasonCertificateExpired
	}
	if errors.Is(err, nats.ErrSecureConnRequired) || errors.Is(err, nats.ErrSecureConnWanted) ||
		strings.Contains(errorText, "tls") || strings.Contains(errorText, "x509") {
		return metrics.NatsErrorReasonTLS
	}

	if errors.Is(err, nats.ErrAuthorization) || errors.Is(err, nats.ErrAuthExpired) ||
		errors.Is(err, nats.ErrAuthRevoked) || errors.Is(err, nats.ErrAccountAuthExpired) {
		return metrics.NatsErrorReasonAuthentication
	}

	var networkError net.Error
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) ||
		(errors.As(err, &networkError) && networkError.Timeout()) {
		return metrics.NatsErrorReasonTimeout
	}

	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) ||
		errors.Is(err, nats.ErrConnectionClosed) || errors.Is(err, nats.ErrConnectionReconnecting) ||
		errors.Is(err, nats.ErrDisconnected) || errors.Is(err, nats.ErrNoServers) ||
		errors.Is(err, nats.ErrStaleConnection) || errors.As(err, &networkError) {
		return metrics.NatsErrorReasonConnection
	}
	return metrics.NatsErrorReasonOther
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func newNkeyAuthOption(nKeySeed string) (nats.Option, error) {
	kp, err := nkeys.FromSeed([]byte(nKeySeed))
	if err != nil {
		return nil, err
	}
	publicKey, err := kp.PublicKey()
	if err != nil {
		return nil, err
	}
	return nats.Nkey(publicKey, func(nonce []byte) ([]byte, error) {
		return kp.Sign(nonce)
	}), nil
}

func newAuthCalloutAuthOption(oauth2Fqdn, secretsPath string) (nats.Option, error) {
	oauthTokenProvider, err := newOAuthTokenProvider(oauth2Fqdn, secretsPath)
	if err != nil {
		return nil, err
	}
	authCalloutPluginTokenProvider := &authCalloutPluginTokenProvider{oauthTokenProvider}
	return nats.TokenHandler(authCalloutPluginTokenProvider.GetToken), nil
}

type oauthTokenProvider struct {
	tokenSource credentials.PerRPCCredentials
}

func newOAuthTokenProvider(oauth2Fqdn, secretsPath string) (*oauthTokenProvider, error) {
	authnConfig := &auth.AuthnConfig{
		OIDCConfig: &auth.ProviderConfig{
			Host:            oauth2Fqdn,
			CredentialsFile: secretsPath,
			Scopes:          []string{"admin:nats:Worker"},
		},
	}
	tokenSource, err := authnConfig.GRPCClientWithAuth()
	if err != nil {
		return nil, err
	}
	return &oauthTokenProvider{tokenSource: tokenSource}, nil
}

func (t *oauthTokenProvider) GetToken() string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	creds, err := t.tokenSource.GetRequestMetadata(ctx)
	if err != nil {
		zap.L().Warn("failed to get OAuth2 token for nats auth callout plugin", zap.Error(err))
		return ""
	}
	authorization := creds["authorization"]
	authorization = strings.TrimPrefix(authorization, "Bearer ")
	return authorization
}

type authCalloutPluginTokenProvider struct {
	oauthTokenProvider *oauthTokenProvider
}

// token=b64({"account":"$account","pluginName":"$pluginName","payload":"$payload"})
func (t *authCalloutPluginTokenProvider) GetToken() string {
	oauthToken := t.oauthTokenProvider.GetToken()
	tokenJson, err := json.Marshal(struct {
		Account    string `json:"account"`
		PluginName string `json:"pluginName"`
		Payload    string `json:"payload"`
	}{
		Account:    "Worker",
		PluginName: "ssa",
		Payload:    oauthToken,
	})
	if err != nil {
		zap.L().Warn("failed to marshal OAuth2 token for nats auth callout plugin", zap.Error(err))
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(tokenJson)
}
