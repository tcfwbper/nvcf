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
	"errors"
	"fmt"
	"github.com/jellydator/ttlcache/v3"
	"io"
	"math/rand/v2"
	"net"
	"time"

	nverrors "github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/nvkit/errors"
	"github.com/google/uuid"
	grpcretry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	grpcCodes "google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"nvcf-grpc-proxy/nvcf/pb"
	"nvcf-grpc-proxy/proxy/metrics"
)

var otelFieldCount = len(otel.GetTextMapPropagator().Fields())
var ErrSessionNotFound = nverrors.NewNVError(errors.New("no existing session found")).WithCode(grpcCodes.NotFound)

type geoLookuper interface {
	LookupRegions(ctx context.Context, clientIP net.IP) []string
}

type rateLimiter interface {
	IsRateLimited(ctx context.Context, ncaId, functionId, functionVersionId string, isSyncCheck bool) bool
}

type FunctionInvoker struct {
	conns *NatsConnections
	// js is on the shared connection, for lookups and purges. dispatchJS is on
	// the dispatch connection, for work items.
	js                  jetstream.JetStream
	dispatchJS          jetstream.JetStream
	nvcfClient          pb.ProxyClient
	connectPaths        ConnectPaths
	region              string
	requestRegistration *StatefulRequestRegistration
	geoLookup           geoLookuper
	rateLimit           rateLimiter
	// Resolved work-stream handles, keyed by stream name. Bounded; see
	// workStreamCacheCapacity.
	workStreams *ttlcache.Cache[string, jetstream.Stream]
	// retryPause replaces the pause before a rejected send is retried. Tests
	// only; nil means sleeping for reconnectPollInterval.
	retryPause func(context.Context)
}

func NewFunctionInvoker(conns *NatsConnections, nvcfClient pb.ProxyClient, connectPaths ConnectPaths, jetstreamPlacementTag, region string, geoLookup geoLookuper, rateLimit rateLimiter) (*FunctionInvoker, error) {
	js, err := jetstream.New(conns.Shared)
	if err != nil {
		return nil, err
	}
	dispatchJS, err := jetstream.New(conns.Dispatch)
	if err != nil {
		return nil, err
	}
	requestRegistration, err := NewStatefulRequestRegistration(context.Background(), js, jetstreamPlacementTag, region)
	if err != nil {
		return nil, err
	}

	workStreams := ttlcache.New(
		ttlcache.WithTTL[string, jetstream.Stream](workStreamCacheTTL),
		ttlcache.WithCapacity[string, jetstream.Stream](workStreamCacheCapacity),
		ttlcache.WithDisableTouchOnHit[string, jetstream.Stream](),
	)
	go workStreams.Start()

	return &FunctionInvoker{
		conns:               conns,
		js:                  js,
		dispatchJS:          dispatchJS,
		nvcfClient:          nvcfClient,
		connectPaths:        connectPaths,
		region:              region,
		requestRegistration: requestRegistration,
		geoLookup:           geoLookup,
		rateLimit:           rateLimit,
		workStreams:         workStreams,
	}, nil
}

func (f *FunctionInvoker) Close() error {
	// Started in NewFunctionInvoker; without this its cleanup goroutine
	// outlives the invoker.
	if f.workStreams != nil {
		f.workStreams.Stop()
	}
	f.js.CleanupPublisher()
	f.dispatchJS.CleanupPublisher()
	// Closed last: the director purges pending work before closing the invoker.
	f.conns.Close()
	if closer, ok := f.rateLimit.(io.Closer); ok {
		_ = closer.Close()
	}
	return nil
}

// Result is a named tuple for clearer identities of the return values. intended to be passed by value.
type Result struct {
	WorkerAuthorizationToken string
	RequestId                uuid.UUID
	// QueuedToWorkStream reports whether this invocation published to the
	// rq work stream, which is the only queue PurgePendingWork can drop from.
	//
	// Streaming versions publish to llsrq and a session join publishes to
	// stateful_session.reconnect, and for a streaming version the rq stream may
	// not exist at all. Recording those as pending work makes every later purge
	// look up a stream that cannot hold the message, which costs a round trip
	// and reports a failure that reads as missing purge rights.
	QueuedToWorkStream bool
	// StreamingDispatch is set for a streaming version, whose request is still
	// being sent when the invoke returns. It receives once: true when a worker
	// answers, false when the request is abandoned first.
	StreamingDispatch <-chan bool
}

// WorkerAuth is what a worker must present when it connects back for a session.
type WorkerAuth struct {
	Token             string
	RequestId         uuid.UUID
	FunctionId        string
	FunctionVersionId string
	// CallerNcaId is the authenticated client's NCA ID, not the function owner's.
	CallerNcaId string
}

func (f *FunctionInvoker) InvokeStatefulFunction(ctx context.Context, conn net.Conn, clientAuth, functionId, functionVersionId string, existingRequestId *uuid.UUID, onWorkerAuthSet func(WorkerAuth)) (Result, context.CancelFunc, error) {
	ctx, span := otel.GetTracerProvider().Tracer("proxy-tracer").Start(ctx, "InvokeStatefulFunction",
		trace.WithAttributes(
			attribute.String("function_id", functionId),
			attribute.String("function_version_id", functionVersionId),
			attribute.Bool("has_existing_request_id", existingRequestId != nil),
		))
	defer span.End()

	startInitTime := time.Now()

	proxyAuthResponse, err := f.nvcfClient.AuthStatefulWork(ctx, &pb.ProxyAuthRequest{
		ClientAuthorizationToken: clientAuth,
		FunctionId:               functionId,
		FunctionVersionId:        emptyAsNil(functionVersionId),
	}, grpcretry.WithMax(3))
	if err != nil {
		return Result{}, nil, err
	}

	// the worker needs to send this token back along with the matching request id to get paired up with the client connection
	workerAuthToken := generateWorkerToken()
	var requestId uuid.UUID
	if existingRequestId == nil {
		requestId = uuid.New()
		span.SetAttributes(attribute.Stringer("request_id", requestId))
		functionVersion, err := pickFunctionVersion(proxyAuthResponse)
		if err != nil {
			return Result{}, nil, err
		}

		metrics.IncrFunctionRequest(proxyAuthResponse.FunctionId, functionVersion.FunctionVersionId, proxyAuthResponse.ClientNcaId)

		if functionVersion.GetHasRateLimit() && f.rateLimit.IsRateLimited(ctx, proxyAuthResponse.ClientNcaId, proxyAuthResponse.FunctionId, functionVersion.FunctionVersionId, functionVersion.GetSyncCheck()) {
			// maps to 429 if non-grpc
			err := nverrors.NewNVError(errors.New("exceeded rate limit")).WithCode(grpcCodes.ResourceExhausted)
			return Result{}, nil, err
		}

		onWorkerAuthSet(WorkerAuth{
			Token:             workerAuthToken,
			RequestId:         requestId,
			FunctionId:        proxyAuthResponse.FunctionId,
			FunctionVersionId: functionVersion.FunctionVersionId,
			CallerNcaId:       proxyAuthResponse.ClientNcaId,
		})

		cancelInvokingWorker, queued, streamingDispatch, err := f.startNewSession(ctx, conn, requestId, proxyAuthResponse, workerAuthToken, functionVersion)
		if err != nil {
			return Result{}, nil, err
		}
		elapsed := time.Since(startInitTime).Seconds()
		metrics.SessionInitTimeCounter.WithLabelValues("false").Observe(elapsed)
		return Result{
			WorkerAuthorizationToken: workerAuthToken,
			RequestId:                requestId,
			QueuedToWorkStream:       queued,
			StreamingDispatch:        streamingDispatch,
		}, cancelInvokingWorker, nil
	} else {
		// if there is an existing request id, check that it matches up with an existing request
		// must match function id, version id if provided, and nca id of the client
		requestId = *existingRequestId
		span.SetAttributes(attribute.Stringer("request_id", requestId))
		existingSession, err := f.requestRegistration.LookupStatefulSession(ctx, requestId)
		if err != nil {
			if errors.Is(err, jetstream.ErrMsgNotFound) {
				return Result{}, nil, fmt.Errorf("%w for request id %s", ErrSessionNotFound, requestId)
			}
			return Result{}, nil, err
		}
		// purposely omitting the mismatched data to not leak session information
		if existingSession.FunctionId != functionId {
			return Result{}, nil, fmt.Errorf("existing session function ID mismatch for request id %s", requestId)
		}
		if functionVersionId != "" && existingSession.FunctionVersionId != functionVersionId {
			return Result{}, nil, fmt.Errorf("existing session function version ID mismatch for request id %s", requestId)
		}
		if proxyAuthResponse.ClientNcaId != existingSession.InvokerNcaId {
			return Result{}, nil, fmt.Errorf("existing session invoker NCA ID mismatch for request id %s", requestId)
		}

		onWorkerAuthSet(WorkerAuth{
			Token:             workerAuthToken,
			RequestId:         requestId,
			FunctionId:        existingSession.FunctionId,
			FunctionVersionId: existingSession.FunctionVersionId,
			CallerNcaId:       proxyAuthResponse.ClientNcaId,
		})

		err = f.joinExistingSession(ctx, requestId, proxyAuthResponse, workerAuthToken)
		if err != nil {
			return Result{}, nil, err
		}
		elapsed := time.Since(startInitTime).Seconds()
		metrics.SessionInitTimeCounter.WithLabelValues("true").Observe(elapsed)
		return Result{
			WorkerAuthorizationToken: workerAuthToken,
			RequestId:                requestId,
		}, nil, nil
	}
}

// SessionResumable reports whether a session can still be rejoined. Its worker
// keeps a registration while the session lives and deletes it once it stops
// waiting for the client to come back.
func (f *FunctionInvoker) SessionResumable(ctx context.Context, requestId uuid.UUID) (bool, error) {
	_, err := f.requestRegistration.LookupStatefulSession(ctx, requestId)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return false, nil
	}
	return err == nil, err
}

func generateWorkerToken() string {
	return lo.RandomString(64, lo.AlphanumericCharset)
}

func pickFunctionVersion(proxyAuthResponse *pb.ProxyAuthResponse) (*pb.ProxyAuthResponse_FunctionVersion, error) {
	if len(proxyAuthResponse.FunctionVersions) <= 0 {
		return nil, fmt.Errorf("missing function versions from nvcf api response")
	}
	return proxyAuthResponse.FunctionVersions[rand.IntN(len(proxyAuthResponse.FunctionVersions))], nil
}

func (f *FunctionInvoker) startNewSession(ctx context.Context, conn net.Conn, requestId uuid.UUID, proxyAuthResponse *pb.ProxyAuthResponse, workerAuthToken string, functionVersion *pb.ProxyAuthResponse_FunctionVersion) (context.CancelFunc, bool, <-chan bool, error) {
	marshalledInvokeFunctionRequest, err := f.marshalStatefulSessionRequest(requestId, proxyAuthResponse, workerAuthToken)
	if err != nil {
		return nil, false, nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	if functionVersion.GetType() == pb.ProxyAuthResponse_FunctionVersion_STREAMING {
		// Buffered so the send never blocks if nobody reads the outcome.
		dispatched := make(chan bool, 1)
		go f.sendLLSInvocationRequest(ctx, conn, requestId, functionVersion, marshalledInvokeFunctionRequest, dispatched)
		// Published to llsrq, which PurgePendingWork cannot drop from, and for
		// a streaming version the rq stream may not exist at all.
		return cancel, false, dispatched, nil
	}
	subject := f.requestStreamSubject(functionVersion.FunctionVersionId, requestId)
	err = f.publishDispatch(ctx, metrics.NatsSendWorkItem, publishTimeout, func(publishCtx context.Context) error {
		_, err := f.dispatchJS.PublishMsg(publishCtx, &nats.Msg{
			Subject: subject,
			Header:  otelHeaders(publishCtx),
			Data:    marshalledInvokeFunctionRequest,
		})
		return err
	})
	if err != nil {
		cancel()
		return nil, false, nil, fmt.Errorf("failed to publish function invocation request to nats: %w", err)
	}
	return cancel, true, nil, nil
}

// sendLLSInvocationRequest reports once on dispatched whether a worker answered.
func (f *FunctionInvoker) sendLLSInvocationRequest(ctx context.Context, conn net.Conn, requestId uuid.UUID, functionVersion *pb.ProxyAuthResponse_FunctionVersion, marshalledInvokeFunctionRequest []byte, dispatched chan<- bool) {
	answered := false
	defer func() { dispatched <- answered }()
	ctx, span := otel.GetTracerProvider().Tracer("proxy-tracer").Start(ctx, "sendLLSInvocationRequest",
		trace.WithAttributes(
			attribute.Stringer("request_id", requestId),
		))
	defer span.End()
	var regions []string
	// check for GFN and non-GFN. only GFN is valid for geo lookup.
	if functionVersion.GetBackendType() != pb.ProxyAuthResponse_FunctionVersion_GFN {
		regions = []string{"default"}
	} else {
		regions = append(f.geoLookup.LookupRegions(ctx, getRemoteIP(conn)), "default")
		span.SetAttributes(
			attribute.StringSlice("lls_routing_regions", regions),
			attribute.String("client_ip", conn.RemoteAddr().String()),
		)
	}
	for i := 0; i < len(regions) && ctx.Err() == nil; {
		err := f.tryRegionForLLS(ctx, regions[i], requestId, functionVersion, marshalledInvokeFunctionRequest)
		if err == nil {
			answered = true
			return
		}
		// short circuit and try the next region
		if !errors.Is(err, nats.ErrNoResponders) &&
			!errors.Is(err, context.Canceled) &&
			!errors.Is(err, context.DeadlineExceeded) &&
			ctx.Err() == nil { // a sub-operation may have timed out, but not our root context
			zap.L().Warn("failed to send lls message", zap.Error(err))
			SleepWithContext(ctx, 3*time.Second)
		}
		// stay on the last region until ctx runs out. it will be the default region.
		if i != len(regions)-1 {
			i++
		} else {
			// rate limit if we've already tried all the regions and are on the default region
			SleepWithContext(ctx, time.Second)
		}
	}
}

func getRemoteIP(conn net.Conn) net.IP {
	// String() for tcp connections returns the port too so we cast to directly get the IP
	if addr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		return addr.IP
	}
	// best effort
	return net.ParseIP(conn.RemoteAddr().String())
}

func (f *FunctionInvoker) tryRegionForLLS(ctx context.Context, region string, requestId uuid.UUID, functionVersion *pb.ProxyAuthResponse_FunctionVersion, marshalledInvokeFunctionRequest []byte) error {
	ctx, span := otel.GetTracerProvider().Tracer("proxy-tracer").Start(ctx, "tryRegionForLLS",
		trace.WithAttributes(
			attribute.String("region", region),
		))
	defer span.End()

	// keep trying to get a worker until either somebody responds and we get cancelled or we
	// get cancelled for some other reason
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// dropped message protection:
	// the root ctx will get cancelled when the client disconnects, but we want a shorter ctx here
	// to account for multiple retries within one client request. a nats request may have been
	// received by a worker but then dropped which would cause a non-shortened-timeout request to
	// continue waiting for a reply until the client eventually leaves rather than retrying.
	subject := llsRequestSubject(region, functionVersion.FunctionVersionId, requestId)
	return f.publishDispatch(ctx, metrics.NatsSendDispatch, 3*time.Second, func(requestCtx context.Context) error {
		_, err := f.conns.Dispatch.RequestMsgWithContext(requestCtx, &nats.Msg{
			Subject: subject,
			Header:  otelHeaders(requestCtx),
			Data:    marshalledInvokeFunctionRequest,
		})
		return err
	})
}

func (f *FunctionInvoker) joinExistingSession(ctx context.Context, requestId uuid.UUID, proxyAuthResponse *pb.ProxyAuthResponse, workerAuthToken string) error {
	marshalledInvokeFunctionRequest, err := f.marshalStatefulSessionRequest(requestId, proxyAuthResponse, workerAuthToken)
	if err != nil {
		return err
	}
	subject := reconnectSubject(requestId)
	err = f.publishDispatch(ctx, metrics.NatsSendSessionJoin, publishTimeout, func(publishCtx context.Context) error {
		return f.conns.Dispatch.PublishMsg(&nats.Msg{
			Subject: subject,
			Header:  otelHeaders(publishCtx),
			Data:    marshalledInvokeFunctionRequest,
		})
	})
	if err != nil {
		return fmt.Errorf("failed to publish function invocation request to nats: %w", err)
	}
	return nil
}

// publishTimeout bounds a work-item or session-join send as a whole: waiting for
// the dispatch connection, retrying rejected attempts, and JetStream's wait for
// the ack. It matches JetStream's default API timeout, which is how long a
// publish already waited for its ack.
const publishTimeout = 5 * time.Second

// publishDispatch sends an LLS dispatch, work item or session join on the
// dispatch connection within timeout, in one span. If nats.go rejects an attempt with
// ErrReconnectBufExceeded because the connection is reconnecting, it pauses,
// waits for the connection and tries again, so a brief outage does not fail the
// send. Every attempt shares the one deadline.
//
// Only ErrReconnectBufExceeded is retried. nats.go returns it before taking any
// of the message's bytes, so retrying cannot also deliver the rejected attempt.
// Other errors, such as a missing ack, can follow a send that reached NATS and
// are returned as they are.
func (f *FunctionInvoker) publishDispatch(ctx context.Context, send string, timeout time.Duration, publish func(context.Context) error) (err error) {
	ctx, span := otel.GetTracerProvider().Tracer("proxy-tracer").Start(ctx, "grpc-proxy.nats_send",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attribute.String("nats.send", send)))
	attempts := 0
	start := time.Now()
	defer func() {
		result := metrics.NatsSendResultSuccess
		if err != nil {
			result = metrics.NatsSendResultError
		}
		metrics.NatsSendDuration.WithLabelValues(send, result).Observe(time.Since(start).Seconds())
		span.SetAttributes(attribute.Int("nats.send.attempts", attempts))
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "")
			span.SetAttributes(attribute.Bool("error", true))
		}
		span.End()
	}()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		if err := f.waitDispatchConnected(ctx, send); err != nil {
			return err
		}
		attempts++
		err := publish(ctx)
		recordPublishFailure(send, err)
		if !errors.Is(err, nats.ErrReconnectBufExceeded) {
			return err
		}
		f.pauseBeforeRetry(ctx)
	}
}

// pauseBeforeRetry runs after a rejected attempt. The callbacks may not have
// marked the connection down yet, so without it the next waitDispatchConnected
// could pass at once and the loop would publish again without stopping.
func (f *FunctionInvoker) pauseBeforeRetry(ctx context.Context) {
	if f.retryPause != nil {
		f.retryPause(ctx)
		return
	}
	SleepWithContext(ctx, reconnectPollInterval)
}

// reconnectPollInterval is how often waitConnected checks the connection.
const reconnectPollInterval = 50 * time.Millisecond

// waitDispatchConnected waits for the dispatch connection before a send and
// counts a failed wait.
func (f *FunctionInvoker) waitDispatchConnected(ctx context.Context, send string) error {
	err := waitConnected(ctx, f.conns.DispatchState)
	if reason := sendFailureReason(err); reason != "" {
		metrics.NatsSendFailures.WithLabelValues(send, metrics.NatsSendStageWait, reason).Inc()
	}
	return err
}

// recordPublishFailure counts an attempt that failed on the dispatch
// connection's state. It is not always a rejection. ErrReconnectBufExceeded is a rejection before any bytes
// were sent, counted even when a later retry sends the message.
// ErrConnectionClosed can come from the publish or from the wait for a reply or
// ack, so NATS may have received that attempt. Other errors, such as no
// responders or a missing ack, are not about the connection and are not
// counted.
func recordPublishFailure(send string, err error) {
	switch {
	case errors.Is(err, nats.ErrReconnectBufExceeded):
		metrics.NatsSendFailures.WithLabelValues(send, metrics.NatsSendStagePublish, metrics.NatsSendReasonReconnecting).Inc()
	case errors.Is(err, nats.ErrConnectionClosed):
		metrics.NatsSendFailures.WithLabelValues(send, metrics.NatsSendStagePublish, metrics.NatsSendReasonClosed).Inc()
	}
}

func sendFailureReason(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return metrics.NatsSendReasonCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return metrics.NatsSendReasonTimeout
	case errors.Is(err, nats.ErrConnectionClosed):
		return metrics.NatsSendReasonClosed
	}
	return ""
}

// waitConnected returns once the connection is up, or with an error once ctx
// ends or the connection is closed. The dispatch connection's sends call it so
// they do not try while it is down. It reads ConnState, so it never waits on the
// connection's lock and always honours ctx.
func waitConnected(ctx context.Context, state *ConnState) error {
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("waiting for nats connection: %w", err)
		}
		if state.Closed() {
			return nats.ErrConnectionClosed
		}
		if state.Connected() {
			return nil
		}
		SleepWithContext(ctx, reconnectPollInterval)
	}
}

func otelHeaders(ctx context.Context) nats.Header {
	carrier := make(NatsHeaderCarrier, otelFieldCount)
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return nats.Header(carrier)
}

func (f *FunctionInvoker) marshalStatefulSessionRequest(requestId uuid.UUID, proxyAuthResponse *pb.ProxyAuthResponse, workerAuthToken string) ([]byte, error) {
	connectionConfigs := make([]*pb.WorkerInvokeFunctionRequest_StatefulConfig_ConnectionConfig, 0, 2)
	if f.connectPaths.HTTP1 != "" {
		connectionConfigs = append(connectionConfigs, &pb.WorkerInvokeFunctionRequest_StatefulConfig_ConnectionConfig{
			Config: &pb.WorkerInvokeFunctionRequest_StatefulConfig_ConnectionConfig_Http1Config{Http1Config: &pb.WorkerInvokeFunctionRequest_StatefulConfig_ConnectionConfig_HTTP1ConnectionConfig{
				ProxyURI:                f.connectPaths.HTTP1,
				ProxyAuthorizationToken: workerAuthToken,
			}},
		})
	}
	if f.connectPaths.HTTP3 != "" {
		connectionConfigs = append(connectionConfigs, &pb.WorkerInvokeFunctionRequest_StatefulConfig_ConnectionConfig{
			Config: &pb.WorkerInvokeFunctionRequest_StatefulConfig_ConnectionConfig_Http3Config{Http3Config: &pb.WorkerInvokeFunctionRequest_StatefulConfig_ConnectionConfig_HTTP3ConnectionConfig{
				ProxyURI:                f.connectPaths.HTTP3,
				ProxyAuthorizationToken: workerAuthToken,
			}},
		})
	}
	invokeFunctionRequest := pb.WorkerInvokeFunctionRequest{
		RequestId: requestId.String(),
		NcaId:     proxyAuthResponse.ClientNcaId,
		Subject:   proxyAuthResponse.ClientAuthSubject,
		StatefulConfig: &pb.WorkerInvokeFunctionRequest_StatefulConfig{
			NvcfRegionOfInvoker: f.region,
			ConnectionConfigs:   connectionConfigs,
		},
		RequestTime: timestamppb.Now(),
	}
	marshalledInvokeFunctionRequest, err := proto.Marshal(&invokeFunctionRequest)
	if err != nil {
		return nil, err
	}
	return marshalledInvokeFunctionRequest, nil
}

// format of a subject is rq.${region}.${function_version}.${request_id}
func (f *FunctionInvoker) requestStreamSubject(functionVersion string, requestId uuid.UUID) string {
	return fmt.Sprintf("rq.%s.%s.%s", f.region, functionVersion, requestId)
}

// format of a subject is stateful_session.reconnect.${request_id}
func reconnectSubject(requestId uuid.UUID) string {
	return fmt.Sprintf("stateful_session.reconnect.%s", requestId)
}

// format of a subject is llsrq.${region}.${function_version}.${request_id}
func llsRequestSubject(region, functionVersion string, requestId uuid.UUID) string {
	return fmt.Sprintf("llsrq.%s.%s.%s", region, functionVersion, requestId)
}

func emptyAsNil(s string) *string {
	var ptr *string
	if s != "" {
		ptr = &s
	}
	return ptr
}

type ConnectPaths struct {
	HTTP1 string
	HTTP3 string
}
