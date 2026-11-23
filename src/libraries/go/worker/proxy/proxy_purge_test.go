/*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	pb "github.com/dsx-ai-factory/nvcf/src/libraries/go/worker/proto/nvcf"
	"github.com/dsx-ai-factory/nvcf/src/libraries/go/worker/test/testutils"
)

// TestPurgeFromStream_MissingStreamReturnsError checks that an error in the
// JetStream API reply is returned rather than dropped.
func TestPurgeFromStream_MissingStreamReturnsError(t *testing.T) {
	setupLogger()
	cluster, err := testutils.NewNatsSuperCluster(t)
	require.NoError(t, err)
	defer cluster.Shutdown()

	nc, err := nats.Connect(cluster.Clusters[0].Servers[0].ClientURL())
	require.NoError(t, err)
	defer nc.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	err = PurgeFromStream(ctx, nc, "no_such_stream", jetstream.WithPurgeSubject("no.such.subject"))
	var apiErr *jetstream.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, jetstream.JSErrCodeStreamNotFound, apiErr.ErrorCode)
}

// heldPublishJS holds the first publish until release is closed, like a
// publish the server has received but not stored yet. It is stored once
// released even if the caller stopped waiting.
type heldPublishJS struct {
	jetstream.JetStream
	once     sync.Once
	sent     chan struct{}
	release  chan struct{}
	stored   chan struct{}
	storeErr error // set before stored is closed
}

func (h *heldPublishJS) Publish(ctx context.Context, subject string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	first := false
	h.once.Do(func() { first = true })
	if !first {
		return h.JetStream.Publish(ctx, subject, data, opts...)
	}
	close(h.sent)
	type result struct {
		ack *jetstream.PubAck
		err error
	}
	done := make(chan result, 1)
	go func() {
		<-h.release
		ack, err := h.JetStream.Publish(context.Background(), subject, data, opts...)
		h.storeErr = err
		close(h.stored)
		done <- result{ack, err}
	}()
	select {
	case r := <-done:
		return r.ack, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// TestRegistrationRemovedAfterInFlightPublish checks that a registration
// publish still in flight when Proxy stops cannot land after the purge.
func TestRegistrationRemovedAfterInFlightPublish(t *testing.T) {
	setupLogger()
	cluster, err := testutils.NewNatsSuperCluster(t)
	require.NoError(t, err)
	defer cluster.Shutdown()

	nc, err := nats.Connect(cluster.Clusters[0].Servers[0].ClientURL())
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)

	region := cluster.Clusters[0].Region
	streamName := "stateful_session_lookup_" + region
	stream, err := js.CreateStream(t.Context(), jetstream.StreamConfig{
		Name:              streamName,
		Subjects:          []string{"stateful_session.lookup." + region + ".>"},
		MaxMsgsPerSubject: 1,
		Storage:           jetstream.MemoryStorage,
	})
	require.NoError(t, err)

	held := &heldPublishJS{JetStream: js, sent: make(chan struct{}), release: make(chan struct{}), stored: make(chan struct{})}
	p := &HttpProxy{nc: nc, js: held, functionId: uuid.New().String(), functionVersionId: uuid.New().String()}
	work := &pb.WorkerInvokeFunctionRequest{RequestId: uuid.New().String(), NcaId: "nca-1"}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.keepaliveReconnectRegistration(ctx, work, region)
	}()

	<-held.sent
	cancel()
	// Long enough for a keepalive that stops waiting on cancel to purge first.
	time.Sleep(200 * time.Millisecond)
	close(held.release)
	<-held.stored
	require.NoError(t, held.storeErr, "the held publish must be stored, or an empty stream proves nothing")
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("keepalive did not return")
	}

	info, err := stream.Info(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(0), info.State.Msgs, "a publish stored after the purge left the registration behind")
}
