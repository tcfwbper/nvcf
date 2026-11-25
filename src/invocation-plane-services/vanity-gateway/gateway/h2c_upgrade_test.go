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

package gateway

import (
	"bufio"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// h2cUpgradeHeaders ask for an upgrade to HTTP/2 over cleartext (RFC 7540
// section 3.2). The settings payload is the one curl sends.
const h2cUpgradeHeaders = "Connection: Upgrade, HTTP2-Settings\r\nUpgrade: h2c\r\nHTTP2-Settings: AAMAAABkAAQAoAAAAAIAAAAA\r\n"

// upgradeUpstream records what a model request delivered to the upstream.
type upgradeUpstream struct {
	calls      atomic.Int32
	firstBytes chan struct{}
	reads      chan upstreamBodyRead
	headers    chan http.Header
}

func newUpgradeUpstream() *upgradeUpstream {
	return &upgradeUpstream{firstBytes: make(chan struct{}), reads: make(chan upstreamBodyRead, 1), headers: make(chan http.Header, 1)}
}

// ServeHTTP signals once the first body bytes arrive, then reads the rest.
func (u *upgradeUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	u.calls.Add(1)
	u.headers <- r.Header.Clone()
	hash := sha256.New()
	first := make([]byte, 1)
	n, err := io.ReadFull(r.Body, first)
	close(u.firstBytes)
	hash.Write(first[:n])
	rest, copyErr := io.Copy(hash, r.Body)
	read := upstreamBodyRead{length: int64(n) + rest, err: copyErr}
	if err != nil {
		read.err = err
	}
	copy(read.sum[:], hash.Sum(nil))
	u.reads <- read
	w.WriteHeader(http.StatusOK)
}

// writeUpgradePost writes the head of a POST to path that asks for an h2c
// upgrade, with a declared length or chunked.
func writeUpgradePost(t *testing.T, conn net.Conn, path string, length int, knownLength bool) {
	t.Helper()
	framing := "Transfer-Encoding: chunked\r\n"
	if knownLength {
		framing = fmt.Sprintf("Content-Length: %d\r\n", length)
	}
	_, err := io.WriteString(conn, "POST "+path+" HTTP/1.1\r\nHost: "+overLimitTestHost+"\r\nContent-Type: application/json\r\n"+h2cUpgradeHeaders+framing+"\r\n")
	require.NoError(t, err)
}

// writeBodyPart writes part of a body, as a chunk when the length is unknown.
func writeBodyPart(conn net.Conn, part string, knownLength bool) error {
	if knownLength {
		_, err := io.WriteString(conn, part)
		return err
	}
	_, err := fmt.Fprintf(conn, "%x\r\n%s\r\n", len(part), part)
	return err
}

// TestH2CUpgradeWithBodyIsServedOverHTTP1 sends model requests with a body
// that ask for an h2c upgrade, on the production handler and a real
// listener. The upgrade is ignored, so the body goes through the same limits
// and streaming as a plain HTTP/1.1 request instead of being read whole first.
func TestH2CUpgradeWithBodyIsServedOverHTTP1(t *testing.T) {
	for _, knownLength := range []bool{true, false} {
		t.Run(fmt.Sprintf("streamed known length %v", knownLength), func(t *testing.T) {
			upstream := newUpgradeUpstream()
			addr := assembledSplitLimitServer(t, splitTestBuffered, splitTestStreamed, upstream)
			body := earlyFieldsBody(prefixTestModel, false, splitTestBetween)
			conn, err := net.Dial("tcp", addr)
			require.NoError(t, err)
			defer conn.Close()
			require.NoError(t, conn.SetDeadline(time.Now().Add(30*time.Second)))
			writeUpgradePost(t, conn, "/v1/chat/completions", len(body), knownLength)

			// Send only the routing prefix and a little more, then wait for
			// the upstream to receive body bytes. A server that reads the
			// whole body before routing never forwards any of it here.
			head := 2 * requestPrefixLimit
			require.NoError(t, writeBodyPart(conn, body[:head], knownLength))
			select {
			case <-upstream.firstBytes:
			case <-time.After(10 * time.Second):
				t.Fatal("the upstream got no body bytes before the upload ended: the body was read in full before routing")
			}
			require.NoError(t, writeBodyPart(conn, body[head:], knownLength))
			if !knownLength {
				_, err = io.WriteString(conn, "0\r\n\r\n")
				require.NoError(t, err)
			}

			resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
			require.NoError(t, err)
			_ = resp.Body.Close()
			assert.Equal(t, "HTTP/1.1", resp.Proto)
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			read := <-upstream.reads
			require.NoError(t, read.err)
			assert.Equal(t, int64(len(body)), read.length)
			assert.Equal(t, sha256.Sum256([]byte(body)), read.sum)
			forwarded := <-upstream.headers
			assert.Empty(t, forwarded.Get("Upgrade"), "the ignored upgrade must not reach the upstream")
			assert.Empty(t, forwarded.Get("HTTP2-Settings"))
		})
	}

	lateFields := `{"input":"` + filler(splitTestBetween) + `","model":"` + prefixTestModel + `","stream":false}`
	for _, knownLength := range []bool{true, false} {
		t.Run(fmt.Sprintf("fallback known length %v", knownLength), func(t *testing.T) {
			upstream := newUpgradeUpstream()
			addr := assembledSplitLimitServer(t, splitTestBuffered, splitTestStreamed, upstream)
			conn, err := net.Dial("tcp", addr)
			require.NoError(t, err)
			defer conn.Close()
			require.NoError(t, conn.SetDeadline(time.Now().Add(30*time.Second)))
			writeUpgradePost(t, conn, "/v1/chat/completions", len(lateFields), knownLength)
			go func() {
				if writeBodyPart(conn, lateFields, knownLength) == nil && !knownLength {
					_, _ = io.WriteString(conn, "0\r\n\r\n")
				}
			}()

			resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
			require.NoError(t, err)
			payload, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Equal(t, "HTTP/1.1", resp.Proto)
			requireBodyLimitError(t, "/v1/chat/completions", resp.StatusCode, string(payload), splitTestBuffered)
			assert.Zero(t, upstream.calls.Load())
		})
	}
}

// TestH2CUpgradeWithoutBodyStillUpgrades checks that a request without a body
// that asks for an h2c upgrade still switches to HTTP/2 and gets its answer
// on stream 1.
func TestH2CUpgradeWithoutBodyStillUpgrades(t *testing.T) {
	addr := assembledSplitLimitServer(t, splitTestBuffered, splitTestStreamed, newUpgradeUpstream())
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))
	_, err = io.WriteString(conn, "GET /v1/models HTTP/1.1\r\nHost: "+overLimitTestHost+"\r\n"+h2cUpgradeHeaders+"\r\n")
	require.NoError(t, err)
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)

	_, err = io.WriteString(conn, http2.ClientPreface)
	require.NoError(t, err)
	framer := http2.NewFramer(conn, br)
	require.NoError(t, framer.WriteSettings())
	status := ""
	decoder := hpack.NewDecoder(4096, func(f hpack.HeaderField) {
		if f.Name == ":status" {
			status = f.Value
		}
	})
	for status == "" {
		frame, err := framer.ReadFrame()
		require.NoError(t, err)
		if headers, ok := frame.(*http2.HeadersFrame); ok && headers.StreamID == 1 {
			_, err = decoder.Write(headers.HeaderBlockFragment())
			require.NoError(t, err)
		}
	}
	assert.Equal(t, "200", status)
}

// TestH2CPriorKnowledgeIsUnchanged sends a streamed body above the buffered
// limit over HTTP/2 with prior knowledge, which never uses the upgrade.
func TestH2CPriorKnowledgeIsUnchanged(t *testing.T) {
	upstream := newUpgradeUpstream()
	addr := assembledSplitLimitServer(t, splitTestBuffered, splitTestStreamed, upstream)
	body := earlyFieldsBody(prefixTestModel, false, splitTestBetween)
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/v1/chat/completions", strings.NewReader(body))
	require.NoError(t, err)
	req.Host = overLimitTestHost
	req.Header.Set("Content-Type", "application/json")
	resp, err := h2cClient().Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, "HTTP/2.0", resp.Proto)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	read := <-upstream.reads
	require.NoError(t, read.err)
	assert.Equal(t, sha256.Sum256([]byte(body)), read.sum)
}

func TestRemoveH2CUpgrade(t *testing.T) {
	tests := []struct {
		name                        string
		connection, upgrade         string
		wantConnection, wantUpgrade string
	}{
		{"h2c only", "Upgrade, HTTP2-Settings", "h2c", "", ""},
		{"mixed case and spacing", "keep-alive ,upgrade,http2-settings", "H2C", "keep-alive", ""},
		{"another protocol kept", "Upgrade, HTTP2-Settings", "h2c, example/1", "Upgrade", "example/1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{"Connection": {tc.connection}, "Upgrade": {tc.upgrade}, "Http2-Settings": {"AAMAAABkAAQAoAAAAAIAAAAA"}}
			removeH2CUpgrade(h)
			assert.Equal(t, tc.wantConnection, h.Get("Connection"))
			assert.Equal(t, tc.wantUpgrade, h.Get("Upgrade"))
			assert.Empty(t, h.Values("HTTP2-Settings"))
		})
	}
}
