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
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"go.uber.org/zap"
)

// Model API request bodies are read behind the buffered limit. Only a request
// that takes the streamed path is promoted to the streamed limit, and only
// for the body the limit middleware installed. Every other read, including
// one the promotion step does not reach, keeps the buffered limit.

type limitedRequestBodyKey struct{}

// limitedRequestBody is the buffered-limit body the middleware installs, with
// what promotion needs: the response writer the limit reports to and the
// client body with its count of bytes read.
type limitedRequestBody struct {
	writer   http.ResponseWriter
	raw      *countedRequestBody
	buffered int64
	body     io.ReadCloser
}

// countedRequestBody counts the bytes read from the client body. The server
// serializes reads of a request body, and promotion happens before dispatch,
// so the count needs no lock.
type countedRequestBody struct {
	io.ReadCloser
	read int64
}

func (b *countedRequestBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.read += int64(n)
	return n, err
}

// limitModelRequestBody bounds a model API request body to limit bytes, as
// chi's RequestSize middleware does, and keeps what promoteRequestBody needs
// to raise the bound for a streamed request.
func limitModelRequestBody(limit int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Body == nil {
				next.ServeHTTP(w, r)
				return
			}
			raw := &countedRequestBody{ReadCloser: r.Body}
			limited := &limitedRequestBody{writer: w, raw: raw, buffered: limit, body: http.MaxBytesReader(w, raw, limit)}
			r.Body = limited.body
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), limitedRequestBodyKey{}, limited)))
		})
	}
}

// promoteRequestBody returns the rest of a streamed primary request body
// bounded by limit, counting the bytes already read toward it. It returns
// request.Body unchanged when request.Body is not the body the limit
// middleware installed, the request is a shadow replay, or limit is not above
// the buffered limit. limit is an argument so a per-model limit can be passed
// later.
func promoteRequestBody(request *http.Request, limit int64) io.ReadCloser {
	limited, ok := request.Context().Value(limitedRequestBodyKey{}).(*limitedRequestBody)
	if !ok || request.Body != limited.body || isShadowRequest(request) || limit <= limited.buffered {
		return request.Body
	}
	remaining := limit - limited.raw.read
	if remaining <= 0 {
		return request.Body
	}
	return &promotedRequestBody{
		ReadCloser:    http.MaxBytesReader(limited.writer, limited.raw, remaining),
		ctx:           request.Context(),
		raw:           limited.raw,
		limit:         limit,
		buffered:      limited.buffered,
		contentLength: request.ContentLength,
	}
}

// promotedRequestBody reports overflow with the whole streamed limit and
// counts, once, a body that ends above the buffered limit.
type promotedRequestBody struct {
	io.ReadCloser
	ctx           context.Context
	raw           *countedRequestBody
	limit         int64
	buffered      int64
	contentLength int64
	completed     bool
}

func (b *promotedRequestBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	var maxBytesErr *http.MaxBytesError
	if errors.As(err, &maxBytesErr) {
		return n, &http.MaxBytesError{Limit: b.limit}
	}
	if errors.Is(err, io.EOF) && !b.completed && b.raw.read > b.buffered {
		b.completed = true
		addRequestBodyLarge(b.ctx, requestBodyStreamedComplete, requestBodySizeBucket(b.contentLength), 1)
	}
	return n, err
}

// abandonRequestBody stops a model request body that is rejected unread or
// after the prefix, without reading more of it. On HTTP/1, closing the body
// reads up to 256 KiB of the remaining upload, and so does writing the reply
// on a connection that stays open, so a client that pauses would hold the
// error. There the reply instead closes the connection, and the read deadline
// expires now, so the server's own close of the body after the reply fails at
// once instead of waiting for the rest of the upload. Other bodies are closed
// now.
func abandonRequestBody(request *http.Request) {
	limited, ok := request.Context().Value(limitedRequestBodyKey{}).(*limitedRequestBody)
	if ok && request.ProtoMajor == 1 {
		limited.writer.Header().Set("Connection", "close")
		if err := http.NewResponseController(limited.writer).SetReadDeadline(time.Now()); err != nil {
			zap.L().Debug("could not end the read of a rejected request body", zap.Error(err))
		}
		return
	}
	_ = request.Body.Close()
}
