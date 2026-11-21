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

package api

import (
	"errors"
	"net/http"

	echo "github.com/labstack/echo/v4"

	"github.com/dsx-ai-factory/nvcf/src/invocation-plane-services/llm-gateway/provider"
)

func providerHTTPError(err error) error {
	var httpErr *echo.HTTPError
	if errors.As(err, &httpErr) {
		return httpErr
	}
	// A backend that rejects the request after the 200 puts the status in its
	// error event; the client gets it as if the backend had answered with it.
	var streamErr *provider.UpstreamStreamError
	if errors.As(err, &streamErr) && isUpstreamClientError(streamErr.Status) {
		return provider.NewUpstreamHTTPError(streamErr.Status, nil, streamErr.Body)
	}
	if errors.Is(err, provider.ErrStreamIncomplete) {
		return echo.NewHTTPError(http.StatusBadGateway, msgUpstreamStreamIncomplete).SetInternal(err)
	}

	// Transport errors carry internal addresses; keep them for the request log only.
	return echo.NewHTTPError(http.StatusBadGateway, "upstream request failed").SetInternal(err)
}

// isUpstreamClientError reports whether an upstream error event's status is a
// client error to pass on. 499 means the client went away.
func isUpstreamClientError(status int) bool {
	return status >= http.StatusBadRequest && status < statusClientClosedRequest
}
