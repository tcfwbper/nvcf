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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net"
	"net/http"
	"os"
	"reflect"

	echo "github.com/labstack/echo/v4"
)

// requestBodyReadError maps a failed body read to a client error. The cause
// names the connection's addresses, so it stays internal.
func requestBodyReadError(err error) *echo.HTTPError {
	var netErr net.Error
	if errors.Is(err, os.ErrDeadlineExceeded) || (errors.As(err, &netErr) && netErr.Timeout()) {
		return echo.NewHTTPError(http.StatusRequestTimeout, "timed out reading request body").SetInternal(err)
	}
	return echo.NewHTTPError(http.StatusBadRequest, "failed to read request body").SetInternal(err)
}

// requireJSONBody rejects a body that is not a JSON object before any routing
// check, so the client hears what is wrong with the body rather than a
// missing model prefix.
func requireJSONBody(req *http.Request, body []byte) error {
	if err := requireJSONContentType(req); err != nil {
		return err
	}
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "request body must be a JSON object")
	}
	// The binder decodes only the first JSON value, so trailing data would
	// otherwise reach routing.
	if !json.Valid(trimmed) {
		return echo.NewHTTPError(http.StatusBadRequest, "request body must be valid JSON")
	}
	if trimmed[0] != '{' {
		return echo.NewHTTPError(http.StatusBadRequest, "request body must be a JSON object")
	}
	return nil
}

func requireJSONContentType(req *http.Request) error {
	mediaType, _, err := mime.ParseMediaType(requestContentType(req))
	if err != nil || mediaType != echo.MIMEApplicationJSON {
		return echo.NewHTTPError(http.StatusUnsupportedMediaType, "Content-Type must be application/json")
	}
	return nil
}

// requestDecodeError maps a failure to decode the request body to a client
// error that names the field and the expected JSON type, not the Go type.
func requestDecodeError(err error) error {
	var httpErr *echo.HTTPError
	if errors.As(err, &httpErr) && httpErr.Code == http.StatusUnsupportedMediaType {
		return echo.NewHTTPError(http.StatusUnsupportedMediaType, "Content-Type must be application/json")
	}
	return echo.NewHTTPError(http.StatusBadRequest, requestDecodeMessage(err)).SetInternal(err)
}

func requestDecodeMessage(err error) string {
	return jsonDecodeMessage(err, "request body", "")
}

// jsonDecodeMessage describes why subject failed to decode. fieldPrefix
// qualifies field names when subject is nested in a larger request.
func jsonDecodeMessage(err error, subject, fieldPrefix string) string {
	var typeErr *json.UnmarshalTypeError
	var syntaxErr *json.SyntaxError
	switch {
	case errors.As(err, &typeErr):
		if typeErr.Field == "" {
			return subject + " must be a JSON object"
		}
		return fmt.Sprintf("field %q must be %s", fieldPrefix+typeErr.Field, jsonTypeDescription(typeErr.Type))
	case errors.As(err, &syntaxErr), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, io.EOF):
		return subject + " must be valid JSON"
	}
	// Remaining errors come from the request models' own validation.
	for {
		var httpErr *echo.HTTPError
		if !errors.As(err, &httpErr) {
			return err.Error()
		}
		if httpErr.Internal == nil {
			if message, ok := httpErr.Message.(string); ok {
				return message
			}
			return http.StatusText(httpErr.Code)
		}
		err = httpErr.Internal
	}
}

func jsonTypeDescription(t reflect.Type) string {
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == nil {
		return "a different JSON type"
	}
	switch t.Kind() {
	case reflect.Bool:
		return "a boolean"
	case reflect.String:
		return "a string"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		shift := 64 - t.Bits()
		return fmt.Sprintf("an integer from %d to %d", int64(math.MinInt64)>>shift, int64(math.MaxInt64)>>shift)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return fmt.Sprintf("an integer from 0 to %d", uint64(math.MaxUint64)>>(64-t.Bits()))
	case reflect.Float32, reflect.Float64:
		return "a number"
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return "a base64 string"
		}
		return "an array"
	case reflect.Array:
		return "an array"
	case reflect.Map, reflect.Struct:
		return "an object"
	default:
		return "a different JSON type"
	}
}
