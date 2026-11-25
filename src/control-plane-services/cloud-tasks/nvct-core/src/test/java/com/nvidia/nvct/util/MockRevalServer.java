/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nvidia.nvct.util;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.absent;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;

import com.github.tomakehurst.wiremock.WireMockServer;
import java.net.URI;
import java.util.List;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@UtilityClass
public class MockRevalServer {
    public static final String POLICY_REJECTED_KIND = "PolicyRejectedKind";
    public static final String POLICY_REJECTED_ERROR = "policy rejected " + POLICY_REJECTED_KIND;
    public static final String POLICY_FAILED_SILENTLY_KIND = "PolicyFailedSilentlyKind";

    private static WireMockServer wireMockServer;

    public static void start(URI listenUrl) {
        stop();

        wireMockServer = new WireMockServer(options().port(listenUrl.getPort()));

        var validationOk = aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody("{\"valid\": true}");

        var validationError = aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody("""
                {"valid": false, "validationErrors": ["spam", "eggs"]}
                """);

        wireMockServer.stubFor(
                post("/v1/validate")
                        .withRequestBody(
                                matchingJsonPath(
                                        "$.helmChart",
                                        containing("invalid-helm-chart")))
                        .willReturn(validationError));
        wireMockServer.stubFor(
                post("/v1/validate")
                        .atPriority(1)
                        .withRequestBody(
                                matchingJsonPath(
                                        "$.validationPolicies[0].allowedExtraKubernetesTypes[0].kind",
                                        equalTo(POLICY_REJECTED_KIND)))
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody("""
                                {"valid": false, "validationErrors": [],
                                 "validationPolicies": [{"id": "task", "valid": false,
                                 "validationErrors": ["%s"]}]}
                                """.formatted(POLICY_REJECTED_ERROR))));
        wireMockServer.stubFor(
                post("/v1/validate")
                        .atPriority(1)
                        .withRequestBody(
                                matchingJsonPath(
                                        "$.validationPolicies[0].allowedExtraKubernetesTypes[0].kind",
                                        equalTo(POLICY_FAILED_SILENTLY_KIND)))
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody("""
                                {"valid": true, "validationErrors": [],
                                 "validationPolicies": [{"id": "task", "valid": false, "validationErrors": []}]}
                                """)));
        wireMockServer.stubFor(
                post("/v1/validate")
                        .atPriority(4)
                        .withRequestBody(
                                matchingJsonPath("$.configuration.fail", equalTo("fail")))
                        .willReturn(validationError));
        wireMockServer.stubFor(
                post("/v1/validate")
                        .withRequestBody(
                                matchingJsonPath(
                                        "$.helmChart",
                                        containing("test-helm-chart")))
                        .willReturn(validationOk));

        wireMockServer.stubFor(
                post("/v1/validate")
                        .withHeader("Authorization", absent())
                        .willReturn(
                                aResponse()
                                        .withStatus(401)
                                        .withBody("Authorization header required")));

        wireMockServer.start();

    }

    public static List<String> validateRequestBodies() {
        return wireMockServer.findAll(postRequestedFor(urlEqualTo("/v1/validate"))).stream()
                .map(r -> r.getBodyAsString())
                .toList();
    }

    public static void resetRequests() {
        wireMockServer.resetRequests();
    }

    public static void stop() {
        if (wireMockServer != null && wireMockServer.isRunning()) {
            wireMockServer.stop();
        }
    }
}
