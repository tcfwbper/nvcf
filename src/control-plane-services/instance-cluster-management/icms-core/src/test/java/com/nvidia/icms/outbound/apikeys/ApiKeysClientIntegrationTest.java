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
package com.nvidia.icms.outbound.apikeys;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.exactly;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.nvidia.icms.configuration.staticclientauth.StaticClientAuthConfiguration.StaticClientApiKeysProperties;
import com.nvidia.icms.outbound.exception.ApiKeysException;
import com.nvidia.icms.util.IcmsOAuth2ClientUtils;
import com.nvidia.icms.util.IcmsOAuth2ClientUtils.ManagedHttpResources;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.web.reactive.function.client.WebClient;
import tools.jackson.databind.json.JsonMapper;

class ApiKeysClientIntegrationTest {

    private WireMockServer server;
    private ManagedHttpResources resources;

    @BeforeEach
    void startServer() {
        server = new WireMockServer(wireMockConfig().dynamicPort());
        server.start();
        resources = IcmsOAuth2ClientUtils.getClientHttpConnectorManaged("api-keys-test");
    }

    @AfterEach
    void stopServer() {
        resources.close();
        server.stop();
    }

    @Test
    void staticTokenIsUsedForPolicyEvaluation() {
        var properties = new StaticClientApiKeysProperties();
        properties.setToken("static-api-keys-token");
        var path = "/v1/namespaces/icms/evaluations/apikey.allow";
        server.stubFor(post(urlPathEqualTo(path)).willReturn(aResponse().withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody("""
                        {"namespace":"icms","rule_name":"apikey.allow",
                         "result":{"allowed":true,"ncaId":"account-1","ownerId":"owner-1",
                           "policy":{"aud":"icms-api","scopes":["cluster_listing"],
                                     "resources":[]}}}
                        """)));

        var client = new ApiKeysClient(server.baseUrl(), "icms", "apikey", "allow",
                "apiKey", "unused", "unused", "pdp-evaluate", server.baseUrl() + "/token",
                Optional.of(properties),
                new JsonMapper(), resources, WebClient.builder());
        var result = client.fetchApiKeyValidationResult("nvapi-example");

        assertTrue(result.isValid());
        server.verify(postRequestedFor(urlPathEqualTo(path))
                .withHeader("Authorization", equalTo("Bearer static-api-keys-token"))
                .withRequestBody(equalToJson("""
                        {"input":{"apiKey":"nvapi-example"}}
                        """)));
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 401, 403, 404})
    void clientErrorsRemainDistinctAndAreNotRetried(int status) {
        var properties = new StaticClientApiKeysProperties();
        properties.setToken("static-api-keys-token");
        var path = "/v1/namespaces/icms/evaluations/apikey.allow";
        server.stubFor(post(urlPathEqualTo(path))
                .willReturn(aResponse().withStatus(status).withBody("key not found")));
        var client = new ApiKeysClient(server.baseUrl(), "icms", "apikey", "allow",
                "apiKey", "unused", "unused", "pdp-evaluate", server.baseUrl() + "/token",
                Optional.of(properties),
                new JsonMapper(), resources, WebClient.builder());

        ApiKeysException exception = assertThrows(ApiKeysException.class, () ->
                client.fetchApiKeyValidationResult("nvapi-invalid"));
        assertFalse(exception.isRetryable());
        assertTrue(exception.getMessage().contains(Integer.toString(status)));
        server.verify(exactly(1), postRequestedFor(urlPathEqualTo(path)));
    }

    @Test
    void unavailableServiceReturnsRetryableFailure() {
        var properties = new StaticClientApiKeysProperties();
        properties.setToken("static-api-keys-token");
        server.stubFor(post(urlPathEqualTo("/v1/namespaces/icms/evaluations/apikey.allow"))
                .willReturn(aResponse().withStatus(503)));
        var client = new ApiKeysClient(server.baseUrl(), "icms", "apikey", "allow",
                "apiKey", "unused", "unused", "pdp-evaluate", server.baseUrl() + "/token",
                Optional.of(properties),
                new JsonMapper(), resources, WebClient.builder());

        ApiKeysException exception = assertThrows(ApiKeysException.class, () ->
                client.fetchApiKeyValidationResult("nvapi-example"));
        assertTrue(exception.isRetryable());
        server.verify(exactly(3), postRequestedFor(
                urlPathEqualTo("/v1/namespaces/icms/evaluations/apikey.allow")));
    }

    @Test
    void unprocessableEvaluationIsNotRetryable() {
        var properties = new StaticClientApiKeysProperties();
        properties.setToken("static-api-keys-token");
        server.stubFor(post(urlPathEqualTo("/v1/namespaces/icms/evaluations/apikey.allow"))
                .willReturn(aResponse().withStatus(422).withBody("invalid evaluation input")));
        var client = new ApiKeysClient(server.baseUrl(), "icms", "apikey", "allow",
                "apiKey", "unused", "unused", "pdp-evaluate", server.baseUrl() + "/token",
                Optional.of(properties), new JsonMapper(), resources, WebClient.builder());

        ApiKeysException exception = assertThrows(ApiKeysException.class, () ->
                client.fetchApiKeyValidationResult("nvapi-example"));
        assertFalse(exception.isRetryable());
        server.verify(exactly(1), postRequestedFor(
                urlPathEqualTo("/v1/namespaces/icms/evaluations/apikey.allow")));
    }

    @Test
    void deniedPolicyIsReturnedForServiceValidation() {
        var properties = new StaticClientApiKeysProperties();
        properties.setToken("static-api-keys-token");
        server.stubFor(post(urlPathEqualTo("/v1/namespaces/icms/evaluations/apikey.allow"))
                .willReturn(aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                                {"namespace":"icms","rule_name":"apikey.allow",
                                 "result":{"allowed":false}}
                                """)));
        var client = new ApiKeysClient(server.baseUrl(), "icms", "apikey", "allow",
                "apiKey", "unused", "unused", "pdp-evaluate", server.baseUrl() + "/token",
                Optional.of(properties),
                new JsonMapper(), resources, WebClient.builder());

        assertFalse(client.fetchApiKeyValidationResult("nvapi-invalid").isAllowed());
    }

    @Test
    void absentPolicyResultIsNotRetryable() {
        var properties = new StaticClientApiKeysProperties();
        properties.setToken("static-api-keys-token");
        server.stubFor(post(urlPathEqualTo("/v1/namespaces/icms/evaluations/apikey.allow"))
                .willReturn(aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"namespace\":\"icms\",\"rule_name\":\"apikey.allow\"}")));
        var client = new ApiKeysClient(server.baseUrl(), "icms", "apikey", "allow",
                "apiKey", "unused", "unused", "pdp-evaluate", server.baseUrl() + "/token",
                Optional.of(properties),
                new JsonMapper(), resources, WebClient.builder());

        var exception = assertThrows(ApiKeysException.class,
                () -> client.fetchApiKeyValidationResult("nvapi-invalid"));
        assertEquals("Unexpected response from ApiKeys service. No body returned.",
                exception.getMessage());
        assertFalse(exception.isRetryable());
    }
}
