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

import com.nvidia.boot.exceptions.BootResponseException;
import com.nvidia.boot.exceptions.UpstreamException;
import com.nvidia.icms.configuration.staticclientauth.FixedBearerExchangeFilterFunction;
import com.nvidia.icms.configuration.staticclientauth.StaticClientAuthConfiguration.StaticClientApiKeysProperties;
import com.nvidia.icms.outbound.apikeys.model.ApiKeyValidationRequest;
import com.nvidia.icms.outbound.apikeys.model.ApiKeyValidationResponse;
import com.nvidia.icms.outbound.apikeys.model.ApiKeyValidationResult;
import com.nvidia.icms.outbound.exception.ApiKeysException;
import com.nvidia.icms.util.IcmsOAuth2ClientUtils;
import com.nvidia.icms.util.IcmsOAuth2ClientUtils.ManagedHttpResources;
import java.time.Duration;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;
import tools.jackson.databind.json.JsonMapper;

@Component
@RefreshScope
@Slf4j
public class ApiKeysClient {

    private static final String CLIENT_REGISTRATION_ID = "api-keys";
    private static final RetryBackoffSpec RETRY_SPEC = Retry.backoff(2, Duration.ofMillis(200))
            .jitter(0.75)
            .filter(UpstreamException.class::isInstance)
            .onRetryExhaustedThrow((spec, signal) -> signal.failure());

    private final WebClient webClient;
    private final JsonMapper jsonMapper;
    private final String evaluationUri;
    private final String requestPropertyName;

    public ApiKeysClient(
            @Value("${icms.api-keys.url}") String apiKeysBaseUrl,
            @Value("${icms.api-keys.namespace}") String namespace,
            @Value("${icms.api-keys.package-name}") String packageName,
            @Value("${icms.api-keys.policy-name}") String policyName,
            @Value("${icms.api-keys.request-property-name:apiKey}") String requestPropertyName,
            @Value("${spring.security.oauth2.client.registration.api-keys.client-id}") String clientId,
            @Value("${spring.security.oauth2.client.registration.api-keys.client-secret}") String clientSecret,
            @Value("${spring.security.oauth2.client.registration.api-keys.scope}") String scope,
            @Value("${spring.security.oauth2.client.provider.api-keys.token-uri}") String tokenUri,
            Optional<StaticClientApiKeysProperties> staticClientApiKeysProperties,
            JsonMapper jsonMapper,
            @Qualifier("apiKeysHttpResources") ManagedHttpResources httpResources,
            WebClient.Builder webClientBuilder) { // Prototype-scoped - Safe to mutate.

        this.jsonMapper = jsonMapper;
        this.evaluationUri = "/v1/namespaces/%s/evaluations/%s.%s"
                .formatted(namespace, packageName, policyName);
        this.requestPropertyName = requestPropertyName;

        ExchangeFilterFunction authFilter = staticClientApiKeysProperties
                .map(p -> (ExchangeFilterFunction) new FixedBearerExchangeFilterFunction(p::getToken))
                .orElseGet(() -> IcmsOAuth2ClientUtils.getOAuth2ExchangeFilter(
                        webClientBuilder, CLIENT_REGISTRATION_ID, tokenUri,
                        clientId, clientSecret, scope));
        var builder = webClientBuilder
                .baseUrl(apiKeysBaseUrl)
                .clientConnector(httpResources.connector())
                .filter(IcmsOAuth2ClientUtils.getRetryableFilter(CLIENT_REGISTRATION_ID))
                .filter(authFilter);
        this.webClient = builder.build();
    }

    public ApiKeyValidationResult fetchApiKeyValidationResult(String apiKey) {
        try {
            ApiKeyValidationResponse response = webClient.post()
                    .uri(evaluationUri)
                    .accept(MediaType.APPLICATION_JSON)
                    .bodyValue(ApiKeyValidationRequest.builder()
                            .jsonField(requestPropertyName, apiKey).build())
                    .retrieve()
                    .onStatus(HttpStatusCode::is4xxClientError, rejected -> {
                        log.warn("4xx error from API Keys for {}: {}", evaluationUri,
                                rejected.statusCode());
                        return rejected.createException();
                    })
                    .onStatus(HttpStatusCode::is5xxServerError, rejected -> {
                        log.warn("5xx error from API Keys for {}: {}", evaluationUri,
                                rejected.statusCode());
                        return Mono.error(new UpstreamException("API Keys returned 5xx error"));
                    })
                    .bodyToMono(ApiKeyValidationResponse.class)
                    .retryWhen(RETRY_SPEC)
                    .block();
            return parseResponse(response);

        } catch (WebClientResponseException e) {
            throw new ApiKeysException("ApiKeys service error: " + e.getStatusCode(), e,
                    e.getStatusCode().is5xxServerError());

        } catch (WebClientRequestException e) {
            throw new ApiKeysException("ApiKeys service is unreachable", e, true);
        } catch (UpstreamException e) {
            throw new ApiKeysException("ApiKeys service error", e,
                    !(e.getCause() instanceof BootResponseException cause
                            && cause.getStatusCode().is4xxClientError()));
        } catch (ApiKeysException e) {
            throw e;
        } catch (Exception e) {
            throw new ApiKeysException("Unknown error from ApiKeys service.", e);
        }
    }

    private ApiKeyValidationResult parseResponse(ApiKeyValidationResponse response) {
        if (response == null || response.getResult() == null) {
            throw new ApiKeysException("Unexpected response from ApiKeys service. No body returned.");
        }
        try {
            return jsonMapper.convertValue(response.getResult(), ApiKeyValidationResult.class);
        } catch (Exception exception) {
            throw new ApiKeysException("Failed to parse user details.", exception);
        }
    }
}
