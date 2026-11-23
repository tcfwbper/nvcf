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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Ticker;
import com.nvidia.icms.errors.IcmsAuthenticationException;
import com.nvidia.icms.outbound.apikeys.model.ApiKeyValidationResult;
import com.nvidia.icms.outbound.exception.ApiKeysException;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
// API Key Service
public class ApiKeysService {

    public static final String API_KEY_FAILURE_ERROR = "Invalid authentication token";

    private final ApiKeysClient apiKeysClient;
    private final LoadingCache<String, ApiKeyValidationResult> apiKeysCache;
    private final Cache<String, ApiKeyValidationResult> apiKeysBackupCache;

    @Autowired
    public ApiKeysService(ApiKeysClient apiKeysClient) {
        this(apiKeysClient, Ticker.systemTicker());
    }

    ApiKeysService(ApiKeysClient apiKeysClient, Ticker ticker) {
        this.apiKeysClient = apiKeysClient;
        this.apiKeysCache = Caffeine.newBuilder()
                .maximumSize(512).expireAfterWrite(Duration.ofMinutes(1))
                .ticker(ticker)
                .scheduler(Scheduler.systemScheduler())
                .build(this::loadValidationResult);
        this.apiKeysBackupCache = Caffeine.newBuilder()
                .maximumSize(512).expireAfterWrite(Duration.ofMinutes(60))
                .ticker(ticker)
                .scheduler(Scheduler.systemScheduler())
                .build();
    }

    public ApiKeyValidationResult fetchValidationResult(String apiKey) {
        return apiKeysCache.get(apiKey);
    }

    private ApiKeyValidationResult loadValidationResult(String apiKey) {
        ApiKeyValidationResult policyResult;
        try {
            policyResult = apiKeysClient.fetchApiKeyValidationResult(apiKey);
        } catch (ApiKeysException e) {
            if (!e.isRetryable()) {
                apiKeysBackupCache.invalidate(apiKey);
                throw e;
            }
            var cached = apiKeysBackupCache.getIfPresent(apiKey);
            if (cached == null) {
                log.warn("API Keys is unavailable and no cached validation result exists");
                throw e;
            }
            log.warn("Using cached API key validation result while API Keys is unavailable");
            return cached;
        } catch (IcmsAuthenticationException e) {
            apiKeysBackupCache.invalidate(apiKey);
            throw e;
        }

        if (policyResult == null || !policyResult.isValid()) {
            apiKeysBackupCache.invalidate(apiKey);
            log.warn("ApiKey validation failure: Invalid authz result");
            throw new IcmsAuthenticationException(API_KEY_FAILURE_ERROR);
        }
        apiKeysBackupCache.put(apiKey, policyResult);
        return policyResult;
    }
}
