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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Ticker;
import com.nvidia.icms.errors.IcmsAuthenticationException;
import com.nvidia.icms.outbound.exception.ApiKeysException;
import com.nvidia.icms.outbound.apikeys.model.ApiKeyValidationResult;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ApiKeysServiceTest {

    @Mock
    private ApiKeysClient apiKeysClient;
    private ApiKeysService apiKeysService;
    private final AtomicLong now = new AtomicLong();
    private final Ticker ticker = now::get;

    @BeforeEach
    public void setup() {
        apiKeysService = new ApiKeysService(apiKeysClient, ticker);
    }

    private ApiKeyValidationResult getPolicyResult() {
        return ApiKeyValidationResult.builder()
                .allowed(true)
                .ncaId("dummy_nca_id")
                .ownerId("dummy_owner_id")
                .policy(ApiKeyValidationResult.Policy.builder()
                                .product("dummy_product")
                                .resources(List.of(ApiKeyValidationResult.Resource.builder()
                                                           .id("ea946155-893c-43b6-920d-3f4410534a80")
                                                           .type("cluster")
                                                           .build()))
                                .scopes(List.of("nvca-cluster"))
                                .build())
                .build();
    }

    @Test
    void fetchApiKeyValidationResult_success() {
        when(apiKeysClient.fetchApiKeyValidationResult("dummy_api_key"))
                .thenReturn(getPolicyResult());

        ApiKeyValidationResult
                policyResult = apiKeysService.fetchValidationResult("dummy_api_key");

        assertTrue(policyResult.isAllowed());
    }

    @Test
    void fetchApiKeyValidationResult_withInvalidDetails_throwsException() {
        var dummyPolicy = ApiKeyValidationResult.builder()
                .allowed(false)
                .build();
        when(apiKeysClient.fetchApiKeyValidationResult("dummy_api_key"))
                .thenReturn(dummyPolicy);

        IcmsAuthenticationException exception =
                assertThrows(IcmsAuthenticationException.class,
                             () -> apiKeysService.fetchValidationResult("dummy_api_key"));

        assertEquals("Invalid authentication token", exception.getMessage());
    }

    @Test
    void fetchApiKeyValidationResult_withError_throwsException() {
        when(apiKeysClient.fetchApiKeyValidationResult("dummy_api_key"))
                .thenThrow(new ApiKeysException("dummy_error"));

        ApiKeysException exception =
                assertThrows(ApiKeysException.class,
                             () -> apiKeysService.fetchValidationResult("dummy_api_key"));

        assertEquals("dummy_error", exception.getMessage());
    }

    @Test
    void cachesEachValidKeyForOneMinute() {
        when(apiKeysClient.fetchApiKeyValidationResult("dummy_api_key"))
                .thenReturn(getPolicyResult());

        apiKeysService.fetchValidationResult("dummy_api_key");
        apiKeysService.fetchValidationResult("dummy_api_key");
        verify(apiKeysClient).fetchApiKeyValidationResult("dummy_api_key");

        now.addAndGet(TimeUnit.MINUTES.toNanos(1));
        apiKeysService.fetchValidationResult("dummy_api_key");
        verify(apiKeysClient, times(2)).fetchApiKeyValidationResult("dummy_api_key");
    }

    @Test
    void outageUsesOnlySameKeysBackupAfterPrimaryExpires() {
        when(apiKeysClient.fetchApiKeyValidationResult("dummy_api_key"))
                .thenReturn(getPolicyResult())
                .thenThrow(new ApiKeysException("API Keys unavailable", true));
        when(apiKeysClient.fetchApiKeyValidationResult("other_key"))
                .thenThrow(new ApiKeysException("API Keys unavailable", true));

        apiKeysService.fetchValidationResult("dummy_api_key");
        now.addAndGet(TimeUnit.MINUTES.toNanos(1));

        assertTrue(apiKeysService.fetchValidationResult("dummy_api_key").isAllowed());
        verify(apiKeysClient, times(2)).fetchApiKeyValidationResult("dummy_api_key");
        assertThrows(ApiKeysException.class, () -> apiKeysService.fetchValidationResult("other_key"));
    }

    @Test
    void backupExpiresAfterSixtyMinutes() {
        when(apiKeysClient.fetchApiKeyValidationResult("dummy_api_key"))
                .thenReturn(getPolicyResult())
                .thenThrow(new ApiKeysException("API Keys unavailable", true));

        apiKeysService.fetchValidationResult("dummy_api_key");
        now.addAndGet(TimeUnit.MINUTES.toNanos(60));

        assertThrows(ApiKeysException.class, () -> apiKeysService.fetchValidationResult("dummy_api_key"));
    }

    @Test
    void authorizationFailuresDoNotUseBackup() {
        when(apiKeysClient.fetchApiKeyValidationResult("dummy_api_key"))
                .thenReturn(getPolicyResult())
                .thenThrow(new IcmsAuthenticationException("Key revoked"))
                .thenThrow(new ApiKeysException("API Keys unavailable", true));

        apiKeysService.fetchValidationResult("dummy_api_key");
        now.addAndGet(TimeUnit.MINUTES.toNanos(1));

        assertThrows(IcmsAuthenticationException.class,
                () -> apiKeysService.fetchValidationResult("dummy_api_key"));
        assertThrows(ApiKeysException.class,
                () -> apiKeysService.fetchValidationResult("dummy_api_key"));
        verify(apiKeysClient, times(3)).fetchApiKeyValidationResult("dummy_api_key");
    }

    @Test
    void nonRetryableClientFailureDoesNotUseBackup() {
        when(apiKeysClient.fetchApiKeyValidationResult("dummy_api_key"))
                .thenReturn(getPolicyResult())
                .thenThrow(new ApiKeysException("Invalid evaluation request"))
                .thenThrow(new ApiKeysException("API Keys unavailable", true));

        apiKeysService.fetchValidationResult("dummy_api_key");
        now.addAndGet(TimeUnit.MINUTES.toNanos(1));

        assertThrows(ApiKeysException.class,
                () -> apiKeysService.fetchValidationResult("dummy_api_key"));
        assertThrows(ApiKeysException.class,
                () -> apiKeysService.fetchValidationResult("dummy_api_key"));
        verify(apiKeysClient, times(3)).fetchApiKeyValidationResult("dummy_api_key");
    }

    @Test
    void deniedPolicyEvictsPreviouslyValidBackupBeforeOutage() {
        var denied = getPolicyResult();
        denied.setAllowed(false);
        when(apiKeysClient.fetchApiKeyValidationResult("dummy_api_key"))
                .thenReturn(getPolicyResult())
                .thenReturn(denied)
                .thenThrow(new ApiKeysException("API Keys unavailable", true));

        apiKeysService.fetchValidationResult("dummy_api_key");
        now.addAndGet(TimeUnit.MINUTES.toNanos(1));

        assertThrows(IcmsAuthenticationException.class,
                () -> apiKeysService.fetchValidationResult("dummy_api_key"));
        assertThrows(ApiKeysException.class,
                () -> apiKeysService.fetchValidationResult("dummy_api_key"));
        verify(apiKeysClient, times(3)).fetchApiKeyValidationResult("dummy_api_key");
    }

    @Test
    void invalidResponsesAreNeverCached() {
        var invalid = getPolicyResult();
        invalid.setAllowed(false);
        when(apiKeysClient.fetchApiKeyValidationResult("dummy_api_key"))
                .thenReturn(invalid)
                .thenThrow(new ApiKeysException("API Keys unavailable", true));

        assertThrows(IcmsAuthenticationException.class,
                () -> apiKeysService.fetchValidationResult("dummy_api_key"));
        assertThrows(ApiKeysException.class, () -> apiKeysService.fetchValidationResult("dummy_api_key"));
    }
}
