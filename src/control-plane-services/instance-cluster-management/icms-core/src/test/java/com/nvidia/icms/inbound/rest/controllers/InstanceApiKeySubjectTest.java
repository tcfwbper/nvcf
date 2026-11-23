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
package com.nvidia.icms.inbound.rest.controllers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.nvidia.icms.inbound.rest.converters.CreateInstanceApiModelConverter;
import com.nvidia.icms.inbound.rest.model.GetSpotInstanceRequests;
import com.nvidia.icms.outbound.apikeys.model.ApiKeyValidationResult;
import com.nvidia.icms.service.InstanceService;
import com.nvidia.icms.uec.UnifiedErrorReporter;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

class InstanceApiKeySubjectTest {

    private final InstanceService instanceService = mock(InstanceService.class);
    private final InstanceController controller = new InstanceController(instanceService,
            mock(CreateInstanceApiModelConverter.class), mock(UnifiedErrorReporter.class));
    private final HttpServletRequest request = mock(HttpServletRequest.class);

    @AfterEach
    void clearContext() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void actionRoutesUseApiKeyOwnerAsCustomer() {
        var key = ApiKeyValidationResult.builder().allowed(true).ownerId("nvcf-api")
                .ncaId("account-1").policy(ApiKeyValidationResult.Policy.builder()
                        .scopes(List.of("instance-request"))
                        .resources(List.of()).build()).build();
        var principal = key.getOAuth2Principal();
        SecurityContextHolder.getContext().setAuthentication(
                new UsernamePasswordAuthenticationToken(principal, "", principal.getAuthorities()));

        when(instanceService.describeInstanceRequests(eq("nvcf-api"), eq(Set.of("req-1")), any()))
                .thenReturn(new GetSpotInstanceRequests());
        controller.describeInstances(request, "DescribeInstanceRequests", Set.of("req-1"),
                null, null, null);
        verify(instanceService).describeInstanceRequests(eq("nvcf-api"), eq(Set.of("req-1")), any());

        controller.describeInstances(request, "DescribeInstances", null, null,
                List.of("instance-1"), null);
        verify(instanceService).describeInstances("nvcf-api", List.of("instance-1"));

        controller.terminateInstances(request, "TerminateInstances", Set.of("instance-1"), null);
        verify(instanceService).terminateInstances(eq("nvcf-api"), eq(Set.of("instance-1")), any());

        controller.cancelInstanceRequests(request, "CancelInstanceRequests", Set.of("req-1"), null);
        verify(instanceService).cancelInstanceRequests(eq("nvcf-api"), eq(Set.of("req-1")), any());
    }

}
