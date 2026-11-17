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
package com.nvidia.nvcf.rest.function.invocation;

import static com.nvidia.nvcf.util.NvcfConstants.DEFAULT_VISIBILITY;
import static com.nvidia.nvcf.util.NvcfConstants.SPAN_TAG_NCA_ID;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

import com.nvidia.nvcf.rest.function.invocation.dto.ListInvocableFunctionsResponse;
import com.nvidia.nvcf.rest.function.management.dto.VisibilityEnum;
import com.nvidia.nvcf.service.account.AccountService;
import com.nvidia.nvcf.service.ratelimit.RateLimiterService;
import com.nvidia.nvcf.util.NvcfUtils;
import io.micrometer.tracing.Tracer;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequiredArgsConstructor
@Validated
@RequestMapping(value = "/v2/nvcf/accounts/{ncaId}", produces = APPLICATION_JSON_VALUE)
@Tag(name = "Cross-Account Invocable Functions for NVIDIA Super Admins",
        description = """
                Defines endpoints for NVIDIA Super Admins to list the function version ids of the
                 specified NVIDIA Cloud Account based on the bearer token specified using HTTP
                 Authorization header. The bearer token must have 'admin:list_functions' scope.
                """
)
public class XAccountFunctionInvocationController {

    private static final String NCA_ID_DESCRIPTION = "Id of the NVIDIA Cloud Account";

    private final AccountService accountService;
    private final FunctionInvocationFacade functionInvocationFacade;
    private final RateLimiterService rateLimiterService;
    private final Tracer tracer;

    @GetMapping("invocable/functions/versions/ids")
    @Operation(
            summary = "List Invocable Functions",
            description = """
                    Lists the function and version ids of the invocable functions that the
                     specified NVIDIA Cloud Account owns or that are shared with it: function
                     versions in ACTIVE or DEGRADING status. Requires a bearer token with
                     'admin:list_functions' scope in the HTTP Authorization header.
                    """
    )
    @PreAuthorize("hasAuthority('admin:list_functions')")
    public ListInvocableFunctionsResponse listInvocableFunctions(
            @Parameter(description = NCA_ID_DESCRIPTION, required = true)
            @PathVariable String ncaId,
            @Parameter(description = """
                    Query param 'visibility' indicates the kind of functions to be included
                     in the response.
                    """)
            @RequestParam(required = false,
                    defaultValue = DEFAULT_VISIBILITY) Set<VisibilityEnum> visibility,
            Authentication authentication) {
        NvcfUtils.addTagsToCurrentSpan(tracer, Map.of(SPAN_TAG_NCA_ID, ncaId));
        rateLimiterService.verifyLimits(ncaId);
        accountService.assertAccountExistsOrThrow(ncaId);
        accountService.assertAccountIdFromPathMatches(ncaId, authentication);
        return functionInvocationFacade.listInvocableFunctions(ncaId, authentication, visibility);
    }
}
