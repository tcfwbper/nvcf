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
package com.nvidia.nvcf.rest.function.management.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.annotation.Nullable;
import jakarta.validation.Valid;
import java.time.Duration;

@Schema(types = {"object"}, description = "Function-level LLM invocation configuration.")
public record LlmInvocationConfigDto(
        @Nullable
        @Valid
        @Schema(description = "Function-level request priority configuration.")
        PriorityDto priority,

        @Nullable
        @Schema(description = "ISO 8601 duration allowed for an LLM function to terminate. "
                + "When absent, NVCF sends the configured minimum (PT1M by default) to ICMS at launch.",
                example = "PT3M")
        Duration terminationGracePeriodDuration) {

    public LlmInvocationConfigDto(PriorityDto priority) {
        this(priority, null);
    }
}
