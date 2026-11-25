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
package com.nvidia.nvct.service.reval;

import com.nvidia.boot.exceptions.UpstreamException;
import com.nvidia.nvct.configuration.staticclientauth.FixedBearerExchangeFilterFunction;
import com.nvidia.nvct.configuration.staticclientauth.StaticClientAuthConfiguration.StaticClientRevalProperties;
import com.nvidia.nvct.persistence.task.entity.TaskEntity;
import com.nvidia.nvct.rest.task.dto.GpuSpecificationDto;
import com.nvidia.nvct.rest.task.dto.ValidationPolicyNameEnum;
import com.nvidia.nvct.service.registry.RegistryArtifactValidationService;
import com.nvidia.nvct.service.registry.RegistryCredentialService;
import com.nvidia.nvct.service.registry.dto.K8sSecretsDto;
import com.nvidia.nvct.util.NvctOAuth2ClientUtils;
import com.nvidia.nvct.util.NvctOAuth2ClientUtils.ManagedHttpResources;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.support.WebClientAdapter;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;
import tools.jackson.databind.json.JsonMapper;


@Slf4j
@Service
@RefreshScope
public class RevalClient {
    private static final String MESG_REVAL_ERROR =
            "Internal error during validation. Reval Status %s";
    private static final String MESG_REVAL_INTERNAL_ERRORS = "ReVal internal errors: {}";
    private static final String MESG_HELM_CHART_VALIDATION_ERRORS =
            "Helm chart validation error(s) from NVCF ReVal service: %s";
    private static final String MESG_UNKNOWN_VALIDATION_ERROR =
            "Helm chart validation failure. Unknown error.";
    public static final String CLIENT_REGISTRATION_ID = "reval";
    private static final String VALIDATION_POLICY_ID = "task";

    private final RevalStubService revalStubService;
    private final boolean enabled;
    private final RegistryArtifactValidationService registryArtifactValidationService;
    private final RegistryCredentialService registryCredentialService;
    private final JsonMapper jsonMapper;

    // We could have used OAuth2AuthorizedClientManager and relied on Spring Security to
    // pick up the configuration properties using ClientRegistrationRepository directly. However,
    // the client-secret value held in the ClientRegistrationRepository does not get refreshed when
    // client-secret is rotated. Addressing these issues requires introducing a refreshable
    // ClientRegistrationRepository that wasn't clean. Instead, we will keep it simple and use
    // the tried and tested approach of using @Value and @RefreshScope annotations and wire
    // things up ourselves.
    public RevalClient(
            @Value("${nvct.reval.base-url}") String baseUrl,
            @Value("${nvct.reval.enabled:true}") boolean enabled,
            @Value("${spring.security.oauth2.client.registration.reval.client-id}") String clientId,
            @Value("${spring.security.oauth2.client.registration.reval.client-secret}")
            String clientSecret,
            @Value("${spring.security.oauth2.client.registration.reval.scope}") String scope,
            @Value("${spring.security.oauth2.client.provider.reval.token-uri}") String tokenUri,
            Optional<StaticClientRevalProperties> staticClientRevalProperties,
            RegistryArtifactValidationService registryArtifactValidationService,
            RegistryCredentialService registryCredentialService,
            ManagedHttpResources revalHttpResources,
            WebClient.Builder webClientBuilder,   // Prototype-scoped - Safe to mutate.
            JsonMapper jsonMapper) {
        var authFilter = oauthFilter(staticClientRevalProperties, webClientBuilder,
                                     clientId, clientSecret, scope, tokenUri);
        var webClient = webClientBuilder
                .baseUrl(baseUrl)
                .clientConnector(revalHttpResources.connector())
                .filter(NvctOAuth2ClientUtils.getRetryableFilter(CLIENT_REGISTRATION_ID))
                .filter(authFilter)
                .build();
        var adapter = WebClientAdapter.create(webClient);
        var factory = HttpServiceProxyFactory.builderFor(adapter).build();
        this.revalStubService = factory.createClient(RevalStubService.class);

        this.enabled = enabled;
        this.registryArtifactValidationService = registryArtifactValidationService;
        this.registryCredentialService = registryCredentialService;
        this.jsonMapper = jsonMapper;
    }

    private static ExchangeFilterFunction oauthFilter(
            Optional<StaticClientRevalProperties> staticClientRevalProperties,
            WebClient.Builder webClientBuilder,
            String clientId,
            String clientSecret,
            String scope,
            String tokenUri) {
        return staticClientRevalProperties
                .map(p -> (ExchangeFilterFunction)
                        new FixedBearerExchangeFilterFunction(p::getToken))
                .orElseGet(() -> NvctOAuth2ClientUtils
                        .getOAuth2ExchangeFilter(webClientBuilder, CLIENT_REGISTRATION_ID,
                                                 tokenUri, clientId, clientSecret, scope));
    }

    public String validate(
            String ncaId,
            TaskEntity task,
            GpuSpecificationDto gpuSpec) {
        if (!enabled) {
            return StringUtils.EMPTY;
        }
        var imageRegistryAuthConfig = validateAndGetContainerRegistryImagePullSecrets(task);
        var helmRegistryAuthConfig = validateAndGetHelmRegistryImagePullSecrets(task);

        var request = RevalStubService.RevalValidateRequest
                .builder()
                .helmChart(task.getHelmChart())
                .instanceType(gpuSpec.instanceType())
                .gpu(gpuSpec.gpu())
                .configuration(gpuSpec.configuration())
                .imageRegistryAuthConfig(jsonMapper.valueToTree(imageRegistryAuthConfig))
                .helmRegistryAuthConfig(jsonMapper.valueToTree(helmRegistryAuthConfig))
                .validationPolicies(validationPolicies(gpuSpec))
                .build();
        var response = this.revalStubService.validate(request);
        if (response.getStatusCode().is2xxSuccessful() && response.hasBody()) {
            var body = response.getBody();
            assert body != null;  // Shuts up Sonar as it doesn't recognize hasBody() check above.

            if (!CollectionUtils.isEmpty(body.getInternalErrors())) {
                var internalErrors = String.join(", ", body.getInternalErrors());
                log.warn(MESG_REVAL_INTERNAL_ERRORS, internalErrors);
            }

            if (isValid(body)) {
                return StringUtils.EMPTY;
            }

            var validationErrors = validationErrors(body);
            String errors;
            if (validationErrors.isEmpty()) {
                errors = MESG_UNKNOWN_VALIDATION_ERROR;
            } else {
                errors = String.join(", ", validationErrors);
            }

            var msg = String.format(MESG_HELM_CHART_VALIDATION_ERRORS, errors);
            log.error(msg);
            return msg;
        }

        var msg = String.format(MESG_REVAL_ERROR, response.getStatusCode());
        log.error(msg);

        if (response.getStatusCode().is4xxClientError()) {
            // 4xx from Reval means that NVCT API is doing something wrong when invoking it.
            // That's why we throw IllegalStateException.
            throw new IllegalStateException(msg);
        }

        throw new UpstreamException(msg);
    }

    // Forward only the task's extra types; submit-time rules stay Default for every policy.
    private static List<RevalStubService.ValidationPolicy> validationPolicies(GpuSpecificationDto gpuSpec) {
        var policy = gpuSpec.helmValidationPolicy();
        if (policy == null || policy.extraKubernetesTypes() == null) {
            return null;
        }
        var extraTypes = policy.extraKubernetesTypes().stream()
                .map(t -> RevalStubService.KubernetesType.builder()
                        .group(t.group())
                        .version(t.version())
                        .kind(t.kind())
                        .build())
                .toList();
        return List.of(RevalStubService.ValidationPolicy.builder()
                .id(VALIDATION_POLICY_ID)
                .name(ValidationPolicyNameEnum.DEFAULT.toString())
                .allowedExtraKubernetesTypes(extraTypes)
                .build());
    }

    // A failed policy result rejects the chart even if the top-level flag says valid.
    private static boolean isValid(RevalStubService.RevalValidateResponse body) {
        return body.isValid() && (body.getValidationPolicies() == null
                || body.getValidationPolicies().stream()
                        .allMatch(RevalStubService.ValidationPolicyResult::isValid));
    }

    // With policies, ReVal reports errors per policy instead of at the top level.
    private static List<String> validationErrors(RevalStubService.RevalValidateResponse body) {
        var errors = new ArrayList<String>();
        if (body.getValidationErrors() != null) {
            errors.addAll(body.getValidationErrors());
        }
        if (body.getValidationPolicies() != null) {
            for (var policy : body.getValidationPolicies()) {
                if (policy.isValid()) {
                    continue;
                }
                if (CollectionUtils.isEmpty(policy.getValidationErrors())) {
                    errors.add(MESG_UNKNOWN_VALIDATION_ERROR);
                } else {
                    errors.addAll(policy.getValidationErrors());
                }
            }
        }
        return errors;
    }

    private K8sSecretsDto validateAndGetContainerRegistryImagePullSecrets(TaskEntity task) {
        registryArtifactValidationService.validateContainerRegistryCredentialsExist(task);
        return registryCredentialService.getContainerRegistryImagePullSecrets(task);
    }

    private K8sSecretsDto validateAndGetHelmRegistryImagePullSecrets(TaskEntity task) {
        registryArtifactValidationService.validateHelmRegistryCredentialsExist(task);
        return registryCredentialService.getHelmRegistryImagePullSecrets(task);
    }

}
