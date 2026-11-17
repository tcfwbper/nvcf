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

import static com.nvidia.nvcf.IntegrationTestConfiguration.MOCK_OAUTH2_TOKEN_SERVER;
import static com.nvidia.nvcf.persistence.function.entity.FunctionStatus.ACTIVE;
import static com.nvidia.nvcf.persistence.function.entity.FunctionStatus.DEGRADING;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.ALL;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.OWN;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.OWN_AND_SHARED;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.OWN_BY_NCA_ID_2;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.PUBLIC;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.SHARED;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.concat;
import static com.nvidia.nvcf.util.MockApiKeysServer.resetToDefault;
import static com.nvidia.nvcf.util.MockApiKeysServer.setResponse;
import static com.nvidia.nvcf.util.NvcfConstants.ADMIN_SCOPE_LIST_FUNCTIONS;
import static com.nvidia.nvcf.util.NvcfConstants.ADMIN_SCOPE_LIST_FUNCTIONS_DETAILS;
import static com.nvidia.nvcf.util.TestConstants.SCOPE_INVOKE_FUNCTION;
import static com.nvidia.nvcf.util.TestConstants.SCOPE_LIST_FUNCTIONS;
import static com.nvidia.nvcf.util.TestConstants.TEST_ADMIN_SUBJECT;
import static com.nvidia.nvcf.util.TestConstants.TEST_AUTHORIZED_NCA_ID_1;
import static com.nvidia.nvcf.util.TestConstants.TEST_NCA_ID;
import static com.nvidia.nvcf.util.TestConstants.TEST_NCA_ID_2;
import static com.nvidia.nvcf.util.TestConstants.TEST_OWNER_ID;
import static org.assertj.core.api.Assertions.assertThat;

import com.nvidia.boot.mock.ngc.MockCasServer;
import com.nvidia.boot.mock.ngc.MockNgcContainerRegistryServer;
import com.nvidia.nvcf.IntegrationTestConfiguration;
import com.nvidia.nvcf.NvcfTestApp;
import com.nvidia.nvcf.persistence.function.entity.FunctionStatus;
import com.nvidia.nvcf.rest.account.TestAccountService;
import com.nvidia.nvcf.rest.function.invocation.dto.InvocableFunctionDto;
import com.nvidia.nvcf.rest.function.invocation.dto.ListInvocableFunctionsResponse;
import com.nvidia.nvcf.rest.function.management.dto.FunctionStatusEnum;
import com.nvidia.nvcf.rest.function.management.dto.ListFunctionsResponse;
import com.nvidia.nvcf.service.apikeys.ApiKeyValidationResult.Resource;
import com.nvidia.nvcf.service.common.TestCommonService;
import com.nvidia.nvcf.util.MockApiKeysServer;
import com.nvidia.nvcf.util.MockEssServer;
import jakarta.annotation.Nullable;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ContextConfiguration;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Slf4j
@AutoConfigureTestRestTemplate
@SpringBootTest(
        classes = {NvcfTestApp.class, IntegrationTestConfiguration.class},
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.profiles.active=test")
@ContextConfiguration(initializers = IntegrationTestConfiguration.Initializer.class)
class XAccountFunctionInvocationControllerTest {

    private static final String INVOCABLE_FUNCTIONS_URI =
            "/v2/nvcf/accounts/%s/invocable/functions/versions/ids";
    private static final String FUNCTIONS_URI = "/v2/nvcf/accounts/%s/functions";
    private static final ParameterizedTypeReference<Map<String, List<Map<String, String>>>>
            JSON_BODY = new ParameterizedTypeReference<>() {
            };

    @Autowired
    private TestRestTemplate testRestTemplate;

    @Autowired
    private TestCommonService testCommonService;

    @Autowired
    private TestAccountService testAccountService;

    @Autowired
    private TestInvocableFunctionsService testInvocableFunctionsService;

    @Value("${nvcf.api-keys.base-url}")
    private String apiKeysBaseUrl;

    @Value("${nvcf.ess.base-url}")
    private String essBaseUrl;

    @Value("${nvcf.registries.recognized.helm.ngc.hostname}")
    private String casBaseUrl;

    @Value("${nvcf.registries.recognized.helm.ngc.oauth2.base-url}")
    private String authnBaseUrl;

    @Value("${nvcf.registries.recognized.container.ngc.hostname}")
    private String ngcContainerRegistryUrl;

    @BeforeAll
    void beforeAll() {
        log.info("{}: Started running tests", this.getClass().getSimpleName());
        MockEssServer.start(essBaseUrl);
        MockApiKeysServer.start(apiKeysBaseUrl);
        MockCasServer.start(authnBaseUrl, casBaseUrl);
        MockNgcContainerRegistryServer.start(ngcContainerRegistryUrl);

        testAccountService.createDefaultAccountsClientsAndRegistries();
    }

    @BeforeEach
    void beforeEach() {
        testInvocableFunctionsService.createFunctionVersions();
    }

    @AfterAll
    void cleanup() {
        testAccountService.cleanupAccountsClientsAndRegistries();

        MockApiKeysServer.stop();
        MockEssServer.stop();
        MockCasServer.stop();
        MockNgcContainerRegistryServer.stop();

        log.info("{}: Completed running tests", this.getClass().getSimpleName());
    }

    @AfterEach
    void reset() {
        testCommonService.reset();
        resetToDefault();
        testInvocableFunctionsService.deleteAccounts();
    }

    Stream<Arguments> scopeArgs() {
        return Stream.of(
                Arguments.of(List.of(ADMIN_SCOPE_LIST_FUNCTIONS), HttpStatus.OK),
                Arguments.of(List.of(ADMIN_SCOPE_LIST_FUNCTIONS,
                                     ADMIN_SCOPE_LIST_FUNCTIONS_DETAILS),
                             HttpStatus.OK),
                Arguments.of(List.of(ADMIN_SCOPE_LIST_FUNCTIONS_DETAILS), HttpStatus.FORBIDDEN),
                Arguments.of(List.of(SCOPE_LIST_FUNCTIONS), HttpStatus.FORBIDDEN),
                Arguments.of(List.of(SCOPE_INVOKE_FUNCTION), HttpStatus.FORBIDDEN),
                Arguments.of(List.of(), HttpStatus.FORBIDDEN));
    }

    @ParameterizedTest
    @MethodSource("scopeArgs")
    void shouldAcceptOnlyAdminListFunctionsScope(List<String> scopes, HttpStatus expectedStatus) {
        var response = listInvocableFunctions(jwt(scopes), TEST_NCA_ID,
                                              "visibility=private,authorized");

        assertThat(response.getStatusCode()).isEqualTo(expectedStatus);
        if (expectedStatus.isError()) {
            return;
        }
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().functions())
                .containsExactlyInAnyOrderElementsOf(OWN_AND_SHARED);
    }

    Stream<Arguments> rejectedBearerArgs() {
        return Stream.of(
                Arguments.of("no bearer token", (Supplier<String>) () -> null,
                             HttpStatus.UNAUTHORIZED),
                Arguments.of("malformed token", (Supplier<String>) () -> "invalid",
                             HttpStatus.UNAUTHORIZED),
                Arguments.of("expired JWT",
                             (Supplier<String>) () -> MOCK_OAUTH2_TOKEN_SERVER.getJwt(
                                     TEST_ADMIN_SUBJECT, List.of(ADMIN_SCOPE_LIST_FUNCTIONS),
                                     -3600),
                             HttpStatus.UNAUTHORIZED),
                // API keys never carry the admin scope, even for the account in the path.
                Arguments.of("API key of the account in the path",
                             (Supplier<String>) () -> {
                                 setResponse(TEST_NCA_ID, TEST_OWNER_ID,
                                             List.of(new Resource("account-functions", "*"),
                                                     new Resource("authorized-functions", "*")),
                                             List.of(SCOPE_LIST_FUNCTIONS));
                                 return "nvapi-stg-some-key";
                             },
                             HttpStatus.FORBIDDEN));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("rejectedBearerArgs")
    void shouldRejectABearerTokenThatCannotList(
            String description,
            Supplier<String> bearer,
            HttpStatus expectedStatus) {
        var response = listInvocableFunctions(bearer.get(), TEST_NCA_ID, "");

        assertThat(response.getStatusCode()).isEqualTo(expectedStatus);
    }

    // The response has only the function and version id of each version, with no details.
    @Test
    void shouldReturnOnlyFunctionAndVersionIds() {
        var response = exchangeJson(TEST_NCA_ID, "");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getHeaders().getContentType()).isNotNull();
        assertThat(response.getHeaders().getContentType()
                           .isCompatibleWith(MediaType.APPLICATION_JSON)).isTrue();
        assertThat(response.getBody()).isNotNull().containsOnlyKeys("functions");
        var functions = response.getBody().get("functions");
        assertThat(functions).allSatisfy(function -> assertThat(function)
                .containsOnlyKeys("id", "versionId"));
        assertThat(functions.stream()
                           .map(function -> new InvocableFunctionDto(
                                   UUID.fromString(function.get("id")),
                                   UUID.fromString(function.get("versionId"))))
                           .toList())
                .containsExactlyInAnyOrderElementsOf(ALL);
    }

    // An account without invocable function versions gets an empty list, not an error.
    @Test
    void shouldReturnAnEmptyListWithoutInvocableVersions() {
        var response = exchangeJson(TEST_AUTHORIZED_NCA_ID_1, "visibility=private,authorized");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull().containsOnlyKeys("functions");
        assertThat(response.getBody().get("functions")).isEmpty();
    }

    // A version is listed only in a status that accepts invocations, whether the account owns
    // it, it is shared with the account at the version or the function level, or it is public.
    @ParameterizedTest
    @EnumSource(FunctionStatus.class)
    void shouldListAVersionOnlyInAnInvocableStatus(FunctionStatus status) {
        var versions = testInvocableFunctionsService.createFunctionVersionsInStatus(status);
        var expected = status == ACTIVE || status == DEGRADING ? concat(ALL, versions) : ALL;

        var response = listInvocableFunctions(adminJwt(), TEST_NCA_ID, "");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().functions())
                .as("%s versions listed", status)
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    Stream<Arguments> listingArgs() {
        return Stream.of(
                Arguments.of(TEST_NCA_ID, "visibility=private", OWN),
                Arguments.of(TEST_NCA_ID, "visibility=authorized", SHARED),
                Arguments.of(TEST_NCA_ID, "visibility=public", PUBLIC),
                Arguments.of(TEST_NCA_ID, "visibility=private,authorized", OWN_AND_SHARED),
                Arguments.of(TEST_NCA_ID, "visibility=private&visibility=authorized",
                             OWN_AND_SHARED),
                Arguments.of(TEST_NCA_ID, "", ALL),
                Arguments.of(TEST_NCA_ID_2, "visibility=private", OWN_BY_NCA_ID_2),
                Arguments.of(TEST_NCA_ID_2, "visibility=authorized", List.of()),
                Arguments.of(TEST_NCA_ID_2, "", concat(OWN_BY_NCA_ID_2, PUBLIC)),
                // An account that owns no functions and has none shared with it.
                Arguments.of(TEST_AUTHORIZED_NCA_ID_1, "visibility=private,authorized", List.of()),
                Arguments.of(TEST_AUTHORIZED_NCA_ID_1, "", PUBLIC));
    }

    // The result is for the account in the path, not the caller's, and has exactly the
    // invocable versions of the cross-account function listing for the same query.
    @ParameterizedTest
    @MethodSource("listingArgs")
    void shouldListTheInvocableVersionsOfTheCrossAccountFunctionListing(
            String ncaId,
            String query,
            List<InvocableFunctionDto> expected) {
        var token = adminJwt();

        var response = listInvocableFunctions(token, ncaId, query);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().functions())
                .containsExactlyInAnyOrderElementsOf(expected)
                .containsExactlyInAnyOrderElementsOf(invocableVersionsOfListing(token, ncaId,
                                                                                query));
    }

    @Test
    void shouldRejectAnInvalidVisibility() {
        var response = listInvocableFunctions(adminJwt(), TEST_NCA_ID, "visibility=invalid");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void shouldRejectAnUnknownAccount() {
        var response = listInvocableFunctions(adminJwt(), "test-unknown-nca-id", "");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    private static String jwt(List<String> scopes) {
        return MOCK_OAUTH2_TOKEN_SERVER.getJwt(TEST_ADMIN_SUBJECT, scopes, 100);
    }

    private static String adminJwt() {
        return jwt(List.of(ADMIN_SCOPE_LIST_FUNCTIONS));
    }

    private ResponseEntity<ListInvocableFunctionsResponse> listInvocableFunctions(
            @Nullable String bearer,
            String ncaId,
            String query) {
        return testRestTemplate.exchange(
                request(withQuery(INVOCABLE_FUNCTIONS_URI.formatted(ncaId), query), bearer),
                ListInvocableFunctionsResponse.class);
    }

    private ResponseEntity<Map<String, List<Map<String, String>>>> exchangeJson(
            String ncaId,
            String query) {
        return testRestTemplate.exchange(
                request(withQuery(INVOCABLE_FUNCTIONS_URI.formatted(ncaId), query), adminJwt()),
                JSON_BODY);
    }

    // The function and version ids of the versions in an invocable status in the cross-account
    // function listing.
    private List<InvocableFunctionDto> invocableVersionsOfListing(
            String bearer,
            String ncaId,
            String query) {
        var response = testRestTemplate.exchange(
                request(withQuery(FUNCTIONS_URI.formatted(ncaId), query), bearer),
                ListFunctionsResponse.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        return response.getBody().functions().stream()
                .filter(function -> function.status() == FunctionStatusEnum.ACTIVE
                        || function.status() == FunctionStatusEnum.DEGRADING)
                .map(function -> new InvocableFunctionDto(function.id(), function.versionId()))
                .toList();
    }

    private static RequestEntity<Void> request(String uri, @Nullable String bearer) {
        var builder = RequestEntity.get(URI.create(uri));
        if (bearer != null) {
            builder = builder.header("Authorization", "Bearer " + bearer);
        }
        return builder.build();
    }

    private static String withQuery(String uri, String query) {
        return query.isEmpty() ? uri : uri + "?" + query;
    }
}
