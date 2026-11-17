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
import static com.nvidia.nvcf.persistence.function.entity.FunctionStatus.DEGRADED;
import static com.nvidia.nvcf.persistence.function.entity.FunctionStatus.DEGRADING;
import static com.nvidia.nvcf.persistence.function.entity.FunctionStatus.DEPLOYING;
import static com.nvidia.nvcf.persistence.function.entity.FunctionStatus.INACTIVE;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.ALL;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.FIXTURE_VERSIONS;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.OWN;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.OWN_AND_SHARED;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.OWN_BY_NCA_ID_2;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.PUBLIC;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.SHARED;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.SHARED_DEGRADED_VERSION_ID;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.SHARED_DEGRADING_VERSION_ID;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.SHARED_FUNCTION_ID;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.SHARED_INACTIVE_VERSION_ID;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.concat;
import static com.nvidia.nvcf.service.azp.AuthorizedPartiesService.AUTHORIZED_WILDCARD_ACCOUNT;
import static com.nvidia.nvcf.util.MockApiKeysServer.resetToDefault;
import static com.nvidia.nvcf.util.MockApiKeysServer.setApiKeyValidationResponse;
import static com.nvidia.nvcf.util.MockApiKeysServer.setResponse;
import static com.nvidia.nvcf.util.NvcfConstants.SCOPE_LIST_FUNCTIONS_DETAILS;
import static com.nvidia.nvcf.util.TestConstants.SCOPE_DEPLOY_FUNCTION;
import static com.nvidia.nvcf.util.TestConstants.SCOPE_INVOKE_FUNCTION;
import static com.nvidia.nvcf.util.TestConstants.SCOPE_LIST_FUNCTIONS;
import static com.nvidia.nvcf.util.TestConstants.SCOPE_QUEUE_DETAILS;
import static com.nvidia.nvcf.util.TestConstants.TEST_AUTHORIZED_CLIENT_ID_1;
import static com.nvidia.nvcf.util.TestConstants.TEST_CLIENT_SUBJECT;
import static com.nvidia.nvcf.util.TestConstants.TEST_CLIENT_SUBJECT_2;
import static com.nvidia.nvcf.util.TestConstants.TEST_FUNCTION_ID;
import static com.nvidia.nvcf.util.TestConstants.TEST_FUNCTION_ID_2;
import static com.nvidia.nvcf.util.TestConstants.TEST_FUNCTION_ID_3;
import static com.nvidia.nvcf.util.TestConstants.TEST_NCA_ID;
import static com.nvidia.nvcf.util.TestConstants.TEST_NCA_ID_2;
import static com.nvidia.nvcf.util.TestConstants.TEST_OWNER_ID;
import static com.nvidia.nvcf.util.TestConstants.TEST_VERSION_ID_1;
import static com.nvidia.nvcf.util.TestConstants.TEST_VERSION_ID_2;
import static com.nvidia.nvcf.util.TestConstants.TEST_VERSION_ID_5;
import static org.assertj.core.api.Assertions.assertThat;

import com.nvidia.boot.mock.ngc.MockCasServer;
import com.nvidia.boot.mock.ngc.MockNgcContainerRegistryServer;
import com.nvidia.nvcf.IntegrationTestConfiguration;
import com.nvidia.nvcf.NvcfTestApp;
import com.nvidia.nvcf.persistence.function.entity.FunctionStatus;
import com.nvidia.nvcf.rest.account.TestAccountService;
import com.nvidia.nvcf.rest.function.invocation.dto.InvocableFunctionDto;
import com.nvidia.nvcf.rest.function.invocation.dto.InvocationTokenRequest;
import com.nvidia.nvcf.rest.function.invocation.dto.ListInvocableFunctionsResponse;
import com.nvidia.nvcf.rest.function.management.dto.FunctionStatusEnum;
import com.nvidia.nvcf.rest.function.management.dto.ListFunctionsResponse;
import com.nvidia.nvcf.service.apikeys.ApiKeyValidationResult.Resource;
import com.nvidia.nvcf.service.common.TestCommonService;
import com.nvidia.nvcf.util.MockApiKeysServer;
import com.nvidia.nvcf.util.MockEssServer;
import com.nvidia.nvcf.util.MockNotaryServer;
import jakarta.annotation.Nullable;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
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
class FunctionInvocationControllerTest {

    private static final String INVOCABLE_FUNCTIONS_URI =
            "/v2/nvcf/invocable/functions/versions/ids";
    private static final String FUNCTIONS_URI = "/v2/nvcf/functions";
    private static final String API_KEY = "nvapi-stg-some-key";
    private static final List<Resource> ALL_RESOURCES = List.of(
            new Resource("account-functions", "*"),
            new Resource("authorized-functions", "*"));
    private static final UUID UNKNOWN_FUNCTION_ID =
            UUID.fromString("7c2f0b1e-0000-4000-8000-0000000000ff");
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

    @Value("${nvcf.notary.base-url}")
    private String notaryBaseUrl;

    @Value("${nvcf.notary.audiences.nvcf}")
    private String nvcfAudience;

    @BeforeAll
    void beforeAll() {
        log.info("{}: Started running tests", this.getClass().getSimpleName());
        MockEssServer.start(essBaseUrl);
        MockApiKeysServer.start(apiKeysBaseUrl);
        MockCasServer.start(authnBaseUrl, casBaseUrl);
        MockNgcContainerRegistryServer.start(ngcContainerRegistryUrl);
        MockNotaryServer.start(notaryBaseUrl, nvcfAudience, nvcfAudience);

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
        MockNotaryServer.stop();

        log.info("{}: Completed running tests", this.getClass().getSimpleName());
    }

    @AfterEach
    void reset() {
        testCommonService.reset();
        resetToDefault();
        testInvocableFunctionsService.deleteAccounts();
    }

    // Only the list_functions scope is accepted: the response has no function details, and
    // listing is not invocation.
    Stream<Arguments> scopeArgs() {
        return Stream.of(
                Arguments.of(List.of(SCOPE_LIST_FUNCTIONS), HttpStatus.OK),
                // The default scopes of a generated API key.
                Arguments.of(List.of(SCOPE_INVOKE_FUNCTION, SCOPE_LIST_FUNCTIONS,
                                     SCOPE_QUEUE_DETAILS, SCOPE_LIST_FUNCTIONS_DETAILS),
                             HttpStatus.OK),
                Arguments.of(List.of(SCOPE_LIST_FUNCTIONS_DETAILS), HttpStatus.FORBIDDEN),
                Arguments.of(List.of(SCOPE_INVOKE_FUNCTION), HttpStatus.FORBIDDEN),
                Arguments.of(List.of(SCOPE_LIST_FUNCTIONS_DETAILS, SCOPE_INVOKE_FUNCTION),
                             HttpStatus.FORBIDDEN),
                Arguments.of(List.of(SCOPE_DEPLOY_FUNCTION), HttpStatus.FORBIDDEN),
                Arguments.of(List.of(), HttpStatus.FORBIDDEN));
    }

    @ParameterizedTest
    @MethodSource("scopeArgs")
    void shouldAcceptOnlyListFunctionsScopeForApiKeys(
            List<String> scopes,
            HttpStatus expectedStatus) {
        setResponse(TEST_NCA_ID, TEST_OWNER_ID, ALL_RESOURCES, scopes);

        var response = listInvocableFunctions(API_KEY, "visibility=private,authorized");

        assertThat(response.getStatusCode()).isEqualTo(expectedStatus);
        if (expectedStatus.isError()) {
            return;
        }
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().functions())
                .containsExactlyInAnyOrderElementsOf(OWN_AND_SHARED);
    }

    @ParameterizedTest
    @MethodSource("scopeArgs")
    void shouldAcceptOnlyListFunctionsScopeForTokens(
            List<String> scopes,
            HttpStatus expectedStatus) {
        var response = listInvocableFunctions(jwt(scopes), "visibility=private,authorized");

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
                                     TEST_CLIENT_SUBJECT, List.of(SCOPE_LIST_FUNCTIONS), -3600),
                             HttpStatus.UNAUTHORIZED),
                Arguments.of("API key that the API keys service denies",
                             (Supplier<String>) () -> {
                                 setApiKeyValidationResponse(TEST_NCA_ID, TEST_OWNER_ID,
                                                             ALL_RESOURCES,
                                                             List.of(SCOPE_LIST_FUNCTIONS),
                                                             false);
                                 return API_KEY;
                             },
                             HttpStatus.FORBIDDEN),
                Arguments.of("JWT of a client that belongs to no account",
                             (Supplier<String>) () -> MOCK_OAUTH2_TOKEN_SERVER.getJwt(
                                     "test-client-without-account",
                                     List.of(SCOPE_LIST_FUNCTIONS), 100),
                             HttpStatus.NOT_FOUND));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("rejectedBearerArgs")
    void shouldRejectABearerTokenThatCannotList(
            String description,
            Supplier<String> bearer,
            HttpStatus expectedStatus) {
        var response = listInvocableFunctions(bearer.get(), "");

        assertThat(response.getStatusCode()).isEqualTo(expectedStatus);
    }

    // The response has only the function and version id of each version, with no details.
    @Test
    void shouldReturnOnlyFunctionAndVersionIds() {
        var response = exchangeJson(jwt(List.of(SCOPE_LIST_FUNCTIONS)), "");

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
        var token = MOCK_OAUTH2_TOKEN_SERVER.getJwt(TEST_AUTHORIZED_CLIENT_ID_1,
                                                    List.of(SCOPE_LIST_FUNCTIONS), 100);

        var response = exchangeJson(token, "visibility=private,authorized");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull().containsOnlyKeys("functions");
        assertThat(response.getBody().get("functions")).isEmpty();
    }

    // The result is for the account of the bearer token.
    @Test
    void shouldListTheVersionsOfTheBearerTokenAccount() {
        setResponse(TEST_NCA_ID_2, TEST_OWNER_ID, ALL_RESOURCES, List.of(SCOPE_LIST_FUNCTIONS));
        var token = MOCK_OAUTH2_TOKEN_SERVER.getJwt(TEST_CLIENT_SUBJECT_2,
                                                    List.of(SCOPE_LIST_FUNCTIONS), 100);

        for (var bearer : List.of(token, API_KEY)) {
            var response = listInvocableFunctions(bearer, "");

            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(response.getBody()).isNotNull();
            assertThat(response.getBody().functions())
                    .as("versions listed with %s", API_KEY.equals(bearer) ? "an API key" : "a JWT")
                    .containsExactlyInAnyOrderElementsOf(concat(OWN_BY_NCA_ID_2, PUBLIC));
        }
    }

    // An API key of an organization without an NVCF account, such as a personal organization,
    // can invoke public functions only.
    @Test
    void shouldListOnlyPublicVersionsForAnOrganizationWithoutAnAccount() {
        setResponse("test-org-without-account", TEST_OWNER_ID, ALL_RESOURCES,
                    List.of(SCOPE_LIST_FUNCTIONS));

        var response = listInvocableFunctions(API_KEY, "");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().functions()).containsExactlyInAnyOrderElementsOf(PUBLIC);
    }

    // A version is listed only in a status that accepts invocations, whether the account owns
    // it, it is shared with the account at the version or the function level, or it is public,
    // and whatever the bearer token.
    @ParameterizedTest
    @EnumSource(FunctionStatus.class)
    void shouldListAVersionOnlyInAnInvocableStatus(FunctionStatus status) {
        var versions = testInvocableFunctionsService.createFunctionVersionsInStatus(status);
        var expected = status == ACTIVE || status == DEGRADING ? concat(ALL, versions) : ALL;
        setResponse(TEST_NCA_ID, TEST_OWNER_ID, ALL_RESOURCES, List.of(SCOPE_LIST_FUNCTIONS));

        for (var bearer : List.of(jwt(List.of(SCOPE_LIST_FUNCTIONS)), API_KEY)) {
            var response = listInvocableFunctions(bearer, "");

            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(response.getBody()).isNotNull();
            assertThat(response.getBody().functions())
                    .as("%s versions listed with %s", status,
                        API_KEY.equals(bearer) ? "an API key" : "a JWT")
                    .containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    // A status change shows in the next listing, for own and shared versions alike.
    @Test
    void shouldFollowStatusChanges() {
        var own = new InvocableFunctionDto(TEST_FUNCTION_ID, TEST_VERSION_ID_1);
        var shared = new InvocableFunctionDto(TEST_FUNCTION_ID_2, TEST_VERSION_ID_2);
        var token = jwt(List.of(SCOPE_LIST_FUNCTIONS));

        for (var status : List.of(INACTIVE, DEGRADING, DEGRADED, DEPLOYING, ACTIVE)) {
            testInvocableFunctionsService.setStatus(own, status);
            testInvocableFunctionsService.setStatus(shared, status);

            var response = listInvocableFunctions(token, "visibility=private,authorized");

            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(response.getBody()).isNotNull();
            var invocable = status == ACTIVE || status == DEGRADING;
            assertThat(response.getBody().functions().contains(own))
                    .as("own version listed in %s", status).isEqualTo(invocable);
            assertThat(response.getBody().functions().contains(shared))
                    .as("shared version listed in %s", status).isEqualTo(invocable);
        }
    }

    // The listing has exactly the versions that the invocation access check accepts for the
    // same bearer token. The token endpoint runs that check, as function invocation does,
    // before it asks the Notary Service for an invocation token.
    Stream<Arguments> invocationArgs() {
        return Stream.of(
                Arguments.of("JWT", null),
                Arguments.of("API key with account-functions and authorized-functions",
                             ALL_RESOURCES),
                Arguments.of("API key with account-functions",
                             List.of(new Resource("account-functions", "*"))),
                Arguments.of("API key with authorized-functions",
                             List.of(new Resource("authorized-functions", "*"))),
                Arguments.of("API key with function resources",
                             List.of(function(TEST_FUNCTION_ID, TEST_VERSION_ID_1),
                                     function(TEST_FUNCTION_ID_2, null),
                                     function(TEST_FUNCTION_ID_3, null),
                                     function(SHARED_FUNCTION_ID, SHARED_INACTIVE_VERSION_ID))),
                Arguments.of("API key without resources", List.of()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invocationArgs")
    void shouldListExactlyTheVersionsThatInvocationAccepts(
            String bearerKind,
            @Nullable List<Resource> resources) {
        var scopes = List.of(SCOPE_LIST_FUNCTIONS, SCOPE_INVOKE_FUNCTION);
        var bearer = resources == null ? jwt(scopes) : API_KEY;
        if (resources != null) {
            setResponse(TEST_NCA_ID, TEST_OWNER_ID, resources, scopes);
        }

        var response = listInvocableFunctions(bearer, "");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        var listed = response.getBody().functions();
        assertThat(listed).doesNotHaveDuplicates();
        for (var version : FIXTURE_VERSIONS) {
            var invocation = issueInvocationToken(bearer, version);
            assertThat(listed.contains(version))
                    .as("%s: %s listed, and its invocation token request got %s",
                        bearerKind, version, invocation)
                    .isEqualTo(invocation.is2xxSuccessful());
        }
    }

    // An API key's resources limit the versions that the account owns or that are shared with
    // it, as they do for invocation, and a version that they allow is still listed only in an
    // invocable status.
    Stream<Arguments> resourceArgs() {
        return Stream.of(
                Arguments.of(List.of(new Resource("account-functions", "*")), OWN),
                Arguments.of(List.of(new Resource("authorized-functions", "*")), SHARED),
                Arguments.of(List.of(function(TEST_FUNCTION_ID, TEST_VERSION_ID_1),
                                     function(TEST_FUNCTION_ID_2, null)),
                             List.of(new InvocableFunctionDto(TEST_FUNCTION_ID, TEST_VERSION_ID_1),
                                     new InvocableFunctionDto(TEST_FUNCTION_ID_2,
                                                              TEST_VERSION_ID_2))),
                Arguments.of(List.of(new Resource("account-functions", "*"),
                                     function(TEST_FUNCTION_ID_2, TEST_VERSION_ID_2)),
                             concat(OWN, List.of(new InvocableFunctionDto(TEST_FUNCTION_ID_2,
                                                                          TEST_VERSION_ID_2)))),
                // A function shared at the function level.
                Arguments.of(List.of(function(SHARED_FUNCTION_ID, null)),
                             List.of(new InvocableFunctionDto(SHARED_FUNCTION_ID,
                                                              SHARED_DEGRADING_VERSION_ID))),
                // An ACTIVE version that is not shared with the account.
                Arguments.of(List.of(function(TEST_FUNCTION_ID_2, TEST_VERSION_ID_5)), List.of()),
                // Own and shared versions in statuses that do not accept invocations.
                Arguments.of(List.of(function(TEST_FUNCTION_ID_3, null),
                                     function(TEST_FUNCTION_ID_2, SHARED_DEGRADED_VERSION_ID),
                                     function(SHARED_FUNCTION_ID, SHARED_INACTIVE_VERSION_ID)),
                             List.of()),
                Arguments.of(List.of(function(UNKNOWN_FUNCTION_ID, null)), List.of()),
                Arguments.of(List.of(), List.of()));
    }

    @ParameterizedTest
    @MethodSource("resourceArgs")
    void shouldListOnlyTheVersionsThatTheApiKeyResourcesAllow(
            List<Resource> resources,
            List<InvocableFunctionDto> expected) {
        setResponse(TEST_NCA_ID, TEST_OWNER_ID, resources, List.of(SCOPE_LIST_FUNCTIONS));
        var query = "visibility=private,authorized";

        var response = listInvocableFunctions(API_KEY, query);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().functions())
                .containsExactlyInAnyOrderElementsOf(expected)
                .containsExactlyInAnyOrderElementsOf(invocableVersionsOfListing(API_KEY, query));
    }

    // Public versions are not limited by an API key's resources, as for invocation.
    @Test
    void shouldListPublicVersionsWhateverTheApiKeyResources() {
        setResponse(TEST_NCA_ID, TEST_OWNER_ID,
                    List.of(function(TEST_FUNCTION_ID, TEST_VERSION_ID_1)),
                    List.of(SCOPE_LIST_FUNCTIONS));
        var allowed = List.of(new InvocableFunctionDto(TEST_FUNCTION_ID, TEST_VERSION_ID_1));

        var response = listInvocableFunctions(API_KEY, "");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().functions())
                .containsExactlyInAnyOrderElementsOf(concat(allowed, PUBLIC))
                .containsExactlyInAnyOrderElementsOf(invocableVersionsOfListing(API_KEY, ""));
    }

    // A version that the account owns and makes public is listed once.
    @Test
    void shouldListAVersionOnce() {
        testInvocableFunctionsService.share(TEST_NCA_ID, TEST_FUNCTION_ID, Optional.empty(),
                                            AUTHORIZED_WILDCARD_ACCOUNT);

        var response = listInvocableFunctions(jwt(List.of(SCOPE_LIST_FUNCTIONS)), "");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().functions()).containsExactlyInAnyOrderElementsOf(ALL);
    }

    Stream<Arguments> visibilityArgs() {
        return Stream.of(
                Arguments.of("visibility=private", OWN),
                Arguments.of("visibility=authorized", SHARED),
                Arguments.of("visibility=public", PUBLIC),
                Arguments.of("visibility=PUBLIC", PUBLIC),
                Arguments.of("visibility=private,private", OWN),
                Arguments.of("visibility=private,authorized", OWN_AND_SHARED),
                Arguments.of("visibility=private&visibility=authorized", OWN_AND_SHARED),
                Arguments.of("visibility=private,AUTHORIZED", OWN_AND_SHARED),
                Arguments.of("visibility=public,private,authorized", ALL),
                Arguments.of("visibility=", ALL),
                Arguments.of("", ALL));
    }

    // Each visibility includes only its kind of function, and the result has exactly the
    // invocable versions of the function listing for the same query.
    @ParameterizedTest
    @MethodSource("visibilityArgs")
    void shouldListTheInvocableVersionsOfTheFunctionListing(
            String query,
            List<InvocableFunctionDto> expected) {
        var token = jwt(List.of(SCOPE_LIST_FUNCTIONS));

        var response = listInvocableFunctions(token, query);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().functions())
                .containsExactlyInAnyOrderElementsOf(expected)
                .containsExactlyInAnyOrderElementsOf(invocableVersionsOfListing(token, query));
    }

    @ParameterizedTest
    @ValueSource(strings = {"visibility=invalid", "visibility=private,invalid"})
    void shouldRejectAnInvalidVisibility(String query) {
        var response = listInvocableFunctions(jwt(List.of(SCOPE_LIST_FUNCTIONS)), query);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    private static String jwt(List<String> scopes) {
        return MOCK_OAUTH2_TOKEN_SERVER.getJwt(TEST_CLIENT_SUBJECT, scopes, 100);
    }

    private static Resource function(UUID id, @Nullable UUID versionId) {
        return new Resource("function", id + "/" + (versionId == null ? "*" : versionId));
    }

    private ResponseEntity<ListInvocableFunctionsResponse> listInvocableFunctions(
            @Nullable String bearer,
            String query) {
        return testRestTemplate.exchange(request(withQuery(INVOCABLE_FUNCTIONS_URI, query), bearer),
                                         ListInvocableFunctionsResponse.class);
    }

    private ResponseEntity<Map<String, List<Map<String, String>>>> exchangeJson(
            String bearer,
            String query) {
        return testRestTemplate.exchange(request(withQuery(INVOCABLE_FUNCTIONS_URI, query), bearer),
                                         JSON_BODY);
    }

    // The function and version ids of the versions in an invocable status in the function
    // listing.
    private List<InvocableFunctionDto> invocableVersionsOfListing(String bearer, String query) {
        var response = testRestTemplate.exchange(request(withQuery(FUNCTIONS_URI, query), bearer),
                                                 ListFunctionsResponse.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        return response.getBody().functions().stream()
                .filter(function -> function.status() == FunctionStatusEnum.ACTIVE
                        || function.status() == FunctionStatusEnum.DEGRADING)
                .map(function -> new InvocableFunctionDto(function.id(), function.versionId()))
                .toList();
    }

    private HttpStatusCode issueInvocationToken(String bearer, InvocableFunctionDto version) {
        var request = RequestEntity
                .post(URI.create("/v2/nvcf/tokens/functions/" + version.id()
                                         + "/versions/" + version.versionId()))
                .header("Authorization", "Bearer " + bearer)
                .contentType(MediaType.APPLICATION_JSON)
                .body(InvocationTokenRequest.builder().clientId("test_client_id").build());
        return testRestTemplate.exchange(request, String.class).getStatusCode();
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
