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

import static com.nvidia.nvcf.persistence.function.entity.FunctionStatus.ACTIVE;
import static com.nvidia.nvcf.persistence.function.entity.FunctionStatus.DEGRADED;
import static com.nvidia.nvcf.persistence.function.entity.FunctionStatus.DEGRADING;
import static com.nvidia.nvcf.persistence.function.entity.FunctionStatus.DEPLOYING;
import static com.nvidia.nvcf.persistence.function.entity.FunctionStatus.ERROR;
import static com.nvidia.nvcf.persistence.function.entity.FunctionStatus.INACTIVE;
import static com.nvidia.nvcf.service.azp.AuthorizedPartiesService.AUTHORIZED_WILDCARD_ACCOUNT;
import static com.nvidia.nvcf.util.TestConstants.TEST_FUNCTION_ID;
import static com.nvidia.nvcf.util.TestConstants.TEST_FUNCTION_ID_2;
import static com.nvidia.nvcf.util.TestConstants.TEST_FUNCTION_ID_3;
import static com.nvidia.nvcf.util.TestConstants.TEST_NCA_ID;
import static com.nvidia.nvcf.util.TestConstants.TEST_NCA_ID_2;
import static com.nvidia.nvcf.util.TestConstants.TEST_PUBLIC_FUNCTION_ID_1;
import static com.nvidia.nvcf.util.TestConstants.TEST_PUBLIC_FUNCTION_NCA_ID;
import static com.nvidia.nvcf.util.TestConstants.TEST_PUBLIC_FUNCTION_VERSION_ID_1;
import static com.nvidia.nvcf.util.TestConstants.TEST_PUBLIC_FUNCTION_VERSION_ID_2;
import static com.nvidia.nvcf.util.TestConstants.TEST_VERSION_ID_1;
import static com.nvidia.nvcf.util.TestConstants.TEST_VERSION_ID_2;
import static com.nvidia.nvcf.util.TestConstants.TEST_VERSION_ID_3;
import static com.nvidia.nvcf.util.TestConstants.TEST_VERSION_ID_4;
import static com.nvidia.nvcf.util.TestConstants.TEST_VERSION_ID_5;

import com.nvidia.nvcf.persistence.function.FunctionsRepository;
import com.nvidia.nvcf.persistence.function.entity.FunctionStatus;
import com.nvidia.nvcf.rest.account.TestAccountService;
import com.nvidia.nvcf.rest.azp.TestAuthorizedPartiesService;
import com.nvidia.nvcf.rest.azp.dto.AuthorizedPartyDto;
import com.nvidia.nvcf.rest.function.invocation.dto.InvocableFunctionDto;
import com.nvidia.nvcf.service.function.FunctionLookupService;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

/**
 * Creates the function versions for the tests of the invocable functions endpoints: versions in
 * every status that TEST_NCA_ID owns, that TEST_NCA_ID_2 shares with it at the version and the
 * function level, that TEST_NCA_ID_2 does not share, and that are public.
 */
@Service
@RequiredArgsConstructor
public class TestInvocableFunctionsService {

    // TEST_NCA_ID_2 shares this function with TEST_NCA_ID at the function level.
    public static final UUID SHARED_FUNCTION_ID =
            UUID.fromString("7c2f0b1e-0000-4000-8000-000000000001");
    public static final UUID SHARED_DEGRADING_VERSION_ID =
            UUID.fromString("7c2f0b1e-0000-4000-8000-000000000101");
    public static final UUID SHARED_INACTIVE_VERSION_ID =
            UUID.fromString("7c2f0b1e-0000-4000-8000-000000000102");
    // TEST_NCA_ID_2 shares this version of TEST_FUNCTION_ID_2 with TEST_NCA_ID.
    public static final UUID SHARED_DEGRADED_VERSION_ID =
            UUID.fromString("7c2f0b1e-0000-4000-8000-000000000103");
    // Versions of TEST_FUNCTION_ID_3, which TEST_NCA_ID owns.
    public static final UUID DEPLOYING_VERSION_ID =
            UUID.fromString("7c2f0b1e-0000-4000-8000-000000000104");
    public static final UUID ERROR_VERSION_ID =
            UUID.fromString("7c2f0b1e-0000-4000-8000-000000000105");
    public static final UUID DEGRADED_VERSION_ID =
            UUID.fromString("7c2f0b1e-0000-4000-8000-000000000106");

    // Created in the status under test.
    private static final UUID STATUS_FUNCTION_ID =
            UUID.fromString("7c2f0b1e-0000-4000-8000-000000000002");
    private static final UUID STATUS_VERSION_ID =
            UUID.fromString("7c2f0b1e-0000-4000-8000-000000000201");
    private static final UUID STATUS_SHARED_FUNCTION_ID =
            UUID.fromString("7c2f0b1e-0000-4000-8000-000000000003");
    private static final UUID STATUS_SHARED_VERSION_ID =
            UUID.fromString("7c2f0b1e-0000-4000-8000-000000000202");
    private static final UUID STATUS_PUBLIC_FUNCTION_ID =
            UUID.fromString("7c2f0b1e-0000-4000-8000-000000000004");
    private static final UUID STATUS_PUBLIC_VERSION_ID =
            UUID.fromString("7c2f0b1e-0000-4000-8000-000000000203");
    private static final UUID STATUS_FUNCTION_SHARED_FUNCTION_ID =
            UUID.fromString("7c2f0b1e-0000-4000-8000-000000000005");
    private static final UUID STATUS_FUNCTION_SHARED_VERSION_ID =
            UUID.fromString("7c2f0b1e-0000-4000-8000-000000000204");

    // The invocable versions, in ACTIVE or DEGRADING status, of each kind for TEST_NCA_ID.
    public static final List<InvocableFunctionDto> OWN = List.of(
            version(TEST_FUNCTION_ID, TEST_VERSION_ID_1),
            version(TEST_FUNCTION_ID, TEST_VERSION_ID_3));
    public static final List<InvocableFunctionDto> SHARED = List.of(
            version(TEST_FUNCTION_ID_2, TEST_VERSION_ID_2),
            version(SHARED_FUNCTION_ID, SHARED_DEGRADING_VERSION_ID));
    public static final List<InvocableFunctionDto> PUBLIC = List.of(
            version(TEST_PUBLIC_FUNCTION_ID_1, TEST_PUBLIC_FUNCTION_VERSION_ID_1));
    public static final List<InvocableFunctionDto> OWN_AND_SHARED = concat(OWN, SHARED);
    public static final List<InvocableFunctionDto> ALL = concat(OWN, SHARED, PUBLIC);

    // The invocable versions that TEST_NCA_ID_2 owns.
    public static final List<InvocableFunctionDto> OWN_BY_NCA_ID_2 = List.of(
            version(TEST_FUNCTION_ID_2, TEST_VERSION_ID_2),
            version(TEST_FUNCTION_ID_2, TEST_VERSION_ID_5),
            version(SHARED_FUNCTION_ID, SHARED_DEGRADING_VERSION_ID));

    // Every version that createFunctionVersions creates, whatever its status and owner.
    public static final List<InvocableFunctionDto> FIXTURE_VERSIONS = List.of(
            version(TEST_PUBLIC_FUNCTION_ID_1, TEST_PUBLIC_FUNCTION_VERSION_ID_1),
            version(TEST_PUBLIC_FUNCTION_ID_1, TEST_PUBLIC_FUNCTION_VERSION_ID_2),
            version(TEST_FUNCTION_ID, TEST_VERSION_ID_1),
            version(TEST_FUNCTION_ID, TEST_VERSION_ID_3),
            version(TEST_FUNCTION_ID_3, TEST_VERSION_ID_4),
            version(TEST_FUNCTION_ID_3, DEPLOYING_VERSION_ID),
            version(TEST_FUNCTION_ID_3, ERROR_VERSION_ID),
            version(TEST_FUNCTION_ID_3, DEGRADED_VERSION_ID),
            version(TEST_FUNCTION_ID_2, TEST_VERSION_ID_2),
            version(TEST_FUNCTION_ID_2, TEST_VERSION_ID_5),
            version(TEST_FUNCTION_ID_2, SHARED_DEGRADED_VERSION_ID),
            version(SHARED_FUNCTION_ID, SHARED_DEGRADING_VERSION_ID),
            version(SHARED_FUNCTION_ID, SHARED_INACTIVE_VERSION_ID));

    private final TestAccountService testAccountService;
    private final TestAuthorizedPartiesService testAuthPartiesService;
    private final FunctionLookupService functionLookupService;
    private final FunctionsRepository functionsRepository;

    public void createFunctionVersions() {
        testAccountService.createAccountAndAssociateClients(TEST_PUBLIC_FUNCTION_NCA_ID, null);
        create(TEST_PUBLIC_FUNCTION_ID_1, TEST_PUBLIC_FUNCTION_VERSION_ID_1,
               TEST_PUBLIC_FUNCTION_NCA_ID, ACTIVE);
        create(TEST_PUBLIC_FUNCTION_ID_1, TEST_PUBLIC_FUNCTION_VERSION_ID_2,
               TEST_PUBLIC_FUNCTION_NCA_ID, INACTIVE);
        share(TEST_PUBLIC_FUNCTION_NCA_ID, TEST_PUBLIC_FUNCTION_ID_1, Optional.empty(),
              AUTHORIZED_WILDCARD_ACCOUNT);

        create(TEST_FUNCTION_ID, TEST_VERSION_ID_1, TEST_NCA_ID, ACTIVE);
        create(TEST_FUNCTION_ID, TEST_VERSION_ID_3, TEST_NCA_ID, DEGRADING);
        create(TEST_FUNCTION_ID_3, TEST_VERSION_ID_4, TEST_NCA_ID, INACTIVE);
        create(TEST_FUNCTION_ID_3, DEPLOYING_VERSION_ID, TEST_NCA_ID, DEPLOYING);
        create(TEST_FUNCTION_ID_3, ERROR_VERSION_ID, TEST_NCA_ID, ERROR);
        create(TEST_FUNCTION_ID_3, DEGRADED_VERSION_ID, TEST_NCA_ID, DEGRADED);

        create(TEST_FUNCTION_ID_2, TEST_VERSION_ID_2, TEST_NCA_ID_2, ACTIVE);
        create(TEST_FUNCTION_ID_2, TEST_VERSION_ID_5, TEST_NCA_ID_2, ACTIVE);
        create(TEST_FUNCTION_ID_2, SHARED_DEGRADED_VERSION_ID, TEST_NCA_ID_2, DEGRADED);
        share(TEST_NCA_ID_2, TEST_FUNCTION_ID_2, Optional.of(TEST_VERSION_ID_2), TEST_NCA_ID);
        share(TEST_NCA_ID_2, TEST_FUNCTION_ID_2, Optional.of(SHARED_DEGRADED_VERSION_ID),
              TEST_NCA_ID);

        create(SHARED_FUNCTION_ID, SHARED_DEGRADING_VERSION_ID, TEST_NCA_ID_2, DEGRADING);
        create(SHARED_FUNCTION_ID, SHARED_INACTIVE_VERSION_ID, TEST_NCA_ID_2, INACTIVE);
        share(TEST_NCA_ID_2, SHARED_FUNCTION_ID, Optional.empty(), TEST_NCA_ID);
    }

    /**
     * Creates a version in the status that TEST_NCA_ID owns, one that TEST_NCA_ID_2 shares with
     * it at the version level, one that TEST_NCA_ID_2 shares with it at the function level, and
     * a public one.
     *
     * @return the function and version ids of the versions
     */
    public List<InvocableFunctionDto> createFunctionVersionsInStatus(FunctionStatus status) {
        create(STATUS_FUNCTION_ID, STATUS_VERSION_ID, TEST_NCA_ID, status);
        create(STATUS_SHARED_FUNCTION_ID, STATUS_SHARED_VERSION_ID, TEST_NCA_ID_2, status);
        share(TEST_NCA_ID_2, STATUS_SHARED_FUNCTION_ID, Optional.of(STATUS_SHARED_VERSION_ID),
              TEST_NCA_ID);
        create(STATUS_FUNCTION_SHARED_FUNCTION_ID, STATUS_FUNCTION_SHARED_VERSION_ID,
               TEST_NCA_ID_2, status);
        share(TEST_NCA_ID_2, STATUS_FUNCTION_SHARED_FUNCTION_ID, Optional.empty(), TEST_NCA_ID);
        create(STATUS_PUBLIC_FUNCTION_ID, STATUS_PUBLIC_VERSION_ID, TEST_PUBLIC_FUNCTION_NCA_ID,
               status);
        share(TEST_PUBLIC_FUNCTION_NCA_ID, STATUS_PUBLIC_FUNCTION_ID, Optional.empty(),
              AUTHORIZED_WILDCARD_ACCOUNT);
        return List.of(
                version(STATUS_FUNCTION_ID, STATUS_VERSION_ID),
                version(STATUS_SHARED_FUNCTION_ID, STATUS_SHARED_VERSION_ID),
                version(STATUS_FUNCTION_SHARED_FUNCTION_ID, STATUS_FUNCTION_SHARED_VERSION_ID),
                version(STATUS_PUBLIC_FUNCTION_ID, STATUS_PUBLIC_VERSION_ID));
    }

    public void share(String ownerNcaId, UUID functionId, Optional<UUID> versionId, String ncaId) {
        testAuthPartiesService.associateAuthParties(
                ownerNcaId, functionId, versionId,
                Set.of(AuthorizedPartyDto.builder().ncaId(ncaId).build()));
    }

    // Changes only the status of an existing version, keeping its authorized parties.
    public void setStatus(InvocableFunctionDto version, FunctionStatus status) {
        var function = functionLookupService
                .lookupUsingFunctionIdAndVersionIdOrThrow(version.id(), version.versionId());
        function.setFunctionStatus(status);
        functionsRepository.save(function);
    }

    // The function versions themselves are deleted with the rest of the test data.
    public void deleteAccounts() {
        testAccountService.deleteAccount(TEST_PUBLIC_FUNCTION_NCA_ID);
    }

    @SafeVarargs
    public static List<InvocableFunctionDto> concat(List<InvocableFunctionDto>... lists) {
        return Stream.of(lists).flatMap(List::stream).toList();
    }

    private static InvocableFunctionDto version(UUID id, UUID versionId) {
        return new InvocableFunctionDto(id, versionId);
    }

    private void create(UUID id, UUID versionId, String ncaId, FunctionStatus status) {
        testAuthPartiesService.createTestFunction(id, versionId, ncaId, "test-function-" + id,
                                                  status);
    }
}
