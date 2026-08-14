/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.weakref.nitro.operator;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TestHashJoinOperatorResources
{
    @Test
    void testSharedCompatibilityDomainIsOwnerScoped()
    {
        HashJoinIndexPolicy indexPolicy = HashJoinIndexPolicy.defaults();
        HashJoinDynamicFilterPolicy dynamicFilterPolicy = HashJoinDynamicFilterPolicy.defaults();
        HashJoinBuildPolicy buildPolicy = HashJoinBuildPolicy.defaults();
        HashJoinOutputPolicy outputPolicy = HashJoinOutputPolicy.defaults();
        HashJoinFilterPolicy filterPolicy = HashJoinFilterPolicy.defaults();
        HashJoinExecutionPolicy executionPolicy = HashJoinExecutionPolicy.defaults();
        HashJoinOperatorResources first = defaultResources(true);
        HashJoinOperatorResources second = new HashJoinOperatorResources(
                true,
                null,
                indexPolicy,
                dynamicFilterPolicy,
                buildPolicy,
                outputPolicy,
                filterPolicy,
                executionPolicy);
        Object firstLocal = new Object();
        Object secondLocal = new Object();

        assertThat(first.indexPolicy()).isEqualTo(HashJoinIndexPolicy.defaults());
        assertThat(second.indexPolicy()).isSameAs(indexPolicy);
        assertThat(first.dynamicFilterPolicy()).isEqualTo(HashJoinDynamicFilterPolicy.defaults());
        assertThat(second.dynamicFilterPolicy()).isSameAs(dynamicFilterPolicy);
        assertThat(first.buildPolicy()).isEqualTo(HashJoinBuildPolicy.defaults());
        assertThat(second.buildPolicy()).isSameAs(buildPolicy);
        assertThat(first.outputPolicy()).isEqualTo(HashJoinOutputPolicy.defaults());
        assertThat(second.outputPolicy()).isSameAs(outputPolicy);
        assertThat(first.filterPolicy()).isEqualTo(HashJoinFilterPolicy.defaults());
        assertThat(second.filterPolicy()).isSameAs(filterPolicy);
        assertThat(first.executionPolicy()).isEqualTo(HashJoinExecutionPolicy.defaults());
        assertThat(second.executionPolicy()).isSameAs(executionPolicy);
        assertThat(executionPolicy.maxBatchRows()).isEqualTo(10_000);
        assertThat(executionPolicy.duplicateListInitialCapacity()).isEqualTo(2);
        assertThat(buildPolicy.maxBuildBatchRows()).isEqualTo(1 << 16);
        assertThat(buildPolicy.maxInitialPairHashBytes()).isEqualTo(512L << 20);
        assertThat(buildPolicy.initialHashAdmissionSampleRows()).isEqualTo(4096);
        assertThat(buildPolicy.initialHashAdmissionMinSampleRows()).isEqualTo(16);
        assertThat(buildPolicy.pairHashCapMinExpectedRows()).isEqualTo(1_000_000);
        assertThat(buildPolicy.pairHashCapMaxDistinctPercent()).isEqualTo(50);
        assertThat(buildPolicy.pairHashCapMaxDomainPercent()).isEqualTo(25);
        assertThat(buildPolicy.payloadHashCapAlwaysExpectedRows()).isEqualTo(40_000_000);
        assertThat(buildPolicy.payloadHashCapMaxDistinctPercent()).isEqualTo(25);
        assertThat(buildPolicy.payloadHashCapBoundedExpectedRows()).isEqualTo(5_000_000);
        assertThat(outputPolicy.buildDictionarySparseRatio()).isEqualTo(8);
        assertThat(outputPolicy.buildDictionaryMinDistinctValues()).isEqualTo(16);
        assertThat(outputPolicy.buildDictionaryMaxDistinctPercent()).isEqualTo(50);
        assertThat(outputPolicy.composeEncodedOuterDictionaryDepth()).isEqualTo(Integer.MAX_VALUE);
        assertThat(outputPolicy.adaptiveComposeMaxRows()).isEqualTo(1024);
        assertThat(outputPolicy.adaptiveComposeDepth()).isEqualTo(4);
        assertThat(outputPolicy.directDenseSingleMatchRangeOutput()).isFalse();
        assertThat(indexPolicy.flatDictionaryProbeCacheMaxCardinality()).isEqualTo(1 << 16);
        assertThat(indexPolicy.flatDictionaryProbeCacheMinRowsPerEntry()).isEqualTo(2);
        assertThat(indexPolicy.denseCompactPairMinCapacity()).isEqualTo(1 << 25);
        assertThat(indexPolicy.denseCompactSparsePairMinCapacity()).isEqualTo(1 << 23);
        assertThat(indexPolicy.compactCompletedDirectRangeMinSize()).isEqualTo(256);
        assertThat(indexPolicy.denseUnusedBuildMembershipMinKeys()).isEqualTo(1 << 12);
        assertThat(indexPolicy.maxArrayRange()).isEqualTo(1 << 26);
        assertThat(indexPolicy.rangeAdmissionSampleRows()).isEqualTo(4096);
        assertThat(indexPolicy.rangeAdmissionMinSampleRows()).isEqualTo(16);
        assertThat(indexPolicy.directRangeMaxCardinalityRatio()).isEqualTo(2);
        assertThat(indexPolicy.directRangeBuildInitialCapacity()).isEqualTo(1024);
        assertThat(indexPolicy.directRangeBuild()).isTrue();
        assertThat(indexPolicy.directRangeBuildMinRows()).isEqualTo(256);
        assertThat(indexPolicy.compressKeyOnlyDuplicates()).isTrue();
        assertThat(indexPolicy.directDuplicateGroupInitialCapacity()).isEqualTo(1024);
        assertThat(indexPolicy.flatLegacyInitialCapacity()).isEqualTo(1024);
        assertThat(first.bufferPoolCompatibilityGroup(firstLocal))
                .isSameAs(first.bufferPoolCompatibilityGroup(secondLocal));
        assertThat(second.bufferPoolCompatibilityGroup(new Object()))
                .isNotSameAs(first.bufferPoolCompatibilityGroup(firstLocal));
    }

    @Test
    void testDisabledSharingRetainsLocalCompatibilityDomain()
    {
        HashJoinOperatorResources resources = defaultResources(false);
        Object firstLocal = new Object();
        Object secondLocal = new Object();

        assertThat(resources.bufferPoolCompatibilityGroup(firstLocal)).isSameAs(firstLocal);
        assertThat(resources.bufferPoolCompatibilityGroup(secondLocal)).isSameAs(secondLocal);
    }

    private static HashJoinOperatorResources defaultResources(boolean shareBufferPoolAcrossOperators)
    {
        return new HashJoinOperatorResources(
                shareBufferPoolAcrossOperators,
                null,
                HashJoinIndexPolicy.defaults(),
                HashJoinDynamicFilterPolicy.defaults(),
                HashJoinBuildPolicy.defaults(),
                HashJoinOutputPolicy.defaults(),
                HashJoinFilterPolicy.defaults(),
                HashJoinExecutionPolicy.defaults());
    }
}
