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

class TestJoinBufferPolicy
{
    @Test
    void testStandaloneDefaultsAreOwnedByOperatorResources()
    {
        try (OperatorResources resources = OperatorResources.createDefault()) {
            JoinBufferPolicy policy = resources.joinBufferPolicy();

            assertThat(policy).isEqualTo(JoinBufferPolicy.defaults());
            assertThat(policy.positionMappingCache()).isTrue();
            assertThat(policy.positionMappingCacheSize()).isEqualTo(8);
            assertThat(policy.fuseDictionaryPositionCopies()).isTrue();
            assertThat(policy.copyAndCompactRangeSelection()).isTrue();
            assertThat(policy.preserveDictionaryBinaryPositionCopies()).isTrue();
            assertThat(policy.compactAllFalsePositionCopies()).isTrue();
            assertThat(policy.indexedVectorTreeTraversal()).isTrue();
        }
    }
}
