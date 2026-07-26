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

class TestBufferedJoinInputPolicy
{
    @Test
    void testStandaloneDefaultsAreOwnedByOperatorResources()
    {
        try (OperatorResources resources = OperatorResources.createDefault()) {
            BufferedJoinInputPolicy policy = resources.bufferedJoinInputPolicy();

            assertThat(policy).isEqualTo(BufferedJoinInputPolicy.defaults());
            assertThat(policy.maxCoalescedRows()).isEqualTo(4_000_000);
            assertThat(policy.maxPostLoadCoalescedRows()).isEqualTo(1 << 20);
            assertThat(policy.minAutomaticDirectExactRows()).isEqualTo(1 << 18);
            assertThat(policy.directExactCoalesce()).isFalse();
        }
    }
}
