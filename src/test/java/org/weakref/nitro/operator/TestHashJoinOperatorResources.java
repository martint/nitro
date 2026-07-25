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
        HashJoinOperatorResources first = new HashJoinOperatorResources(true);
        HashJoinOperatorResources second = new HashJoinOperatorResources(true);
        Object firstLocal = new Object();
        Object secondLocal = new Object();

        assertThat(first.bufferPoolCompatibilityGroup(firstLocal))
                .isSameAs(first.bufferPoolCompatibilityGroup(secondLocal));
        assertThat(second.bufferPoolCompatibilityGroup(new Object()))
                .isNotSameAs(first.bufferPoolCompatibilityGroup(firstLocal));
    }

    @Test
    void testDisabledSharingRetainsLocalCompatibilityDomain()
    {
        HashJoinOperatorResources resources = new HashJoinOperatorResources(false);
        Object firstLocal = new Object();
        Object secondLocal = new Object();

        assertThat(resources.bufferPoolCompatibilityGroup(firstLocal)).isSameAs(firstLocal);
        assertThat(resources.bufferPoolCompatibilityGroup(secondLocal)).isSameAs(secondLocal);
    }
}
