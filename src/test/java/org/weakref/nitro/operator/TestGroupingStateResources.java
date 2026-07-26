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

class TestGroupingStateResources
{
    @Test
    void testPoolFamilyIsOwnerScoped()
    {
        LongGroupingPolicy policy = LongGroupingPolicy.defaults(true);
        CompositeGroupingPolicy compositePolicy = CompositeGroupingPolicy.defaults();
        GroupingStateResources first = new GroupingStateResources(true, policy, compositePolicy);
        GroupingStateResources second = new GroupingStateResources(true, policy, compositePolicy);

        assertThat(first.longGroupingPolicy()).isSameAs(policy);
        assertThat(first.compositeGroupingPolicy()).isSameAs(compositePolicy);
        assertThat(first.poolZeroedLongDirectIds()).isTrue();
        assertThat(first.zeroedLongDirectIdsFamily()).isSameAs(first.zeroedLongDirectIdsFamily());
        assertThat(second.zeroedLongDirectIdsFamily()).isNotSameAs(first.zeroedLongDirectIdsFamily());
    }

    @Test
    void testPoolingPolicyIsConstructed()
    {
        assertThat(new GroupingStateResources(
                        false,
                        LongGroupingPolicy.defaults(false),
                        CompositeGroupingPolicy.defaults())
                .poolZeroedLongDirectIds())
                .isFalse();
    }
}
