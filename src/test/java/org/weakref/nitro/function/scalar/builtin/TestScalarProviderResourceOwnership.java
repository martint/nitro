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
package org.weakref.nitro.function.scalar.builtin;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.function.scalar.PrimitiveFunction;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class TestScalarProviderResourceOwnership
{
    @Test
    void testProviderInstancesOwnDistinctAllocationContexts()
    {
        assertContextsAreProviderOwned(new AddI64(), new AddI64());
        assertContextsAreProviderOwned(new DivideI64(), new DivideI64());
        assertContextsAreProviderOwned(new RegexpReplaceUtf8(), new RegexpReplaceUtf8());
    }

    @Test
    void testSpecializedHelperContextIsOwnedAndPublishedByParentProvider()
    {
        RegexpReplaceUtf8 function = new RegexpReplaceUtf8();

        assertThat(function.allocationContexts())
                .hasSize(2)
                .extracting(Allocator.Context::name)
                .containsExactlyInAnyOrder("RegexpReplaceUtf8", "ExtractHostUtf8");
    }

    private static void assertContextsAreProviderOwned(PrimitiveFunction first, PrimitiveFunction second)
    {
        Set<Allocator.Context> firstContexts = first.allocationContexts();
        Set<Allocator.Context> secondContexts = second.allocationContexts();

        assertThat(firstContexts).isNotEmpty();
        assertThat(secondContexts).isNotEmpty();
        assertThat(firstContexts).doesNotContainAnyElementsOf(secondContexts);
    }
}
