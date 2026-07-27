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

class TestJoinMatchScratch
{
    @Test
    void ownsReusableScalarAndBatchViews()
    {
        JoinMatchScratch scratch = new JoinMatchScratch();

        assertThat((Object) scratch.scalarSingle()).isSameAs(scratch.scalarSingle());
        assertThat((Object) scratch.scalarChain()).isSameAs(scratch.scalarChain());

        scratch.prepareBatch(4);
        ChainLongList first = scratch.batchChain(0);
        scratch.prepareBatch(4);
        assertThat((Object) scratch.batchChain(0)).isSameAs(first);
        assertThat((Object) scratch.batchChain(3)).isNotNull();
        scratch.prepareBatch(8);
        assertThat((Object) scratch.batchChain(7)).isNotNull();
    }
}
