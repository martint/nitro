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
package org.weakref.nitro;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.Mask;

import static org.assertj.core.api.Assertions.assertThat;

class TestMask
{
    @Test
    void directDictionaryIdComparisonPreservesMatchComplementAndNullSemantics()
    {
        int[] ids = {2, 1, 2, 0, 2, 1};
        boolean[] nulls = {false, false, true, false, false, true};

        Mask matches = Mask.all(ids.length);
        matches.retainDictionaryIdComparison(ids, 2, nulls, true);
        assertThat(matches).containsExactly(0, 4);

        Mask complement = Mask.all(ids.length);
        complement.retainDictionaryIdComparison(ids, 2, nulls, false);
        assertThat(complement).containsExactly(1, 3);

        Mask sparse = Mask.sparse(new int[] {1, 2, 3, 4}, ids.length);
        sparse.retainDictionaryIdComparison(ids, 2, null, false);
        assertThat(sparse).containsExactly(1, 3);
    }
}
