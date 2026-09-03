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
package org.weakref.nitro.data;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.core.function.aggregation.ContributionCarrier.BINARY_REGION;
import static org.weakref.nitro.core.function.aggregation.ContributionCarrier.LONG;

class TestGeneratedAggregationDomainBindings
{
    @Test
    void testBindsMixedCarriersWithOneSharedMapping()
    {
        int[] ids = {0, 1, 0, 1};
        DictionaryVector reference = DictionaryVector.wrap(ids, new I64Vector(new long[] {11, 22}));
        DictionaryVector longs = reference.sharedMappingWithValues(new I32Vector(new int[] {7, 9}));
        BinaryVector binary = new BinaryVector(2, new int[] {0, 2, 5}, new byte[] {1, 2, 3, 4, 5});
        DictionaryVector bytes = reference.sharedMappingWithValues(binary);
        DictionaryVector nulls = reference.sharedMappingWithValues(new BooleanVector(new boolean[] {false, true}));
        GeneratedAggregationDomainBindings bindings = new GeneratedAggregationDomainBindings(2);

        assertThat(bindings.bindInput(0, reference, longs, null, true, LONG)).isTrue();
        assertThat(bindings.bindInput(1, reference, bytes, nulls, true, BINARY_REGION)).isTrue();

        assertThat(bindings.inputs()[0]).isSameAs(((I32Vector) longs.values()).values());
        assertThat(bindings.inputs()[1]).isSameAs(binary.data());
        assertThat(bindings.inputValueOffsets()[1]).isSameAs(binary.offsets());
        assertThat(bindings.inputNulls()[1]).isSameAs(((BooleanVector) nulls.values()).values());
        assertThat(bindings.intInputs()).containsExactly(true, false);
    }

    @Test
    void testRejectsEqualSizedButUnrelatedMapping()
    {
        DictionaryVector reference = DictionaryVector.wrap(new int[] {0, 1}, new I64Vector(new long[] {11, 22}));
        DictionaryVector unrelated = DictionaryVector.wrap(new int[] {1, 0}, new I64Vector(new long[] {7, 9}));
        GeneratedAggregationDomainBindings bindings = new GeneratedAggregationDomainBindings(1);

        assertThat(bindings.bindInput(0, reference, unrelated, null, true, LONG)).isFalse();
        assertThat(bindings.bindInput(0, reference, unrelated.values(), null, true, LONG)).isFalse();
    }
}
