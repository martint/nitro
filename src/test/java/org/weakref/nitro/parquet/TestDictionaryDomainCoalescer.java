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
package org.weakref.nitro.parquet;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;

import static org.assertj.core.api.Assertions.assertThat;

class TestDictionaryDomainCoalescer
{
    @Test
    void testCoalescesEquivalentSiblingMappings()
    {
        DictionaryVector keys = new DictionaryVector(new int[] {0, 1, 0, 1}, new I64Vector(new long[] {10, 20}));
        DictionaryVector values = new DictionaryVector(new int[] {0, 1, 0, 1}, new I64Vector(new long[] {100, 200}));
        DictionaryVector nulls = new DictionaryVector(new int[] {0, 1, 0, 1}, new BooleanVector(new boolean[] {false, true}));

        Streams[] result = DictionaryDomainCoalescer.coalesce(
                Streams.ofValues(keys),
                Streams.of(values, nulls, null));

        DictionaryVector resultKeys = (DictionaryVector) result[0].values();
        DictionaryVector resultValues = (DictionaryVector) result[1].values();
        DictionaryVector resultNulls = (DictionaryVector) result[1].get(Stream.NULLS);
        assertThat(resultKeys.hasSameRowMapping(resultValues)).isTrue();
        assertThat(resultKeys.hasSameRowMapping(resultNulls)).isTrue();
        assertThat(resultValues.values()).isSameAs(values.values());
        assertThat(resultNulls.values()).isSameAs(nulls.values());
    }

    @Test
    void testPreservesIndependentMappingsWhenIdsDiffer()
    {
        DictionaryVector keys = new DictionaryVector(new int[] {0, 1, 0, 1}, new I64Vector(new long[] {10, 20}));
        DictionaryVector values = new DictionaryVector(new int[] {1, 0, 1, 0}, new I64Vector(new long[] {100, 200}));
        Streams keyStreams = Streams.ofValues(keys);
        Streams valueStreams = Streams.ofValues(values);

        Streams[] result = DictionaryDomainCoalescer.coalesce(keyStreams, valueStreams);

        assertThat(result).containsExactly(keyStreams, valueStreams);
        assertThat(keys.hasSameRowMapping(values)).isFalse();
    }
}
