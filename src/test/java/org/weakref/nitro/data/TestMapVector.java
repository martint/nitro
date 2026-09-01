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
import org.weakref.nitro.execution.EngineResources;

import static org.assertj.core.api.Assertions.assertThat;

class TestMapVector
{
    @Test
    void testMaterializesMixedEncodedSegmentsInOnePass()
    {
        MapVector first = map(new int[] {0, 2, 3}, new long[] {1, 2, 3}, new long[] {10, 20, 30});
        MapVector second = map(new int[] {0, 1}, new long[] {4}, new long[] {40});

        EngineResources resources = EngineResources.createDefault();
        Allocator allocator = new Allocator(resources);
        Allocator.Context context = new Allocator.Context("mixed-map-materialize");
        try {
            Vector encodedFirst = DictionaryVector.wrap(new int[] {1, 0}, first);
            MapVector result = (MapVector) encodedFirst.materializeRows(
                    allocator,
                    context,
                    new Vector[] {encodedFirst, second});
            assertThat(result.offsets()).containsExactly(0, 1, 3, 4);
            assertThat(((I64Vector) result.keyValues()).values()).containsExactly(3, 1, 2, 4);
            assertThat(((I64Vector) result.valueValues()).values()).containsExactly(30, 10, 20, 40);
        }
        finally {
            allocator.release(context);
            resources.close();
        }
    }

    private static MapVector map(int[] offsets, long[] keys, long[] values)
    {
        MapVector map = new MapVector(offsets.length - 1);
        System.arraycopy(offsets, 0, map.offsets(), 0, offsets.length);
        map.setEntries(
                Streams.ofValuesAndNulls(new I64Vector(keys), new BooleanVector(keys.length)),
                Streams.ofValuesAndNulls(new I64Vector(values), new BooleanVector(values.length)));
        return map;
    }
}
