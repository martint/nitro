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

class TestArrayVector
{
    @Test
    void testMaterializesNestedSegmentsRecursively()
    {
        ArrayVector first = array(0, 2, 3);
        ArrayVector firstElements = array(0, 1, 3, 4);
        firstElements.setElements(Streams.ofValuesAndNulls(
                new I64Vector(new long[] {10, 20, 21, 30}),
                new BooleanVector(4)));
        first.setElements(Streams.ofValuesAndNulls(firstElements, new BooleanVector(3)));

        ArrayVector second = array(0, 2);
        ArrayVector secondElements = array(0, 0, 1);
        secondElements.setElements(Streams.ofValuesAndNulls(
                new I64Vector(new long[] {40}),
                new BooleanVector(1)));
        second.setElements(Streams.ofValuesAndNulls(
                secondElements,
                new BooleanVector(new boolean[] {false, true})));

        EngineResources resources = EngineResources.createDefault();
        Allocator allocator = new Allocator(resources);
        Allocator.Context context = new Allocator.Context("nested-array-materialize");
        try {
            Vector encodedFirst = DictionaryVector.wrap(new int[] {0, 1}, first);
            ArrayVector result = (ArrayVector) encodedFirst.materializeRows(
                    allocator,
                    context,
                    new Vector[] {encodedFirst, second});
            assertThat(result.offsets()).containsExactly(0, 2, 3, 5);
            ArrayVector elements = (ArrayVector) result.elementValues();
            assertThat(elements.offsets()).containsExactly(0, 1, 3, 4, 4, 5);
            assertThat(((I64Vector) elements.elementValues()).values()).containsExactly(10, 20, 21, 30, 40);
            assertThat(result.elementNulls().values()).containsExactly(false, false, false, false, true);
        }
        finally {
            allocator.release(context);
            resources.close();
        }
    }

    private static ArrayVector array(int... offsets)
    {
        ArrayVector array = new ArrayVector(offsets.length - 1);
        System.arraycopy(offsets, 0, array.offsets(), 0, offsets.length);
        return array;
    }
}
