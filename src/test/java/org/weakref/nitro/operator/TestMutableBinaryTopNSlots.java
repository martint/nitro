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
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

final class TestMutableBinaryTopNSlots
{
    @Test
    void testArbitraryReplacementNullsAndCompactMaterialization()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("test", getClass());
        BinaryVector values = binary("long-value", "middle", "z");
        BinaryVector.Trait ascii = BinaryVector.Trait.flag("ascii");
        values.addTrait(ascii);
        Output input = Output.of(Streams.ofValuesAndNulls(
                values,
                new BooleanVector(new boolean[] {false, true, false})));
        VectorAccess.BinaryRegions regions = VectorAccess.binaryRegions(input.borrow(Stream.VALUES));

        try (MutableBinaryTopNSlots slots = new MutableBinaryTopNSlots(allocator, context, 2)) {
            slots.copy(regions, values.traits(), true, false, 0, 0);
            slots.copy(regions, values.traits(), true, false, 2, 1);
            assertThat(slots.compare(0, 1)).isNegative();

            // Replacing a longer value with a null exercises non-monotonic slot updates.
            slots.copy(regions, values.traits(), true, true, 1, 0);
            assertThat(slots.isNull(0)).isTrue();

            Streams result = slots.materialize(new int[] {1, 0}, 2);
            VectorAccess.BinaryRegions output = VectorAccess.binaryRegions(result.values());
            assertThat(string(output, 0)).isEqualTo("z");
            assertThat(string(output, 1)).isEmpty();
            assertThat(((BinaryVector) result.values()).traits()).containsExactly(ascii);
            assertThat(VectorAccess.booleanValues(result.get(Stream.NULLS)).value(1)).isTrue();
        }
        allocator.release(context);
    }

    private static BinaryVector binary(String... strings)
    {
        int bytes = 0;
        for (String string : strings) {
            bytes += string.getBytes(StandardCharsets.UTF_8).length;
        }
        BinaryVector vector = new BinaryVector(strings.length, bytes);
        for (int position = 0; position < strings.length; position++) {
            vector.setBytes(position, strings[position].getBytes(StandardCharsets.UTF_8));
        }
        return vector;
    }

    private static String string(VectorAccess.BinaryRegions values, int position)
    {
        return new String(values.data(position), values.offset(position), values.length(position), StandardCharsets.UTF_8);
    }
}
