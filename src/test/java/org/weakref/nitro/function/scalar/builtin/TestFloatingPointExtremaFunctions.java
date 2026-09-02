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
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;

import java.util.EnumSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.execution.EngineResources.createDefault;

final class TestFloatingPointExtremaFunctions
{
    @Test
    void testVariadicGreatestHonorsTrinoDoubleAndNullSemantics()
    {
        Streams result = evaluate(
                new GreatestF64(),
                List.of(
                        Streams.ofValues(new F64Vector(new double[] {-0.0, 7.0, 4.0, Double.NaN})),
                        Streams.ofValues(new F64Vector(new double[] {0.0, 3.0, 8.0, 10.0}))
                                .with(Stream.NULLS, new BooleanVector(new boolean[] {false, true, false, false})),
                        Streams.ofValues(new F64Vector(new double[] {-1.0, 6.0, 5.0, 9.0}))),
                Mask.all(4));

        double[] values = ((F64Vector) result.values()).values();
        assertThat(values[0]).isEqualTo(0.0);
        assertThat(values[1]).isEqualTo(7.0);
        assertThat(values[2]).isEqualTo(8.0);
        assertThat(values[3]).isNaN();
        assertThat(Double.doubleToRawLongBits(values[0])).isEqualTo(Double.doubleToRawLongBits(0.0));
        assertThat(((BooleanVector) result.get(Stream.NULLS)).values())
                .containsExactly(false, true, false, false);
    }

    private static Streams evaluate(
            org.weakref.nitro.function.scalar.PrimitiveFunction function,
            List<Streams> inputs,
            Mask mask)
    {
        try (Allocator allocator = new Allocator(createDefault())) {
            return function.apply(
                    inputs,
                    mask,
                    EnumSet.of(Stream.VALUES, Stream.NULLS),
                    Streams.empty(),
                    new PrimitiveExecutionContext(allocator));
        }
    }
}
