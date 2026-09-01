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
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;
import org.weakref.nitro.function.scalar.PrimitiveFunction;

import java.util.EnumSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.execution.EngineResources.createDefault;

final class TestIntegralExtremaFunctions
{
    @Test
    void testVariadicGreatestHonorsMaskAndNulls()
    {
        Streams result = evaluate(
                new GreatestI64(),
                List.of(
                        Streams.ofValues(new I64Vector(new long[] {1, 7, 4, 9})),
                        Streams.ofValues(new I64Vector(new long[] {2, 3, 8, 10}))
                                .with(Stream.NULLS, new BooleanVector(new boolean[] {false, true, false, false})),
                        Streams.ofValues(new I64Vector(new long[] {-1, 6, 5, 11}))),
                Mask.sparse(new int[] {0, 1, 2}, 4));

        assertThat(((I64Vector) result.values()).values()).containsExactly(2, 7, 8, 0);
        assertThat(((BooleanVector) result.get(Stream.NULLS)).values())
                .containsExactly(false, true, false, false);
    }

    @Test
    void testVariadicLeastHonorsMaskAndNulls()
    {
        Streams result = evaluate(
                new LeastI64(),
                List.of(
                        Streams.ofValues(new I64Vector(new long[] {1, 7, 4, 9})),
                        Streams.ofValues(new I64Vector(new long[] {2, 3, 8, 10}))
                                .with(Stream.NULLS, new BooleanVector(new boolean[] {false, true, false, false})),
                        Streams.ofValues(new I64Vector(new long[] {-1, 6, 5, 11}))),
                Mask.sparse(new int[] {0, 1, 2}, 4));

        assertThat(((I64Vector) result.values()).values()).containsExactly(-1, 3, 4, 0);
        assertThat(((BooleanVector) result.get(Stream.NULLS)).values())
                .containsExactly(false, true, false, false);
    }

    private static Streams evaluate(PrimitiveFunction function, List<Streams> inputs, Mask mask)
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
