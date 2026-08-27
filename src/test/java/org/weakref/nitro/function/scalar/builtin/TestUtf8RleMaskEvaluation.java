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
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TestUtf8RleMaskEvaluation
{
    @Test
    void testUniformComparisonPreservesOrClearsExistingSelection()
    {
        LessThanUtf8 function = new LessThanUtf8();
        Streams left = Streams.ofValues(rle("label-0", 5));
        Mask preserved = Mask.sparse(new int[] {1, 3}, 5);
        Mask cleared = Mask.sparse(new int[] {1, 3}, 5);

        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            PrimitiveExecutionContext context = new PrimitiveExecutionContext(allocator);
            assertThat(function.tryEvaluateTrueMaskInPlace(
                    List.of(left, Streams.ofValues(rle("label-1", 5))), preserved, context)).isTrue();
            assertThat(function.tryEvaluateTrueMaskInPlace(
                    List.of(left, Streams.ofValues(rle("label-0", 5))), cleared, context)).isTrue();
        }

        assertThat(preserved.selectedCount()).isEqualTo(2);
        assertThat(preserved.position(0)).isEqualTo(1);
        assertThat(preserved.position(1)).isEqualTo(3);
        assertThat(cleared.selectedCount()).isZero();
    }

    @Test
    void testUniformComparisonFallsBackForPositionNulls()
    {
        LessThanUtf8 function = new LessThanUtf8();
        Streams left = Streams.ofValues(rle("label-0", 5))
                .with(Stream.NULLS, new BooleanVector(new boolean[] {false, true, false, false, true}));
        Mask mask = Mask.all(5);

        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            assertThat(function.tryEvaluateTrueMaskInPlace(
                    List.of(left, Streams.ofValues(rle("label-1", 5))),
                    mask,
                    new PrimitiveExecutionContext(allocator))).isTrue();
        }

        assertThat(mask.selectedCount()).isEqualTo(3);
        assertThat(mask.position(0)).isZero();
        assertThat(mask.position(1)).isEqualTo(2);
        assertThat(mask.position(2)).isEqualTo(3);
    }

    private static RleVector rle(String value, int count)
    {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        BinaryVector values = new BinaryVector(1, bytes.length);
        values.setBytes(0, bytes);
        return new RleVector(new int[] {count}, values);
    }
}
