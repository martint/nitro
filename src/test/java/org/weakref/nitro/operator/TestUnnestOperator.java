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
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.ArrayVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.MapVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.StructVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.execution.EngineResources;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.OperatorAssertions.operator;
import static org.weakref.nitro.data.Row.row;

public class TestUnnestOperator
{
    @Test
    void testExpandsArrayInBoundedBatches()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        ArrayVector arrays = array(new int[] {0, 2, 2, 5}, 10, 11, 30, 31, 32);
        Operator source = new TableOperator(
                Schema.unspecified(2),
                List.of(TableOperator.Page.values(
                        3,
                        new Vector[] {new I64Vector(new long[] {1, 2, 3}), arrays},
                        Mask.all(3))));
        Field output = Schema.unspecified(1).field(0);

        Operator unnest = new UnnestOperator(
                allocator,
                source,
                new int[] {0},
                List.of(UnnestOperator.Mapping.direct(1, List.of(output))),
                Optional.empty(),
                false,
                2);

        assertThat(operator(unnest)).matchesExactly(List.of(
                row(1L, 10L),
                row(1L, 11L),
                row(3L, 30L),
                row(3L, 31L),
                row(3L, 32L)));
    }

    @Test
    void testZipsArrayAndMapWithOuterPaddingAndOrdinality()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        ArrayVector arrays = array(new int[] {0, 2, 2}, 10, 11);
        MapVector maps = new MapVector(2);
        System.arraycopy(new int[] {0, 1, 1}, 0, maps.offsets(), 0, 3);
        maps.setEntries(
                Streams.ofValues(new I64Vector(new long[] {100})),
                Streams.ofValues(new I64Vector(new long[] {1_000})));
        Operator source = new TableOperator(
                Schema.unspecified(3),
                List.of(TableOperator.Page.values(
                        2,
                        new Vector[] {new I64Vector(new long[] {7, 8}), arrays, maps},
                        Mask.all(2))));
        Field output = Schema.unspecified(1).field(0);

        Operator unnest = new UnnestOperator(
                allocator,
                source,
                new int[] {0},
                List.of(
                        UnnestOperator.Mapping.direct(1, List.of(output)),
                        UnnestOperator.Mapping.direct(2, List.of(output, output))),
                Optional.of(output),
                true,
                16);

        assertThat(operator(unnest)).matchesExactly(List.of(
                row(7L, 10L, 100L, 1_000L, 1L),
                row(7L, 11L, null, null, 2L),
                row(8L, null, null, null, 1L)));
    }

    @Test
    void testPreservesDictionaryStructuralDomain()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        ArrayVector domain = array(new int[] {0, 2, 3}, 10, 11, 20);
        DictionaryVector arrays = DictionaryVector.wrap(new int[] {0, 1, 0}, domain);
        Operator source = new TableOperator(
                Schema.unspecified(1),
                List.of(TableOperator.Page.values(
                        3,
                        new Vector[] {arrays},
                        Mask.all(3))));
        Field output = Schema.unspecified(1).field(0);

        Operator unnest = new UnnestOperator(
                allocator,
                source,
                new int[0],
                List.of(UnnestOperator.Mapping.direct(0, List.of(output))),
                Optional.empty(),
                false,
                16);

        assertThat(operator(unnest)).matchesExactly(List.of(
                row(10L), row(11L), row(20L), row(10L), row(11L)));
    }

    @Test
    void testFlattensArrayOfRowsAndInheritsRowNulls()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        StructVector rows = new StructVector(3);
        rows.setField("id", Streams.ofValues(new I64Vector(new long[] {10, 20, 30})));
        rows.setField("score", Streams.ofValues(new I64Vector(new long[] {100, 200, 300})));
        ArrayVector arrays = new ArrayVector(2);
        System.arraycopy(new int[] {0, 2, 3}, 0, arrays.offsets(), 0, 3);
        arrays.setElements(Streams.ofValuesAndNulls(
                rows,
                new BooleanVector(new boolean[] {false, true, false})));
        Operator source = new TableOperator(
                Schema.unspecified(1),
                List.of(TableOperator.Page.values(
                        2,
                        new Vector[] {arrays},
                        Mask.all(2))));
        Field output = Schema.unspecified(1).field(0);

        Operator unnest = new UnnestOperator(
                allocator,
                source,
                new int[0],
                List.of(new UnnestOperator.Mapping(
                        0,
                        List.of(
                                new UnnestOperator.OutputMapping(0, List.of(0), output),
                                new UnnestOperator.OutputMapping(0, List.of(1), output)))),
                Optional.empty(),
                false,
                16);

        assertThat(operator(unnest)).matchesExactly(List.of(
                row(10L, 100L),
                row(null, null),
                row(30L, 300L)));
    }

    @Test
    void testForwardsContiguousRepeatedChildren()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        I64Vector elements = new I64Vector(new long[] {10, 11, 20});
        ArrayVector arrays = new ArrayVector(2);
        System.arraycopy(new int[] {0, 2, 3}, 0, arrays.offsets(), 0, 3);
        arrays.setElements(Streams.ofValues(elements));
        Operator source = new TableOperator(
                Schema.unspecified(1),
                List.of(TableOperator.Page.values(
                        2,
                        new Vector[] {arrays},
                        Mask.all(2))));
        Operator unnest = new UnnestOperator(
                allocator,
                source,
                new int[0],
                List.of(UnnestOperator.Mapping.direct(0, List.of(Schema.unspecified(1).field(0)))),
                Optional.empty(),
                false,
                16);

        try (Batch batch = unnest.next()) {
            assertThat(batch.borrowMask().selectedCount()).isEqualTo(3);
            assertThat(batch.output(0).borrow(Stream.VALUES)).isSameAs(elements);
        }
        assertThat(unnest.hasNext()).isFalse();
        unnest.close();
    }

    private static ArrayVector array(int[] offsets, long... elements)
    {
        ArrayVector array = new ArrayVector(offsets.length - 1);
        System.arraycopy(offsets, 0, array.offsets(), 0, offsets.length);
        array.setElements(Streams.ofValues(new I64Vector(elements)));
        return array;
    }
}
