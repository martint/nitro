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
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Output;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.aggregation.CountAll;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for multi-long-key grouping with a NULL in one key over differing underlying values
 * (TPC-DS q75/q77: a GroupId rollup / LEFT-join leaves the original value under the null bit). All rows whose
 * key tuple is logically {@code (k, NULL)} must collapse into a single group regardless of the stale value
 * carried beneath the null flag.
 */
public class TestMultiKeyNullGrouping
{
    @Test
    void multiLongKeyCollapsesNullsWithDifferingUnderlyingValues()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        I64Vector key0 = new I64Vector(new long[] {7, 7, 7});
        // key1 is NULL on all rows, but carries different stale values (5, 9, 13) under the null flag.
        I64Vector key1 = new I64Vector(new long[] {5, 9, 13});
        BooleanVector key1Nulls = new BooleanVector(new boolean[] {true, true, true});

        Operator source = new Operator()
        {
            private boolean hasNext = true;

            @Override
            public int outputCount()
            {
                return 2;
            }

            @Override
            public boolean hasNext()
            {
                return hasNext;
            }

            @Override
            public Batch next()
            {
                hasNext = false;
                return new Batch(
                        Mask.all(3),
                        Output.of(Streams.ofValues(key0)),
                        Output.of(Streams.ofValuesAndNulls(key1, key1Nulls)));
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public void close() {}
        };

        Operator grouped = new GroupedAggregationOperator(allocator, List.of(0, 1), List.of(new CountAll()), source);
        List<Row> rows = OperatorAssertions.OperatorAssert.toRows(grouped);
        assertThat(rows).containsExactly(new Row(7L, null, 3L));
    }

    @Test
    void flatKeyGroupingCollapsesNullsWithDifferingUnderlyingValues()
    {
        // (string, long) key forces the FlatGroupingTable path (mixed types). The long key is NULL on all rows
        // but carries different stale values (5, 9, 13) under the null flag (TPC-DS q77 ROLLUP subtotal). All
        // rows whose tuple is logically ("c", NULL) must collapse into one group.
        Allocator allocator = new Allocator(EngineResources.createDefault());
        org.weakref.nitro.data.BinaryVector key0 = utf8("c", "c", "c");
        I64Vector key1 = new I64Vector(new long[] {5, 9, 13});
        BooleanVector key1Nulls = new BooleanVector(new boolean[] {true, true, true});

        Operator source = new Operator()
        {
            private boolean hasNext = true;

            @Override
            public int outputCount()
            {
                return 2;
            }

            @Override
            public boolean hasNext()
            {
                return hasNext;
            }

            @Override
            public Batch next()
            {
                hasNext = false;
                return new Batch(
                        Mask.all(3),
                        Output.of(Streams.ofValues(key0)),
                        Output.of(Streams.ofValuesAndNulls(key1, key1Nulls)));
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public void close() {}
        };

        Operator grouped = new GroupedAggregationOperator(allocator, List.of(0, 1), List.of(new CountAll()), source);
        List<Row> rows = OperatorAssertions.OperatorAssert.toRows(grouped);
        assertThat(rows).containsExactly(new Row("c", null, 3L));
    }

    private static org.weakref.nitro.data.BinaryVector utf8(String... values)
    {
        int totalBytes = 0;
        byte[][] encoded = new byte[values.length][];
        for (int index = 0; index < values.length; index++) {
            encoded[index] = values[index].getBytes(java.nio.charset.StandardCharsets.UTF_8);
            totalBytes += encoded[index].length;
        }
        org.weakref.nitro.data.BinaryVector vector = new org.weakref.nitro.data.BinaryVector(values.length, totalBytes);
        vector.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        vector.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        for (int index = 0; index < values.length; index++) {
            vector.setBytes(index, encoded[index]);
        }
        return vector;
    }
}
