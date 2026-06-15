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
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.GroupIdOperator;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Output;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.aggregation.Sum;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ROLLUP(channel, id) over data that contains a genuine NULL detail key (null id) must keep the null-id DETAIL
 * row distinct from the channel SUBTOTAL row via the grouping-set discriminator (group_id). Models TPC-DS q77.
 */
public class TestRollupNullDetailKey
{
    @Test
    void rollupKeepsNullDetailDistinctFromSubtotal()
    {
        Allocator allocator = new Allocator();
        // channel discriminator = 1 for all rows; id = {10, NULL, 20}; value = {1, 2, 3}.
        I64Vector channel = new I64Vector(new long[] {1, 1, 1});
        I64Vector id = new I64Vector(new long[] {10, 99, 20});
        BooleanVector idNulls = new BooleanVector(new boolean[] {false, true, false});
        I64Vector value = new I64Vector(new long[] {1, 2, 3});

        Operator source = new Operator()
        {
            private boolean hasNext = true;

            @Override
            public int outputCount()
            {
                return 3;
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
                        Output.of(Streams.ofValues(channel)),
                        Output.of(Streams.ofValuesAndNulls(id, idNulls)),
                        Output.of(Streams.ofValues(value)));
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public void close() {}
        };

        // ROLLUP(channel, id): detail {channel,id,value} and subtotal {channel, NULL-id, value}.
        Operator grouped = new GroupIdOperator(allocator, source, new int[][] {
                {0, 1, 2},
                {0, -1, 2}});
        // Group by channel(0), id(1), group_id(3); sum value(2).
        Operator aggregated = new GroupedAggregationOperator(allocator, List.of(0, 1, 3), List.of(new Sum(2)), grouped);

        List<Row> rows = OperatorAssertions.OperatorAssert.toRows(aggregated);
        // detail: (1,10,gid0,1) (1,NULL,gid0,2) (1,20,gid0,3); subtotal: (1,NULL,gid1,6).
        assertThat(rows).containsExactlyInAnyOrder(
                new Row(1L, 10L, 0L, 1L),
                new Row(1L, null, 0L, 2L),
                new Row(1L, 20L, 0L, 3L),
                new Row(1L, null, 1L, 6L));
    }
}
