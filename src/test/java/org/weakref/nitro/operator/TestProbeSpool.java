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
import org.weakref.nitro.OperatorAssertions;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.data.Vector;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TestProbeSpool
{
    @Test
    void testCompleteInputCollectsKeysAndReplaysRows()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        ProbeSpool spool = new ProbeSpool(allocator, new TableOperator(2, List.of(
                page(new long[] {1, 2, 3}, new long[] {11, 12, 13}),
                page(new long[] {4, 2}, new long[] {14, 12}))), 2);

        var keys = spool.prepare(new int[] {0, 1}, 8);

        assertThat(keys).isNotNull();
        assertThat(keys[0]).containsExactlyInAnyOrder(1L, 2L, 3L, 4L);
        assertThat(keys[1]).containsExactlyInAnyOrder(11L, 12L, 13L, 14L);
        assertThat(OperatorAssertions.OperatorAssert.toRows(spool)).containsExactly(
                new Row(1L, 11L),
                new Row(2L, 12L),
                new Row(3L, 13L),
                new Row(4L, 14L),
                new Row(2L, 12L));
    }

    @Test
    void testOverflowReplaysPrefixAndRemainingSourceWithoutFilter()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        ProbeSpool spool = new ProbeSpool(allocator, new TableOperator(1, List.of(
                page(new long[] {1, 2, 3}),
                page(new long[] {4, 5, 6}),
                page(new long[] {7, 8, 9}))), 1);

        assertThat(spool.prepare(new int[] {0}, 4)).isNull();
        assertThat(OperatorAssertions.OperatorAssert.toRows(spool)).containsExactly(
                new Row(1L), new Row(2L), new Row(3L),
                new Row(4L), new Row(5L), new Row(6L),
                new Row(7L), new Row(8L), new Row(9L));
    }

    private static TableOperator.Page page(long[]... columns)
    {
        Vector[] vectors = new Vector[columns.length];
        for (int index = 0; index < columns.length; index++) {
            vectors[index] = new I64Vector(columns[index]);
        }
        return TableOperator.Page.values(columns[0].length, vectors, Mask.all(columns[0].length));
    }
}
