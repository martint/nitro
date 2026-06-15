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
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.data.Utf8Traits;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Output;
import org.weakref.nitro.operator.SemiJoinOperator;
import org.weakref.nitro.operator.Streams;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the SemiJoinOperator dictionary-encoded string-key membership bug: the probe side
 * (q56/q60 i_item_id arrives as a hash-join {@link DictionaryVector}) was matched against a stale per-batch
 * dictionary hash cache because {@code GroupingState.contains} drove {@code FlatGroupingTable.findGroup} without
 * a {@code beginBatch}. Result: nondeterministic membership (0 vs all rows across runs). This drives the join
 * many times and asserts a deterministic, correct result.
 */
public class TestSemiJoinDictionaryKey
{
    @Test
    void semiJoinWithDictionaryProbeKeyIsDeterministicAndCorrect()
    {
        for (int iteration = 0; iteration < 200; iteration++) {
            Allocator allocator = new Allocator();
            // Probe (outer): key column is a DictionaryVector over base {A,B,C,D}; rows reference A,B,C,D,B,D.
            DictionaryVector probeKey = new DictionaryVector(
                    new int[] {0, 1, 2, 3, 1, 3},
                    utf8("A", "B", "C", "D"));
            I64Vector payload = new I64Vector(new long[] {10, 20, 30, 40, 50, 60});
            Operator outer = singleBatch(6, Streams.ofValues(probeKey), Streams.ofValues(payload));
            // Membership (inner): {B, D}, supplied as a dictionary too (mirrors a scan/project output).
            DictionaryVector members = new DictionaryVector(new int[] {0, 1}, utf8("B", "D"));
            Operator inner = singleBatch(2, Streams.ofValues(members));

            Operator semiJoin = new SemiJoinOperator(allocator, outer, 0, inner, 0);
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(semiJoin);
            assertThat(rows)
                    .as("iteration %s", iteration)
                    .containsExactly(
                            new Row("B", 20L),
                            new Row("D", 40L),
                            new Row("B", 50L),
                            new Row("D", 60L));
        }
    }

    private static BinaryVector utf8(String... values)
    {
        int totalBytes = 0;
        byte[][] encoded = new byte[values.length][];
        for (int index = 0; index < values.length; index++) {
            encoded[index] = values[index].getBytes(StandardCharsets.UTF_8);
            totalBytes += encoded[index].length;
        }
        BinaryVector vector = new BinaryVector(values.length, totalBytes);
        vector.addTrait(Utf8Traits.UTF8_STRING);
        vector.addTrait(Utf8Traits.ASCII_ONLY);
        for (int index = 0; index < values.length; index++) {
            vector.setBytes(index, encoded[index]);
        }
        return vector;
    }

    private static Operator singleBatch(int rows, Streams... columns)
    {
        return new Operator()
        {
            private boolean hasNext = true;

            @Override
            public int outputCount()
            {
                return columns.length;
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
                Output[] outputs = new Output[columns.length];
                for (int index = 0; index < columns.length; index++) {
                    outputs[index] = Output.of(columns[index]);
                }
                return new Batch(Mask.all(rows), outputs);
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public void close() {}
        };
    }
}
