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
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.weakref.nitro.data.Row.row;

class TestOrderedMergeSession
{
    @Test
    void testEmptySetOfSourcesIsImmediatelyFinished()
    {
        EngineResources resources = EngineResources.createDefault();
        Allocator allocator = new Allocator(resources);
        try (Operator schema = table(allocator, row(0L));
                OrderedMergeSession merge = merge(
                        allocator,
                        resources,
                        schema,
                        0,
                        4,
                        new int[] {0},
                        new boolean[] {false},
                        new boolean[] {false})) {
            assertThat(merge.hasOutput()).isFalse();
            assertThat(merge.isFinished()).isTrue();
        }
        finally {
            allocator.close();
            resources.close();
        }
    }

    @Test
    void testStreamsMultipleSourcesAndBoundsOutput()
    {
        EngineResources resources = EngineResources.createDefault();
        Allocator allocator = new Allocator(resources);
        try (Operator first = table(allocator, row(1L, 10L), row(4L, 40L), row(7L, 70L));
                Operator second = table(allocator, row(2L, 20L), row(3L, 30L), row(8L, 80L));
                Operator third = table(allocator, row(5L, 50L), row(6L, 60L), row(9L, 90L));
                OrderedMergeSession merge = merge(allocator, resources, first, 3, 2, new int[] {0}, new boolean[] {false}, new boolean[] {false})) {
            merge.addInput(0, first.next());
            merge.addInput(1, second.next());
            merge.addInput(2, third.next());
            merge.finishSource(0);
            merge.finishSource(1);
            merge.finishSource(2);

            List<Long> keys = new ArrayList<>();
            List<Long> payloads = new ArrayList<>();
            while (!merge.isFinished()) {
                assertThat(merge.hasOutput()).isTrue();
                try (Batch output = merge.getOutput()) {
                    assertThat(output.borrowMask().selectedCount()).isLessThanOrEqualTo(2);
                    appendLongs(output, 0, keys);
                    appendLongs(output, 1, payloads);
                }
            }
            assertThat(keys).containsExactly(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
            assertThat(payloads).containsExactly(10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L, 90L);
        }
        finally {
            allocator.close();
            resources.close();
        }
    }

    @Test
    void testWaitsForNextHeadBeforeContinuing()
    {
        EngineResources resources = EngineResources.createDefault();
        Allocator allocator = new Allocator(resources);
        try (Operator firstPage = table(allocator, row(1L), row(3L));
                Operator secondPage = table(allocator, row(2L), row(4L));
                Operator firstContinuation = table(allocator, row(5L), row(7L));
                OrderedMergeSession merge = merge(allocator, resources, firstPage, 2, 16, new int[] {0}, new boolean[] {false}, new boolean[] {false})) {
            merge.addInput(0, firstPage.next());
            merge.addInput(1, secondPage.next());
            merge.finishSource(1);

            try (Batch output = merge.getOutput()) {
                assertThat(longs(output, 0)).containsExactly(1L, 2L, 3L);
            }
            assertThat(merge.needsInput(0)).isTrue();
            assertThat(merge.hasOutput()).isFalse();

            merge.addInput(0, firstContinuation.next());
            merge.finishSource(0);
            try (Batch output = merge.getOutput()) {
                assertThat(longs(output, 0)).containsExactly(4L, 5L, 7L);
            }
            assertThat(merge.isFinished()).isTrue();
        }
        finally {
            allocator.close();
            resources.close();
        }
    }

    @Test
    void testDirectionNullPlacementAndMultipleKeys()
    {
        EngineResources resources = EngineResources.createDefault();
        Allocator allocator = new Allocator(resources);
        try (Operator schema = table(allocator, row(0L, 0L));
                OrderedMergeSession merge = merge(
                        allocator,
                        resources,
                        schema,
                        2,
                        16,
                        new int[] {0, 1},
                        new boolean[] {false, true},
                        new boolean[] {true, false})) {
            merge.addInput(0, nullableBatch(
                    new long[] {0, 1, 1},
                    new boolean[] {true, false, false},
                    new long[] {0, 9, 3},
                    new boolean[] {false, false, false}));
            merge.addInput(1, nullableBatch(
                    new long[] {0, 1, 2},
                    new boolean[] {true, false, false},
                    new long[] {0, 7, 1},
                    new boolean[] {true, false, false}));
            merge.finishSource(0);
            merge.finishSource(1);

            try (Batch output = merge.getOutput()) {
                assertThat(nulls(output, 0)).containsExactly(true, true, false, false, false, false);
                assertThat(nulls(output, 1)).containsExactly(false, true, false, false, false, false);
                assertThat(longs(output, 1)).containsExactly(0L, 0L, 9L, 7L, 3L, 1L);
            }
        }
        finally {
            allocator.close();
            resources.close();
        }
    }

    @Test
    void testMergesEncodedOrderingVectorsAndSparseMasks()
    {
        EngineResources resources = EngineResources.createDefault();
        Allocator allocator = new Allocator(resources);
        try (Operator schema = table(allocator, row(0L));
                OrderedMergeSession merge = merge(allocator, resources, schema, 2, 16, new int[] {0}, new boolean[] {false}, new boolean[] {false})) {
            merge.addInput(0, new Batch(
                    Mask.sparse(new int[] {0, 2}, 3),
                    Output.of(Streams.ofValues(DictionaryVector.wrap(
                            new int[] {0, 1, 2},
                            new I64Vector(new long[] {1, 99, 5}))))));
            merge.addInput(1, new Batch(
                    Mask.all(2),
                    Output.of(Streams.ofValues(new RleVector(
                            new int[] {1, 1},
                            new I64Vector(new long[] {2, 6}))))));
            merge.finishSource(0);
            merge.finishSource(1);

            try (Batch output = merge.getOutput()) {
                assertThat(longs(output, 0)).containsExactly(1L, 2L, 5L, 6L);
            }
        }
        finally {
            allocator.close();
            resources.close();
        }
    }

    @Test
    void testClosesConsumedAndAbandonedInputAndRequiresOutputClose()
    {
        EngineResources resources = EngineResources.createDefault();
        Allocator allocator = new Allocator(resources);
        AtomicInteger closes = new AtomicInteger();
        try (Operator schema = table(allocator, row(0L));
                OrderedMergeSession merge = merge(allocator, resources, schema, 2, 1, new int[] {0}, new boolean[] {false}, new boolean[] {false})) {
            merge.addInput(0, trackedBatch(closes, 1, 3));
            merge.addInput(1, trackedBatch(closes, 2, 4));
            merge.finishSource(0);
            merge.finishSource(1);

            Batch output = merge.getOutput();
            assertThat(merge.hasOutput()).isFalse();
            assertThatThrownBy(merge::getOutput).isInstanceOf(IllegalStateException.class);
            output.close();
            assertThat(merge.hasOutput()).isTrue();
        }
        assertThat(closes).hasValue(2);
        allocator.close();
        resources.close();
    }

    private static OrderedMergeSession merge(
            Allocator allocator,
            EngineResources resources,
            Operator schema,
            int sources,
            int maxOutputRows,
            int[] columns,
            boolean[] descending,
            boolean[] nullsFirst)
    {
        return new OrderedMergeSession(
                allocator,
                columns,
                descending,
                nullsFirst,
                schema.outputSchema(),
                sources,
                maxOutputRows,
                resources.operatorResources());
    }

    private static Operator table(Allocator allocator, org.weakref.nitro.data.Row... rows)
    {
        return new ConstantTableOperator(allocator, rows[0].values().length, List.of(rows));
    }

    private static Batch nullableBatch(long[] first, boolean[] firstNulls, long[] second, boolean[] secondNulls)
    {
        return new Batch(
                Mask.all(first.length),
                Output.of(Streams.ofValuesAndNulls(new I64Vector(first), new BooleanVector(firstNulls))),
                Output.of(Streams.ofValuesAndNulls(new I64Vector(second), new BooleanVector(secondNulls))));
    }

    private static Batch trackedBatch(AtomicInteger closes, long... values)
    {
        return new Batch(
                Mask.all(values.length),
                _ -> {},
                java.util.function.Function.identity(),
                _ -> {},
                closes::incrementAndGet,
                Output.of(Streams.ofValues(new I64Vector(values))));
    }

    private static long[] longs(Batch batch, int column)
    {
        VectorAccess.LongValues values = VectorAccess.longValues(batch.output(column).borrow(Stream.VALUES));
        long[] result = new long[batch.borrowMask().selectedCount()];
        for (int index = 0; index < result.length; index++) {
            result[index] = values.value(batch.borrowMask().position(index));
        }
        return result;
    }

    private static boolean[] nulls(Batch batch, int column)
    {
        VectorAccess.BooleanValues values = VectorAccess.booleanValues(batch.output(column).borrow(Stream.NULLS));
        boolean[] result = new boolean[batch.borrowMask().selectedCount()];
        for (int index = 0; index < result.length; index++) {
            result[index] = values.value(batch.borrowMask().position(index));
        }
        return result;
    }

    private static void appendLongs(Batch batch, int column, List<Long> output)
    {
        for (long value : longs(batch, column)) {
            output.add(value);
        }
    }
}
