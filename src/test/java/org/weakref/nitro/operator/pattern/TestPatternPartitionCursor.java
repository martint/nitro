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
package org.weakref.nitro.operator.pattern;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.core.execution.ExecutionContext;
import org.weakref.nitro.core.execution.ExecutionDiagnostics;
import org.weakref.nitro.core.execution.ExecutionPolicy;
import org.weakref.nitro.core.execution.ExecutionSuspension;
import org.weakref.nitro.core.execution.MemoryReservation;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.operator.RowPositionIndex;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.weakref.nitro.operator.pattern.PatternSkipPolicy.Fixed.PAST_LAST;

final class TestPatternPartitionCursor
{
    @Test
    void testEmitsMatchedAndUnmatchedRows()
    {
        LongRows rows = new LongRows(1, 7, 2);
        try (PrimitiveArrayPool arrays = new PrimitiveArrayPool(1 << 20, 0);
                PatternPartitionCursor cursor = cursor(
                        rows,
                        new PatternExpression.Label(0),
                        List.of(context -> context.rows().longValue(0, context.currentRow()) > 5),
                        arrays)) {
            assertThat(cursor.advance(new TestingExecutionContext())).isTrue();
            assertThat(cursor.inputStart()).isZero();
            assertThat(cursor.matched()).isFalse();

            assertThat(cursor.advance(new TestingExecutionContext())).isTrue();
            assertThat(cursor.inputStart()).isOne();
            assertThat(cursor.matched()).isTrue();
            assertThat(cursor.matchNumber()).isOne();
            assertThat(cursor.match().labelAt(0)).isZero();

            assertThat(cursor.advance(new TestingExecutionContext())).isTrue();
            assertThat(cursor.inputStart()).isEqualTo(2);
            assertThat(cursor.matched()).isFalse();
            assertThat(cursor.advance(new TestingExecutionContext())).isFalse();
        }
    }

    @Test
    void testSkipsPastNonEmptyMatchesAndAdvancesAfterEmptyMatches()
    {
        LongRows rows = new LongRows(1, 2, 3, 4);
        try (PrimitiveArrayPool arrays = new PrimitiveArrayPool(1 << 20, 0);
                PatternPartitionCursor pairs = cursor(
                        rows,
                        new PatternExpression.Concatenation(List.of(
                                new PatternExpression.Label(0),
                                new PatternExpression.Label(0))),
                        List.of(_ -> true),
                        arrays)) {
            assertThat(pairs.advance(new TestingExecutionContext())).isTrue();
            assertThat(pairs.inputStart()).isZero();
            assertThat(pairs.match().size()).isEqualTo(2);
            assertThat(pairs.advance(new TestingExecutionContext())).isTrue();
            assertThat(pairs.inputStart()).isEqualTo(2);
            assertThat(pairs.matchNumber()).isEqualTo(2);
            assertThat(pairs.advance(new TestingExecutionContext())).isFalse();
        }

        try (PrimitiveArrayPool arrays = new PrimitiveArrayPool(1 << 20, 0);
                PatternPartitionCursor empty = cursor(rows, PatternExpression.Empty.EMPTY, List.of(), arrays)) {
            for (int position = 0; position < rows.size(); position++) {
                assertThat(empty.advance(new TestingExecutionContext())).isTrue();
                assertThat(empty.inputStart()).isEqualTo(position);
                assertThat(empty.match().size()).isZero();
            }
            assertThat(empty.advance(new TestingExecutionContext())).isFalse();
        }
    }

    @Test
    void testResumesActiveSearchAfterSuspension()
    {
        LongRows rows = new LongRows(7);
        try (PrimitiveArrayPool arrays = new PrimitiveArrayPool(1 << 20, 0);
                PatternPartitionCursor cursor = cursor(
                        rows,
                        new PatternExpression.Label(0),
                        List.of(_ -> true),
                        arrays)) {
            assertThatThrownBy(() -> cursor.advance(new YieldOnceExecutionContext()))
                    .isInstanceOf(ExecutionSuspension.class);
            assertThat(cursor.advance(new TestingExecutionContext())).isTrue();
            assertThat(cursor.inputStart()).isZero();
            assertThat(cursor.matched()).isTrue();
        }
    }

    private static PatternPartitionCursor cursor(
            RowPositionIndex rows,
            PatternExpression pattern,
            List<PatternDefinition> definitions,
            PrimitiveArrayPool arrays)
    {
        PatternSearch search = new PatternSearch(
                new PatternMatcher(PatternCompiler.compile(pattern), arrays),
                new PatternDefinitionEvaluator(rows, definitions));
        return new PatternPartitionCursor(search, PAST_LAST, 0, rows.size());
    }

    private static final class LongRows
            implements RowPositionIndex
    {
        private final Streams values;

        private LongRows(long... values)
        {
            this.values = Streams.ofValues(new I64Vector(values));
        }

        @Override
        public int size()
        {
            return values.values().length();
        }

        @Override
        public Streams column(int column, int position)
        {
            return values;
        }

        @Override
        public int sourcePosition(int position)
        {
            return position;
        }

        @Override
        public boolean sharesSource(int leftPosition, int rightPosition)
        {
            return true;
        }
    }

    private static class TestingExecutionContext
            implements ExecutionContext
    {
        private final MemoryReservation memory = new TestingMemoryReservation();

        @Override
        public MemoryReservation memory()
        {
            return memory;
        }

        @Override
        public ExecutionPolicy policy()
        {
            return new ExecutionPolicy() {};
        }

        @Override
        public ExecutionDiagnostics diagnostics()
        {
            return (_, _) -> {};
        }

        @Override
        public boolean isYieldRequested()
        {
            return false;
        }

        @Override
        public boolean isCancelled()
        {
            return false;
        }

        @Override
        public void requestMemoryRevocation() {}
    }

    private static final class YieldOnceExecutionContext
            extends TestingExecutionContext
    {
        private boolean yield = true;

        @Override
        public boolean isYieldRequested()
        {
            boolean result = yield;
            yield = false;
            return result;
        }
    }

    private static final class TestingMemoryReservation
            implements MemoryReservation
    {
        @Override
        public CompletionStage<Void> reserve(long bytes)
        {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void release(long bytes) {}

        @Override
        public long reservedBytes()
        {
            return 0;
        }
    }
}
