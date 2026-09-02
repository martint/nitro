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
import org.weakref.nitro.core.execution.MemoryReservation;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.operator.RowPositionIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.weakref.nitro.operator.pattern.PatternOutputMode.ALL_SHOW_EMPTY;
import static org.weakref.nitro.operator.pattern.PatternOutputMode.ALL_WITH_UNMATCHED;
import static org.weakref.nitro.operator.pattern.PatternOutputMode.ONE;
import static org.weakref.nitro.operator.pattern.PatternOutputMode.WINDOW;
import static org.weakref.nitro.operator.pattern.PatternPartitionOutput.Kind.EMPTY_MATCH;
import static org.weakref.nitro.operator.pattern.PatternPartitionOutput.Kind.MATCH;
import static org.weakref.nitro.operator.pattern.PatternPartitionOutput.Kind.UNMATCHED;
import static org.weakref.nitro.operator.pattern.PatternSkipPolicy.Fixed.PAST_LAST;

final class TestPatternPartitionOutput
{
    private static final ExecutionContext CONTEXT = new TestingExecutionContext();

    @Test
    void testOneRowPerMatchPositionsMeasuresAtEndOfMatch()
    {
        LongRows rows = new LongRows(7, 8, 2, 9, 10);
        try (PrimitiveArrayPool arrays = new PrimitiveArrayPool(1 << 20, 0);
                PatternPartitionOutput output = output(
                        rows,
                        new PatternExpression.Concatenation(List.of(new PatternExpression.Label(0), new PatternExpression.Label(0))),
                        List.of(context -> context.rows().longValue(0, context.currentRow()) > 5),
                        ONE,
                        arrays)) {
            assertThat(output.advance(CONTEXT)).isTrue();
            assertThat(output.kind()).isEqualTo(MATCH);
            assertThat(output.sourcePosition()).isZero();
            assertThat(output.measureContext().currentRow()).isOne();
            assertThat(output.measureContext().matchNumber()).isOne();

            assertThat(output.advance(CONTEXT)).isTrue();
            assertThat(output.sourcePosition()).isEqualTo(3);
            assertThat(output.measureContext().currentRow()).isEqualTo(4);
            assertThat(output.measureContext().matchNumber()).isEqualTo(2);
            assertThat(output.advance(CONTEXT)).isFalse();
        }
    }

    @Test
    void testAllRowsSuppressesExclusionsAndEmitsUnmatchedRowsOnce()
    {
        LongRows rows = new LongRows(7, 8, 9, 2, 10);
        PatternExpression pattern = new PatternExpression.Concatenation(List.of(
                new PatternExpression.Label(0),
                new PatternExpression.Exclusion(new PatternExpression.Label(0)),
                new PatternExpression.Label(0)));
        try (PrimitiveArrayPool arrays = new PrimitiveArrayPool(1 << 20, 0);
                PatternPartitionOutput output = output(rows, pattern, List.of(_ -> true), ALL_WITH_UNMATCHED, arrays)) {
            List<PatternPartitionOutput.Kind> kinds = new ArrayList<>();
            List<Integer> positions = new ArrayList<>();
            while (output.advance(CONTEXT)) {
                kinds.add(output.kind());
                positions.add(output.sourcePosition());
                if (output.kind() == MATCH) {
                    assertThat(output.measureContext().currentRow()).isEqualTo(output.sourcePosition());
                }
                else {
                    assertThat(output.measureContext()).isNull();
                }
            }
            assertThat(kinds).containsExactly(MATCH, MATCH, UNMATCHED, UNMATCHED);
            assertThat(positions).containsExactly(0, 2, 3, 4);
        }
    }

    @Test
    void testEmptyMatchHasMeasureContext()
    {
        LongRows rows = new LongRows(7, 8);
        try (PrimitiveArrayPool arrays = new PrimitiveArrayPool(1 << 20, 0);
                PatternPartitionOutput output = output(rows, PatternExpression.Empty.EMPTY, List.of(), ALL_SHOW_EMPTY, arrays)) {
            assertThat(output.advance(CONTEXT)).isTrue();
            assertThat(output.kind()).isEqualTo(EMPTY_MATCH);
            assertThat(output.sourcePosition()).isZero();
            assertThat(output.measureContext().matchNumber()).isOne();

            assertThat(output.advance(CONTEXT)).isTrue();
            assertThat(output.kind()).isEqualTo(EMPTY_MATCH);
            assertThat(output.sourcePosition()).isOne();
            assertThat(output.measureContext().matchNumber()).isEqualTo(2);
            assertThat(output.advance(CONTEXT)).isFalse();
        }
    }

    @Test
    void testWindowModeRequiresFrameAwareExecution()
    {
        LongRows rows = new LongRows(7);
        try (PrimitiveArrayPool arrays = new PrimitiveArrayPool(1 << 20, 0)) {
            PatternPartitionCursor cursor = cursor(rows, new PatternExpression.Label(0), List.of(_ -> true), arrays);
            assertThatThrownBy(() -> new PatternPartitionOutput(cursor, WINDOW))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("frame-aware");
            cursor.close();
        }
    }

    private static PatternPartitionOutput output(
            RowPositionIndex rows,
            PatternExpression pattern,
            List<PatternDefinition> definitions,
            PatternOutputMode mode,
            PrimitiveArrayPool arrays)
    {
        return new PatternPartitionOutput(cursor(rows, pattern, definitions, arrays), mode);
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

    private static final class TestingExecutionContext
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
