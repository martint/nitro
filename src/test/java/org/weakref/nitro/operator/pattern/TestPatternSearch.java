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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.assertj.core.api.Assertions.assertThat;

final class TestPatternSearch
{
    @Test
    void testSeekAdvancesUntilDefinitionMatches()
    {
        LongRows rows = new LongRows(1, 2, 7);
        PatternDefinitionEvaluator evaluator = new PatternDefinitionEvaluator(
                rows,
                List.of(context -> context.rows().longValue(0, context.currentRow()) > 5));

        try (PrimitiveArrayPool arrays = new PrimitiveArrayPool(1 << 20, 0);
                PatternSearch.Session search = new PatternSearch(
                        new PatternMatcher(PatternCompiler.compile(new PatternExpression.Label(0)), arrays),
                        evaluator)
                        .start(0, rows.size(), 0, false, 1)) {
            assertThat(search.run(new TestingExecutionContext())).isTrue();
            assertThat(search.patternStart()).isEqualTo(2);
            PatternLabelEvaluator.LabelHistory labels = search.match();
            assertThat(labels.size()).isOne();
            assertThat(labels.labelAt(0)).isZero();
        }
    }

    @Test
    void testInitialDoesNotAdvanceAfterFailure()
    {
        LongRows rows = new LongRows(1, 7);
        PatternDefinitionEvaluator evaluator = new PatternDefinitionEvaluator(
                rows,
                List.of(context -> context.rows().longValue(0, context.currentRow()) > 5));

        try (PrimitiveArrayPool arrays = new PrimitiveArrayPool(1 << 20, 0);
                PatternSearch.Session search = new PatternSearch(
                        new PatternMatcher(PatternCompiler.compile(new PatternExpression.Label(0)), arrays),
                        evaluator)
                        .start(0, rows.size(), 0, true, 1)) {
            assertThat(search.run(new TestingExecutionContext())).isFalse();
            assertThat(search.patternStart()).isZero();
        }
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
