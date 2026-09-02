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
import org.weakref.nitro.core.execution.ExecutionContext;
import org.weakref.nitro.core.execution.ExecutionDiagnostics;
import org.weakref.nitro.core.execution.ExecutionPolicy;
import org.weakref.nitro.core.execution.ExecutionSuspension;
import org.weakref.nitro.core.execution.MemoryReservation;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.pattern.PatternExpression;
import org.weakref.nitro.operator.pattern.PatternMatchNumberValueEvaluator;
import org.weakref.nitro.operator.pattern.PatternValueProgram;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.weakref.nitro.data.Row.row;
import static org.weakref.nitro.operator.pattern.PatternOutputMode.ONE;
import static org.weakref.nitro.operator.pattern.PatternSkipPolicy.Fixed.PAST_LAST;

final class TestPatternRecognitionOperator
{
    @Test
    void testPullsOneRowPerMatchAndEvaluatesMeasures()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            ConstantTableOperator source = new ConstantTableOperator(allocator, 1, List.of(
                    row(7L), row(8L), row(2L), row(9L), row(10L)));
            try (Operator operator = operator(
                    allocator,
                    new TestingExecutionContext(),
                    source,
                    new PatternExpression.Concatenation(List.of(
                            new PatternExpression.Label(0),
                            new PatternExpression.Label(0))),
                    1)) {
                assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                        .containsExactly(row(7L, 1L), row(9L, 2L));
            }
        }
    }

    @Test
    void testRetainsPartiallyBuiltBatchAcrossSuspension()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            ConstantTableOperator source = new ConstantTableOperator(allocator, 1, List.of(
                    row(7L), row(8L), row(9L)));
            YieldOnSecondCheckpoint context = new YieldOnSecondCheckpoint();
            try (Operator operator = operator(
                    allocator,
                    context,
                    source,
                    new PatternExpression.Label(0),
                    4)) {
                assertThatThrownBy(operator::hasNext).isInstanceOf(ExecutionSuspension.class);
                assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                        .containsExactly(row(7L, 1L), row(8L, 2L), row(9L, 3L));
            }
        }
    }

    @Test
    void testKeepsMatchesWithinContiguousPartitions()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            ConstantTableOperator source = new ConstantTableOperator(allocator, 2, List.of(
                    row(1L, 11L), row(1L, 12L), row(1L, 13L), row(2L, 21L), row(2L, 22L)));
            try (Operator operator = new PatternRecognitionOperator(
                    allocator,
                    new TestingExecutionContext(),
                    source,
                    new int[] {0},
                    new WindowInputOrder(true, 0),
                    0,
                    new int[] {1},
                    new PatternExpression.Concatenation(List.of(
                            new PatternExpression.Label(0),
                            new PatternExpression.Label(0))),
                    List.of(_ -> true),
                    PAST_LAST,
                    ONE,
                    new PatternValueProgram(List.of(new PatternMatchNumberValueEvaluator())),
                    true,
                    8,
                    Schema.unspecified(2),
                    EngineResources.from(allocator).operatorResources())) {
                assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                        .containsExactly(row(11L, 1L), row(21L, 1L));
            }
        }
    }

    @Test
    void testRejectsInputWithoutFullPhysicalOrdering()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            ConstantTableOperator source = new ConstantTableOperator(allocator, 1, List.of(row(7L)));
            assertThatThrownBy(() -> new PatternRecognitionOperator(
                    allocator,
                    new TestingExecutionContext(),
                    source,
                    new int[0],
                    WindowInputOrder.unordered(),
                    1,
                    new int[] {0},
                    new PatternExpression.Label(0),
                    List.of(_ -> true),
                    PAST_LAST,
                    ONE,
                    new PatternValueProgram(List.of()),
                    true,
                    8,
                    Schema.unspecified(1),
                    EngineResources.from(allocator).operatorResources()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("not fully partitioned and ordered");
        }
    }

    private static Operator operator(
            Allocator allocator,
            ExecutionContext context,
            Operator source,
            PatternExpression pattern,
            int outputBatchRows)
    {
        return new PatternRecognitionOperator(
                allocator,
                context,
                source,
                new int[0],
                new WindowInputOrder(true, 0),
                0,
                new int[] {0},
                pattern,
                List.of(definition -> definition.rows().longValue(0, definition.currentRow()) > 5),
                PAST_LAST,
                ONE,
                new PatternValueProgram(List.of(new PatternMatchNumberValueEvaluator())),
                true,
                outputBatchRows,
                Schema.unspecified(2),
                EngineResources.from(allocator).operatorResources());
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

    private static final class YieldOnSecondCheckpoint
            extends TestingExecutionContext
    {
        private int checkpoints;

        @Override
        public boolean isYieldRequested()
        {
            return ++checkpoints == 2;
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
