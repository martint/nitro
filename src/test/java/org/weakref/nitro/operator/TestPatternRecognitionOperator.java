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
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.core.type.TypeVectorFactory;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.pattern.PatternExpression;
import org.weakref.nitro.operator.pattern.PatternMatchNumberValueEvaluator;
import org.weakref.nitro.operator.pattern.PatternNavigation;
import org.weakref.nitro.operator.pattern.PatternScalarValueEvaluator;
import org.weakref.nitro.operator.pattern.PatternValuePointer;
import org.weakref.nitro.operator.pattern.PatternValueProgram;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.weakref.nitro.data.Row.row;
import static org.weakref.nitro.operator.pattern.PatternNavigation.Origin.LAST;
import static org.weakref.nitro.operator.pattern.PatternNavigation.Scope.RUNNING;
import static org.weakref.nitro.operator.pattern.PatternOutputMode.ALL_WITH_UNMATCHED;
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
    void testResumesInputLoadingAfterExecutionSuspension()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Operator delegate = new TableOperator(
                    1,
                    List.of(
                            TableOperator.Page.values(1, new Vector[] {new I64Vector(new long[] {7})}, Mask.all(1)),
                            TableOperator.Page.values(1, new Vector[] {new I64Vector(new long[] {8})}, Mask.all(1))));
            Operator source = new Operator()
            {
                private int hasNextCalls;

                @Override
                public int outputCount()
                {
                    return delegate.outputCount();
                }

                @Override
                public Schema outputSchema()
                {
                    return delegate.outputSchema();
                }

                @Override
                public boolean hasNext()
                {
                    if (++hasNextCalls == 2) {
                        throw ExecutionSuspension.yield();
                    }
                    return delegate.hasNext();
                }

                @Override
                public Batch next()
                {
                    return delegate.next();
                }

                @Override
                public void constrain(Mask mask)
                {
                    delegate.constrain(mask);
                }

                @Override
                public void close()
                {
                    delegate.close();
                }
            };

            try (Operator operator = operator(
                    allocator,
                    new TestingExecutionContext(),
                    source,
                    new PatternExpression.Label(0),
                    4)) {
                assertThatThrownBy(operator::hasNext).isSameAs(ExecutionSuspension.yield());
                assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                        .containsExactly(row(7L, 1L), row(8L, 2L));
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
    void testAllRowsWithUnmatchedProducesNullMeasures()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            ConstantTableOperator source = new ConstantTableOperator(allocator, 1, List.of(
                    row(2L), row(7L), row(8L), row(2L), row(9L), row(10L)));
            try (Operator operator = new PatternRecognitionOperator(
                    allocator,
                    new TestingExecutionContext(),
                    source,
                    new int[0],
                    new WindowInputOrder(true, 0),
                    0,
                    new int[] {0},
                    new PatternExpression.Concatenation(List.of(
                            new PatternExpression.Label(0),
                            new PatternExpression.Label(0))),
                    List.of(definition -> definition.rows().longValue(0, definition.currentRow()) > 5),
                    PAST_LAST,
                    ALL_WITH_UNMATCHED,
                    new PatternValueProgram(List.of(new PatternMatchNumberValueEvaluator())),
                    true,
                    8,
                    new Schema(List.of(new Field(longType(), false), new Field(longType(), true))),
                    EngineResources.from(allocator).operatorResources())) {
                assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                        .containsExactly(
                                row(2L, null),
                                row(7L, 1L),
                                row(8L, 1L),
                                row(2L, null),
                                row(9L, 2L),
                                row(10L, 2L));
            }
        }
    }

    @Test
    void testEvaluatesCompiledDefinitionOverMatchLocalValues()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            ConstantTableOperator source = new ConstantTableOperator(allocator, 1, List.of(
                    row(true), row(false), row(true)));
            Reference predicate = new Reference(new Input(0), Stream.VALUES);
            PatternValueProgram inputs = new PatternValueProgram(List.of(new PatternScalarValueEvaluator(
                    new PatternValuePointer.Scalar(
                            0,
                            new PatternNavigation(new int[0], LAST, RUNNING, 0, 0)))));
            var definition = EngineResources.from(allocator).operatorResources().patternEvaluation().definition(
                    allocator,
                    new EvaluationPlan(List.of(), List.of(predicate)),
                    new PrimitiveRegistry(),
                    inputs,
                    predicate,
                    (errors, position) -> assertThat(org.weakref.nitro.data.VectorAccess.booleanValues(errors).value(position)).isFalse());
            PatternValueProgram measureInputs = new PatternValueProgram(List.of(new PatternScalarValueEvaluator(
                    new PatternValuePointer.Scalar(
                            0,
                            new PatternNavigation(new int[0], LAST, RUNNING, 0, 0)))));
            var measure = EngineResources.from(allocator).operatorResources().patternEvaluation().value(
                    allocator,
                    new EvaluationPlan(List.of(), List.of(predicate)),
                    new PrimitiveRegistry(),
                    measureInputs,
                    predicate);

            try (Operator operator = new PatternRecognitionOperator(
                    allocator,
                    new TestingExecutionContext(),
                    source,
                    new int[0],
                    new WindowInputOrder(true, 0),
                    0,
                    new int[] {0},
                    new PatternExpression.Label(0),
                    List.of(definition),
                    PAST_LAST,
                    ONE,
                    new PatternValueProgram(List.of(measure)),
                    true,
                    8,
                    Schema.unspecified(2),
                    EngineResources.from(allocator).operatorResources())) {
                assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                        .containsExactly(row(1L, 1L), row(1L, 1L));
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

    private static TypeBinding longType()
    {
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:bigint");
            }

            @Override
            public Class<?> carrierType()
            {
                return long.class;
            }

            @Override
            public TypeOperators operators()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<TypeVectorFactory> vectorFactory()
            {
                return Optional.of(new TypeVectorFactory()
                {
                    @Override
                    public Vector constant(org.weakref.nitro.data.VectorAllocator allocator, Object value, int length)
                    {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Vector nullValues(org.weakref.nitro.data.VectorAllocator allocator, int length)
                    {
                        return allocator.allocate(I64Vector.class, length, I64Vector::new);
                    }
                });
            }

            @Override
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return Set.of(I64Vector.class);
            }
        };
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
