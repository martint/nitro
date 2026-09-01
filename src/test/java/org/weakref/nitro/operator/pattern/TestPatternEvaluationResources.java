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
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.OperatorResources;
import org.weakref.nitro.operator.RowPositionIndex;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.TestPrimitiveFunctions.primitiveRegistry;

final class TestPatternEvaluationResources
{
    @Test
    void testEvaluatesSelectedRowsInTheirSourceDomain()
    {
        Variable result = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new Call("add", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));
        PatternEvaluationContext context = new PatternEvaluationContext(new TestRows());
        context.resetMatch(0, 2, 0, 1);
        context.resetRow(1, labels(0, 0));

        try (OperatorResources resources = OperatorResources.createDefault();
                Allocator allocator = new Allocator(EngineResources.createDefault())) {
            PatternAggregationInput input = resources.patternEvaluation().aggregationInput(
                    plan,
                    primitiveRegistry(),
                    new int[] {0, 1},
                    Schema.unspecified(1));
            Allocator.Context allocationContext = new Allocator.Context("test-pattern-expression-input");
            input.initialize(allocator, allocationContext);

            input.reset(context, 0, 0);
            assertThat(input.physicalPosition()).isEqualTo(2);
            assertThat(input.physicalSize()).isEqualTo(3);
            assertThat(value(input)).isEqualTo(33);

            input.reset(context, 1, 0);
            assertThat(input.physicalPosition()).isZero();
            assertThat(value(input)).isEqualTo(11);
            input.close();
        }
    }

    private static long value(PatternAggregationInput input)
    {
        return VectorAccess.longValues(input.stream(0, Stream.VALUES)).value(input.physicalPosition());
    }

    private static PatternLabelEvaluator.LabelHistory labels(int... labels)
    {
        return new PatternLabelEvaluator.LabelHistory()
        {
            @Override
            public int size()
            {
                return labels.length;
            }

            @Override
            public int labelAt(int position)
            {
                return labels[position];
            }
        };
    }

    private static final class TestRows
            implements RowPositionIndex
    {
        private final Streams[] columns = {
                Streams.ofValues(new I64Vector(new long[] {10, 20, 30})),
                Streams.ofValues(new I64Vector(new long[] {1, 2, 3})),
        };

        @Override
        public int size()
        {
            return 2;
        }

        @Override
        public Streams column(int column, int position)
        {
            return columns[column];
        }

        @Override
        public int sourcePosition(int position)
        {
            return position == 0 ? 2 : 0;
        }

        @Override
        public int sourceSize(int position)
        {
            return 3;
        }

        @Override
        public boolean sharesSource(int leftPosition, int rightPosition)
        {
            return true;
        }
    }
}
