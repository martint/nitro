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
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.RowPositionIndex;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.operator.pattern.PatternNavigation.Origin.FIRST;
import static org.weakref.nitro.operator.pattern.PatternNavigation.Scope.FINAL;

final class TestIntrinsicPatternValueEvaluators
{
    @Test
    void testAppendsMatchNumberIntoReusableOutput()
    {
        PatternEvaluationContext context = context();
        PatternMatchNumberValueEvaluator evaluator = new PatternMatchNumberValueEvaluator();

        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context allocationContext = new Allocator.Context("match-number");
            Streams output = evaluator.append(context, allocator, allocationContext, Streams.empty(), 0, 2);
            Streams reused = evaluator.append(context, allocator, allocationContext, output, 1, 2);

            assertThat(reused).isSameAs(output);
            assertThat(((I64Vector) output.values()).values()).containsExactly(7, 7);
        }
    }

    @Test
    void testClassifierDelegatesLogicalRepresentationToWriter()
    {
        PatternEvaluationContext context = context();
        AtomicInteger label = new AtomicInteger();
        PatternClassifierValueWriter writer = (ordinal, allocator, allocationContext, output, position, size) -> {
            label.set(ordinal);
            I64Vector values = allocator.allocateOrGrow(
                    allocationContext,
                    output.hasValues() ? (I64Vector) output.values() : null,
                    I64Vector.class,
                    size,
                    I64Vector::new);
            values.values()[position] = ordinal;
            return allocator.reuseOrCreateStreams(output, values, null, null);
        };
        PatternClassifierValueEvaluator selected = new PatternClassifierValueEvaluator(classifier(1), writer);
        PatternClassifierValueEvaluator missing = new PatternClassifierValueEvaluator(classifier(2), writer);

        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context allocationContext = new Allocator.Context("classifier");
            Streams output = selected.append(context, allocator, allocationContext, Streams.empty(), 0, 2);
            assertThat(label).hasValue(1);
            missing.append(context, allocator, allocationContext, output, 1, 2);
            assertThat(label).hasValue(-1);
            assertThat(((I64Vector) output.values()).values()).containsExactly(1, -1);
        }
    }

    private static PatternValuePointer.Classifier classifier(int label)
    {
        return new PatternValuePointer.Classifier(new PatternNavigation(new int[] {label}, FIRST, FINAL, 0, 0));
    }

    private static PatternEvaluationContext context()
    {
        PatternEvaluationContext context = new PatternEvaluationContext(new EmptyRows());
        context.resetMatch(0, 2, 0, 7);
        context.resetRow(1, labels(0, 1));
        return context;
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

    private static final class EmptyRows
            implements RowPositionIndex
    {
        @Override
        public int size()
        {
            return 2;
        }

        @Override
        public Streams column(int column, int position)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int sourcePosition(int position)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean sharesSource(int leftPosition, int rightPosition)
        {
            return true;
        }
    }
}
