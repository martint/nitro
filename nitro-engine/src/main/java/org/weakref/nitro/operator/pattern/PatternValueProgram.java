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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Streams;

import java.util.List;

import static java.util.Objects.requireNonNull;

/// Ordered set of registry-bound pattern values appended into caller-owned reusable columns.
public final class PatternValueProgram
        implements AutoCloseable
{
    private final List<PatternValueEvaluator> evaluators;

    public PatternValueProgram(List<PatternValueEvaluator> evaluators)
    {
        this.evaluators = List.copyOf(requireNonNull(evaluators, "evaluators is null"));
    }

    public int size()
    {
        return evaluators.size();
    }

    public Streams[] append(
            PatternEvaluationContext context,
            Allocator allocator,
            Allocator.Context allocationContext,
            Streams[] output,
            int outputPosition,
            int outputSize)
    {
        requireNonNull(context, "context is null");
        requireNonNull(allocator, "allocator is null");
        requireNonNull(allocationContext, "allocationContext is null");
        if (outputPosition < 0 || outputPosition >= outputSize) {
            throw new IllegalArgumentException("outputPosition is outside output");
        }
        if (output == null) {
            output = new Streams[evaluators.size()];
        }
        else if (output.length != evaluators.size()) {
            throw new IllegalArgumentException("output column count does not match value program");
        }

        for (int index = 0; index < evaluators.size(); index++) {
            Streams column = requireNonNull(evaluators.get(index).append(
                    context,
                    allocator,
                    allocationContext,
                    output[index] == null ? Streams.empty() : output[index],
                    outputPosition,
                    outputSize), "pattern value evaluator returned null");
            if (column.values().length() != outputSize) {
                throw new IllegalStateException("pattern value evaluator returned the wrong output size");
            }
            output[index] = column;
        }
        return output;
    }

    @Override
    public void close()
    {
        RuntimeException failure = null;
        for (PatternValueEvaluator evaluator : evaluators) {
            try {
                evaluator.close();
            }
            catch (RuntimeException closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                }
                else {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
