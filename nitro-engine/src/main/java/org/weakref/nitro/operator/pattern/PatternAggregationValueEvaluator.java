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

import static java.util.Objects.requireNonNull;

/// Binds an aggregate implementation to its RUNNING or FINAL pattern-label domain.
public final class PatternAggregationValueEvaluator
        implements PatternValueEvaluator
{
    private final PatternAggregationSet set;
    private final PatternAggregationFunction function;
    private final PatternAggregationRows rows = new PatternAggregationRows();

    public PatternAggregationValueEvaluator(PatternAggregationSet set, PatternAggregationFunction function)
    {
        this.set = requireNonNull(set, "set is null");
        this.function = requireNonNull(function, "function is null");
    }

    @Override
    public Streams append(
            PatternEvaluationContext context,
            Allocator allocator,
            Allocator.Context allocationContext,
            Streams output,
            int outputPosition,
            int outputSize)
    {
        requireNonNull(context, "context is null");
        requireNonNull(allocator, "allocator is null");
        requireNonNull(allocationContext, "allocationContext is null");
        requireNonNull(output, "output is null");
        rows.reset(context, set);
        return requireNonNull(function.evaluate(
                context,
                rows,
                allocator,
                allocationContext,
                output,
                outputPosition,
                outputSize), "pattern aggregation returned null");
    }

    @Override
    public void close()
    {
        function.close();
    }
}
