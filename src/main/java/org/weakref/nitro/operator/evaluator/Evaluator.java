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
package org.weakref.nitro.operator.evaluator;

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;

import java.util.Arrays;
import java.util.List;

/**
 * Lazy, memoized, mask-driven expression evaluator.
 * <p>
 * The evaluator manages a list of {@link Function} expressions indexed by ordinal.
 * Expressions are evaluated on demand via {@link #evaluate(int, Mask)} and results
 * are memoized per expression. Evaluation is additive: calling
 * {@code evaluate(i, mask1)} followed by {@code evaluate(i, mask2)} computes only
 * the positions in {@code mask2} not already covered by {@code mask1}.
 * <p>
 * Functions call back into the evaluator via {@link EvaluationContext} to request
 * evaluation of their inputs at whatever masks they require. This allows functions
 * to implement conditional logic (e.g., {@code IF}) by splitting masks internally,
 * without any special-casing in the evaluator.
 * <p>
 * Call {@link #reset()} between batches to clear memoized state.
 */
public class Evaluator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("Evaluator");

    private final List<Function> expressions;
    private final Input input;
    private final Allocator allocator;
    private final Result[] buffers;
    private final Mask[] masks;
    private final EvaluationContext context;

    public Evaluator(List<Function> expressions, Input input, Allocator allocator)
    {
        this.expressions = expressions;
        this.input = input;
        this.allocator = allocator;
        this.buffers = new Result[expressions.size()];
        this.masks = new Mask[expressions.size()];

        this.context = new EvaluationContext()
        {
            @Override
            public Result evaluate(int expressionIndex, Mask mask)
            {
                return Evaluator.this.evaluate(expressionIndex, mask);
            }

            @Override
            public Vector input(int inputIndex, Mask mask)
            {
                return Evaluator.this.input.get(inputIndex, mask);
            }

            @Override
            public Allocator allocator()
            {
                return Evaluator.this.allocator;
            }
        };
    }

    /**
     * Evaluate the expression at the given index for the given mask.
     * <p>
     * Only positions not already computed (per the memoized mask) will be evaluated.
     * The returned result may contain values from previous calls at other positions.
     */
    public Result evaluate(int expressionIndex, Mask mask)
    {
        if (expressionIndex < 0 || expressionIndex >= expressions.size()) {
            throw new IllegalArgumentException("Invalid expression index: " + expressionIndex);
        }

        if (mask.none()) {
            return buffers[expressionIndex];
        }

        Mask alreadyEvaluated = masks[expressionIndex];
        if (alreadyEvaluated != null && alreadyEvaluated.containsAll(mask)) {
            return buffers[expressionIndex];
        }

        Mask remaining;
        if (alreadyEvaluated == null) {
            remaining = mask;
        }
        else {
            remaining = mask.difference(alreadyEvaluated);
            if (remaining.none()) {
                return buffers[expressionIndex];
            }
        }

        buffers[expressionIndex] = expressions.get(expressionIndex).apply(buffers[expressionIndex], remaining, context);

        masks[expressionIndex] = (alreadyEvaluated == null) ? remaining : alreadyEvaluated.or(remaining);

        return buffers[expressionIndex];
    }

    /**
     * Clear all memoized results. Call between batches to allow the evaluator to be reused.
     */
    public void reset()
    {
        Arrays.fill(buffers, null);
        Arrays.fill(masks, null);
        allocator.release(ALLOCATION_CONTEXT);
    }
}
