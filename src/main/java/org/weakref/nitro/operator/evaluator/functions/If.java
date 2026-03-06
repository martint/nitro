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
package org.weakref.nitro.operator.evaluator.functions;

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.EvaluationContext;
import org.weakref.nitro.operator.evaluator.Function;

/**
 * Conditional expression: {@code IF(condition, ifTrue, ifFalse)}.
 * <p>
 * Evaluates the condition to produce a boolean mask, then evaluates the
 * true branch for positions where the condition holds and the false branch
 * for the remaining positions. Both results are merged in-place into the
 * output vector.
 * <p>
 * The evaluator has no knowledge of this function's conditional semantics;
 * it is hidden entirely within this implementation via the
 * {@link EvaluationContext} callback protocol.
 */
public class If
        implements Function
{
    private static final Allocator.Context CONTEXT = new Allocator.Context("If");

    private final int condition;
    private final int ifTrue;
    private final int ifFalse;

    public If(int condition, int ifTrue, int ifFalse)
    {
        this.condition = condition;
        this.ifTrue = ifTrue;
        this.ifFalse = ifFalse;
    }

    @Override
    public Vector apply(Vector output, Mask mask, EvaluationContext context)
    {
        BooleanVector cond = (BooleanVector) context.evaluate(condition, mask);
        Mask trueMask = mask.and(cond);
        Mask falseMask = mask.andNot(cond);

        Vector trueResult = trueMask.none() ? null : context.evaluate(ifTrue, trueMask);
        Vector falseResult = falseMask.none() ? null : context.evaluate(ifFalse, falseMask);

        Vector template = trueResult != null ? trueResult : falseResult;
        if (template == null) {
            return output;
        }

        if (output == null) {
            output = allocate(template, context);
        }

        merge(output, trueResult, trueMask);
        merge(output, falseResult, falseMask);

        return output;
    }

    private static Vector allocate(Vector template, EvaluationContext context)
    {
        if (template instanceof I64Vector) {
            return context.allocator().allocate(CONTEXT, template.length(), I64Vector::new);
        }
        if (template instanceof BooleanVector) {
            return context.allocator().allocate(CONTEXT, template.length(), BooleanVector::new);
        }
        throw new UnsupportedOperationException("Unsupported vector type for IF: " + template.getClass().getSimpleName());
    }

    private static void merge(Vector output, Vector source, Mask mask)
    {
        if (source == null || mask.none()) {
            return;
        }
        if (output instanceof I64Vector out && source instanceof I64Vector src) {
            for (int position : mask) {
                out.values()[position] = src.values()[position];
            }
        }
        else if (output instanceof BooleanVector out && source instanceof BooleanVector src) {
            for (int position : mask) {
                out.values()[position] = src.values()[position];
            }
        }
        else {
            throw new UnsupportedOperationException(
                    "Unsupported vector types for IF merge: output=%s, source=%s".formatted(
                            output.getClass().getSimpleName(), source.getClass().getSimpleName()));
        }
    }
}
