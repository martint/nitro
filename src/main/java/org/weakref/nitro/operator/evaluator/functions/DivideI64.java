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
import org.weakref.nitro.operator.evaluator.Result;

public class DivideI64
        implements Function
{
    private static final Allocator.Context CONTEXT = new Allocator.Context("DivideI64");
    private static final Allocator.Context ERRORS_CONTEXT = new Allocator.Context("DivideI64.errors");

    private final int left;
    private final int right;

    public DivideI64(int left, int right)
    {
        this.left = left;
        this.right = right;
    }

    @Override
    public Result apply(Result output, Mask mask, EvaluationContext context)
    {
        Vector leftVec = context.evaluate(left, mask).values();
        Vector rightVec = context.evaluate(right, mask).values();
        I64Vector outValues = (I64Vector) context.allocator().allocateOrGrow(CONTEXT, output != null ? output.values() : null, leftVec.length(), I64Vector::new);
        BooleanVector outErrors = (BooleanVector) context.allocator().allocateOrGrow(ERRORS_CONTEXT, output != null ? output.errors() : null, leftVec.length(), BooleanVector::new);
        return applyFlatFlat(leftVec, rightVec, mask, outValues, outErrors);
    }

    private Result applyFlatFlat(Vector left, Vector right, Mask mask, I64Vector output, BooleanVector errors)
    {
        I64Vector leftFlat = (I64Vector) left;
        I64Vector rightFlat = (I64Vector) right;

        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position < max; position++) {
                apply(leftFlat, rightFlat, output, errors, position);
            }
        }
        else {
            for (int position : mask) {
                apply(leftFlat, rightFlat, output, errors, position);
            }
        }

        return new Result(output, null, errors);
    }

    private static void apply(I64Vector left, I64Vector right, I64Vector output, BooleanVector errors, int position)
    {
        long leftValue = left.values()[position];
        long rightValue = right.values()[position];

        output.values()[position] = rightValue != 0 ? leftValue / rightValue : 0;
        errors.values()[position] = rightValue == 0;
    }
}
