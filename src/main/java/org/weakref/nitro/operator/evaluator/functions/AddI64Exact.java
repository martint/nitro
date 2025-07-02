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

import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.Result;

import static com.google.common.base.Preconditions.checkArgument;

public class AddI64Exact
{
    public Result apply(Vector left, Vector right, Mask mask, Result result)
    {
        checkArgument(left.length() == right.length(), "Vectors must have the same length");
        return applyFlatFlat(left, right, mask, result);
    }

    private Result applyFlatFlat(Vector left, Vector right, Mask mask, Result result)
    {
        I64Vector leftFlat = (I64Vector) left;
        I64Vector rightFlat = (I64Vector) right;

        I64Vector output;
        BooleanVector errors;
        
        if (result == null) {
            // TODO: allocate from pool
            output = new I64Vector(left.length());
            errors = new BooleanVector(left.length());
        }
        else {
            output = (I64Vector) result.result();
            errors = result.errors();
        }

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
        
        return new Result(output, errors);
    }

    private static void apply(I64Vector left, I64Vector right, I64Vector output, BooleanVector errors, int position)
    {
        long leftValue = left.values()[position];
        long rightValue = right.values()[position];

        long result = leftValue + rightValue;
        output.values()[position] = result;
        // HD 2-12 Overflow iff both arguments have the opposite sign of the result
        errors.values()[position] = ((leftValue ^ result) & (rightValue ^ result)) < 0;
    }
}
