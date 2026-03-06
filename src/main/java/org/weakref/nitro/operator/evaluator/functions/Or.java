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
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.EvaluationContext;
import org.weakref.nitro.operator.evaluator.Function;


public class Or
        implements Function
{
    private static final Allocator.Context CONTEXT = new Allocator.Context("Or");

    private final int left;
    private final int right;

    public Or(int left, int right)
    {
        this.left = left;
        this.right = right;
    }

    @Override
    public Vector apply(Vector output, Mask mask, EvaluationContext context)
    {
        Vector leftVec = context.evaluate(left, mask);
        Vector rightVec = context.evaluate(right, mask);
        output = context.allocator().allocateOrGrow(CONTEXT, output, leftVec.length(), BooleanVector::new);
        return applyFlatFlat(leftVec, rightVec, mask, output);
    }

    private Vector applyFlatFlat(Vector left, Vector right, Mask mask, Vector result)
    {
        BooleanVector leftFlat = (BooleanVector) left;
        BooleanVector rightFlat = (BooleanVector) right;

        BooleanVector output = (BooleanVector) result;

        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                output.values()[position] = leftFlat.values()[position] | rightFlat.values()[position];
            }
        }
        else {
            for (int position : mask) {
                output.values()[position] = leftFlat.values()[position] | rightFlat.values()[position];
            }
        }
        return output;
    }
}
