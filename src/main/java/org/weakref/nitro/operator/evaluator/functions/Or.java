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
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;

import static com.google.common.base.Preconditions.checkArgument;

public class Or
{
    public Vector apply(Vector left, Vector right, Mask mask, Vector result)
    {
        checkArgument(left.length() == right.length(), "Vectors must have the same length");

        return applyFlatFlat(left, right, mask, result);
    }

    private Vector applyFlatFlat(Vector left, Vector right, Mask mask, Vector result)
    {
        BooleanVector leftFlat = (BooleanVector) left;
        BooleanVector rightFlat = (BooleanVector) right;

        BooleanVector output = (BooleanVector) result;
        if (output == null) {
            // TODO: allocate from pool
            output = new BooleanVector(left.length());
        }

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
