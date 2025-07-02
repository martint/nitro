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
package org.weakref.nitro.operator.oldevaluator;

import org.weakref.nitro.data.I64VectorWithNulls;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;

public class AddI64
        implements Operation
{
    private final int input1;
    private final int input2;

    public AddI64(int input1, int input2)
    {
        this.input1 = input1;
        this.input2 = input2;
    }

    @Override
    public void apply(int output, Mask mask, Vector[] buffers, EvaluationContext context)
    {
        I64VectorWithNulls in1 = (I64VectorWithNulls) context.evaluate(input1, input1, mask);
        I64VectorWithNulls in2 = (I64VectorWithNulls) context.evaluate(input2, input1, mask);
        I64VectorWithNulls out = (I64VectorWithNulls) context.allocator().allocateOrGrow(null, buffers[output], mask.maxPosition() + 1, I64VectorWithNulls::new);
        buffers[output] = out;
        
        for (int i = 0; i < mask.count(); i++) {
            int position = mask.position(i);
            out.values()[position] = in1.values()[position] + in2.values()[position];
            out.nulls()[position] = in1.nulls()[position] || in2.nulls()[position];
        }
    }
}
