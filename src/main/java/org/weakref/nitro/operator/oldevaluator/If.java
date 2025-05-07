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

import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;

public class If
        implements Operation
{
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
    public void apply(int output, Mask mask, Vector[] buffers, EvaluationContext context)
    {
        BooleanVector condition = (BooleanVector) context.evaluate(this.condition, this.condition, mask);

        // TODO: how to combine both vectors into output? This assumes ifTrue and ifFalse both write to the same
        // output and that it corresponds to the output of the if operation.
        context.evaluate(this.ifTrue, output, mask.and(condition));
        context.evaluate(this.ifFalse, output, mask.andNot(condition));

        // TODO: how to decide what vector type to use for output?
//        Vector out = context.allocator().allocateOrGrow(null, buffers[output], mask.maxPosition() + 1, I64Vector::new);
//        buffers[output] = out;
    }
}
