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
package org.weakref.nitro.operator.evaluator.example;

import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.operator.evaluator.functions.AddI64Exact;
import org.weakref.nitro.operator.evaluator.functions.Or;
import org.weakref.nitro.operator.evaluator.Result;

import static org.weakref.nitro.operator.evaluator.example.Vectors.render;

/**
 * Reuse computation
 */
public class Example2
{
    private static final AddI64Exact ADD_I64_EXACT = new AddI64Exact();
    private static final Or OR = new Or();

    void main()
    {
        Mask inputMask1 = Mask.sparse(new int[] {1, 2, 3, 4, 5}, 10);
        Mask inputMask2 = Mask.sparse(new int[] {4, 5, 6, 7, 8}, 10);
        Long[] a = {null, 2L, null, 3L, Long.MAX_VALUE, 6L, 7L, 8L, 9L, null};
        Long[] b = {10L, null, null, 1L, 1L, 60L, 70L, 80L, null, 100L};

        I64Vector aValues = Vectors.i64Vector(a);
        BooleanVector aNulls = Vectors.nulls(a);
        I64Vector bValues = Vectors.i64Vector(b);
        BooleanVector bNulls = Vectors.nulls(b);

        // a + b \ IM1
        BooleanVector nulls1 = (BooleanVector) OR.apply(aNulls, bNulls, inputMask1, null);
        Result addResult1 = ADD_I64_EXACT.apply(aValues, bValues, inputMask1.andNot(nulls1), null);

        // a + b \ IM2
        Mask remaining = inputMask2.andNot(inputMask1);
        BooleanVector nulls2 = (BooleanVector) OR.apply(aNulls, bNulls, remaining, nulls1);
        Result addResult2 = ADD_I64_EXACT.apply(aValues, bValues, remaining.andNot(nulls2), addResult1);

        Mask resultMask = inputMask1.or(inputMask2);

        System.out.println("Input mask 1: " + inputMask1);
        System.out.println("Input mask 2: " + inputMask2);
        System.out.println("Remaining mask" + remaining);

        System.out.println();
        System.out.println("Result mask: " + resultMask);
        System.out.println("Nulls:       " + nulls2);
        System.out.println("Values:      " + addResult2.result());
        System.out.println("Errors:      " + addResult2.errors());
        System.out.println();
        System.out.println("Result: " + render(resultMask, addResult2.result(), nulls2, addResult2.errors()));
    }
}
