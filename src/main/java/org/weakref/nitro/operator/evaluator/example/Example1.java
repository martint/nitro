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
import org.weakref.nitro.operator.evaluator.functions.DivideI64;
import org.weakref.nitro.operator.evaluator.functions.Or;
import org.weakref.nitro.operator.evaluator.Result;
import org.weakref.nitro.operator.evaluator.functions.SubtractI64Exact;

import static org.weakref.nitro.operator.evaluator.example.Vectors.render;

/**
 * Conditional expression
 */
public class Example1
{
    private static final AddI64Exact ADD_I64_EXACT = new AddI64Exact();
    private static final SubtractI64Exact SUBTRACT_I64_EXACT = new SubtractI64Exact();
    private static final DivideI64 DIVIDE_I64 = new DivideI64();
    private static final Or OR = new Or();

    void main()
    {
        Mask inputMask = Mask.sparse(new int[] {0, 1, 2, 3, 4, 5, 6, 7}, 10);
        Boolean[] condition = {true, false, true, false, true, false, true, false, true, false};
        Long[] a = {null, 2L, null, Long.MIN_VALUE, Long.MAX_VALUE, 6L, 7L, 8L, 9L, null};
        Long[] b = {10L, null, null, 1L, 1L, 60L, 70L, 80L, null, 100L};
        Long[] c = {1L, 1L, 1L, 1L, 1L, 0L, 2L, null, 1L, 1L};

        BooleanVector conditionVector = Vectors.booleanVector(condition);
        I64Vector aValues = Vectors.i64Vector(a);
        BooleanVector aNulls = Vectors.nulls(a);
        I64Vector bValues = Vectors.i64Vector(b);
        BooleanVector bNulls = Vectors.nulls(b);
        I64Vector cValues = Vectors.i64Vector(c);
        BooleanVector cNulls = Vectors.nulls(c);

        // if(condition, a + b, a - b) / c \ IM
        // TODO: nulls & errors
        //   $0 = inputMask ^ condition
        //   $1 = a + b \ $0
        //   $2 = inputMask ^ ~condition
        //   $3 = a - b \ $2
        //   $4 = merge(condition, $1, $3) \ inputMask
        //   $5 = $4 / c \ inputMask

        // if (condition) then a + b
        Mask conditionMask = inputMask.and(conditionVector);
        BooleanVector nulls = (BooleanVector) OR.apply(aNulls, bNulls, conditionMask, null);
        Result addResult = ADD_I64_EXACT.apply(aValues, bValues, conditionMask.andNot(nulls), null);

        // else a - b
        Mask elseMask = inputMask.andNot(conditionVector);
        nulls = (BooleanVector) OR.apply(aNulls, bNulls, elseMask, nulls);
        Result subtractResult = SUBTRACT_I64_EXACT.apply(aValues, bValues, elseMask.andNot(nulls), addResult);

        // divide by c
        Mask candidates = inputMask.andNot(subtractResult.errors()).andNot(nulls);
        nulls = (BooleanVector) OR.apply(nulls, cNulls, candidates, nulls);
        Result divideResult = DIVIDE_I64.apply(aValues, cValues, candidates.andNot(nulls), subtractResult);

        System.out.println("Input mask: " + inputMask);
        System.out.println("Then mask: " + conditionMask);
        System.out.println("Else mask: " + elseMask);
        System.out.println("Nulls:   " + nulls);
        System.out.println("Values: " + divideResult.result());
        System.out.println("Error: " + divideResult.errors());
        System.out.println();
        System.out.println("Result: " + render(inputMask, divideResult.result(), nulls, divideResult.errors()));
    }
}
