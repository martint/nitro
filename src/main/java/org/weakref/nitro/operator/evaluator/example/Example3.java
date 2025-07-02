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
 * Chained computation: a + b + c \ M
 */
public class Example3
{
    private static final AddI64Exact ADD_I64_EXACT = new AddI64Exact();
    private static final Or OR = new Or();

    void main()
    {
        Mask inputMask = Mask.sparse(new int[] {2, 3, 4, 5, 6, 7, 8}, 10);
        Long[] a = {null, 2L, 3L, Long.MAX_VALUE - 1, Long.MAX_VALUE, 6L, 7L, 8L, 9L, null};
        Long[] b = {10L, null, 30L, 1L, 1L, 60L, 70L, 80L, null, 100L};
        Long[] c = {100L, 200L, null, 1L, 1L, 600L, 700L, 800L, null, 1000L};

        I64Vector aValues = Vectors.i64Vector(a);
        BooleanVector aNulls = Vectors.nulls(a);
        I64Vector bValues = Vectors.i64Vector(b);
        BooleanVector bNulls = Vectors.nulls(b);
        I64Vector cValues = Vectors.i64Vector(c);
        BooleanVector cNulls = Vectors.nulls(c);

        // $0_nulls = OR(a.nulls, b.nulls) \ IM
        // $m0 = IM & ~$0_nulls
        // ($0_values, $0_errors) = ADD_I64_EXACT(a_values, b_values) \ $m0
        BooleanVector nulls = (BooleanVector) OR.apply(aNulls, bNulls, inputMask, null);
        Mask m0 = inputMask.andNot(nulls);
        Result addResult = ADD_I64_EXACT.apply(aValues, bValues, m0, null);

        // $m1 = $m0 ~$0_errors
        // $r_nulls = OR($0_nulls, c.nulls) \ $m1
        // ($r_values, $1_errors) = ADD_I64_EXACT($0_values, c_values) \ $m1

        // TODO: how to represent merging of errors?
        //   $r_errors = merge($m1, $1_errors, $0_errors) \ IM
        Mask remaining = m0.andNot(addResult.errors());
        BooleanVector nulls2 = (BooleanVector) OR.apply(nulls, cNulls, remaining, nulls);
        Result addResult2 = ADD_I64_EXACT.apply(addResult.result(), cValues, remaining, addResult);

        System.out.println("Input mask: " + inputMask);
        System.out.println("Remaining mask" + remaining);

        System.out.println();
        System.out.println("Nulls:  " + nulls2);
        System.out.println("Values: " + addResult2.result());
        System.out.println("Errors: " + addResult2.errors());
        System.out.println();
        System.out.println("Result: " + render(inputMask, addResult2.result(), nulls, addResult2.errors()));
    }
}
