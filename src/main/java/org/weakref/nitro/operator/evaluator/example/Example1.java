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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.Evaluator;
import org.weakref.nitro.operator.evaluator.Function;
import org.weakref.nitro.operator.evaluator.functions.AddI64Exact;
import org.weakref.nitro.operator.evaluator.functions.DivideI64;
import org.weakref.nitro.operator.evaluator.functions.If;
import org.weakref.nitro.operator.evaluator.functions.InputReference;
import org.weakref.nitro.operator.evaluator.functions.Or;
import org.weakref.nitro.operator.evaluator.functions.SubtractI64Exact;

import java.util.List;

/**
 * Conditional expression: {@code IF(condition, a + b, a - b) / c}
 * <p>
 * Demonstrates null tracking and overflow/divide-by-zero detection.
 *
 * <pre>
 *    0: InputReference(0)      -- a values
 *    1: InputReference(1)      -- a nulls
 *    2: InputReference(2)      -- b values
 *    3: InputReference(3)      -- b nulls
 *    4: InputReference(4)      -- c values
 *    5: InputReference(5)      -- c nulls
 *    6: InputReference(6)      -- condition
 *    7: Or(1, 3)               -- ab_nulls
 *    8: AddI64Exact(0, 2)      -- a + b
 *    9: SubtractI64Exact(0, 2) -- a - b
 *   10: If(6, 8, 9)            -- IF(condition, a+b, a-b)
 *   11: Or(7, 5)               -- abc_nulls
 *   12: DivideI64(10, 4)       -- IF(condition, a+b, a-b) / c
 * </pre>
 */
public class Example1
{
    void main()
    {
        int batchSize = 10;
        long[] aValues = {0, 2, 0, Long.MIN_VALUE, Long.MAX_VALUE, 6, 7, 8, 9, 0};
        long[] bValues = {10, 0, 0, 1, 1, 60, 70, 80, 0, 100};
        long[] cValues = {1, 1, 1, 1, 1, 0, 2, 0, 1, 1};
        boolean[] aNulls = {true, false, true, false, false, false, false, false, false, true};
        boolean[] bNulls = {false, true, true, false, false, false, false, false, true, false};
        boolean[] cNulls = {false, false, false, false, false, false, false, true, false, false};
        boolean[] cond = {true, false, true, false, true, false, true, false, true, false};

        Vector[] inputs = {
                new I64Vector(aValues),     // 0: a values
                new BooleanVector(aNulls),  // 1: a nulls
                new I64Vector(bValues),     // 2: b values
                new BooleanVector(bNulls),  // 3: b nulls
                new I64Vector(cValues),     // 4: c values
                new BooleanVector(cNulls),  // 5: c nulls
                new BooleanVector(cond),    // 6: condition
        };

        AddI64Exact addExact = new AddI64Exact(0, 2);
        SubtractI64Exact subExact = new SubtractI64Exact(0, 2);

        List<Function> expressions = List.of(
                new InputReference(0),  // 0: a values
                new InputReference(1),  // 1: a nulls
                new InputReference(2),  // 2: b values
                new InputReference(3),  // 3: b nulls
                new InputReference(4),  // 4: c values
                new InputReference(5),  // 5: c nulls
                new InputReference(6),  // 6: condition
                new Or(1, 3),           // 7: ab_nulls
                addExact,               // 8: a + b
                subExact,               // 9: a - b
                new If(6, 8, 9),        // 10: IF(condition, a+b, a-b)
                new Or(7, 5),           // 11: abc_nulls
                new DivideI64(10, 4));  // 12: IF(condition, a+b, a-b) / c

        Allocator allocator = new Allocator();
        Evaluator evaluator = new Evaluator(expressions, (i, mask) -> inputs[i], allocator);

        Mask inputMask = Mask.sparse(new int[] {0, 1, 2, 3, 4, 5, 6, 7}, batchSize);

        // Step 1: compute ab nulls; only evaluate IF for non-null positions
        BooleanVector abNulls = (BooleanVector) evaluator.evaluate(7, inputMask).values();
        Mask nonNullMask = inputMask.andNot(abNulls);

        // Step 2: evaluate IF(condition, a+b, a-b) for non-null positions
        evaluator.evaluate(10, nonNullMask);

        // Step 3: exclude positions with arithmetic overflow
        BooleanVector condVec = (BooleanVector) evaluator.evaluate(6, nonNullMask).values();
        Mask trueMask = nonNullMask.and(condVec);
        Mask falseMask = nonNullMask.andNot(condVec);
        BooleanVector addErrors = evaluator.evaluate(8, trueMask).errors();
        BooleanVector subErrors = evaluator.evaluate(9, falseMask).errors();
        Mask noOverflow = union(
                addErrors != null ? trueMask.andNot(addErrors) : trueMask,
                subErrors != null ? falseMask.andNot(subErrors) : falseMask);

        // Step 4: exclude c nulls
        BooleanVector abcNulls = (BooleanVector) evaluator.evaluate(11, noOverflow).values();
        Mask candidates = noOverflow.andNot(abcNulls);

        // Step 5: divide
        evaluator.evaluate(12, candidates);

        System.out.println("Input mask:     " + inputMask);
        System.out.println("ab nulls:       " + abNulls);
        System.out.println("abc nulls:      " + abcNulls);
        System.out.println("Add overflow:   " + addErrors);
        System.out.println("Sub overflow:   " + subErrors);
        System.out.println("Div by zero:    " + evaluator.evaluate(12, candidates).errors());
        System.out.println("Result:         " + evaluator.evaluate(12, candidates).values());
    }

    private static Mask union(Mask a, Mask b)
    {
        if (a.none()) {
            return b;
        }
        if (b.none()) {
            return a;
        }
        return a.or(b);
    }
}
