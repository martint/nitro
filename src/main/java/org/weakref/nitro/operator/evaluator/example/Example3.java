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
import org.weakref.nitro.operator.evaluator.functions.InputReference;
import org.weakref.nitro.operator.evaluator.functions.Or;

import java.util.List;

/**
 * Chained computation: {@code (a + b) + c} with null tracking and overflow detection.
 *
 * <pre>
 *   0: InputReference(0)    -- a values
 *   1: InputReference(1)    -- a nulls
 *   2: InputReference(2)    -- b values
 *   3: InputReference(3)    -- b nulls
 *   4: InputReference(4)    -- c values
 *   5: InputReference(5)    -- c nulls
 *   6: Or(1, 3)             -- ab_nulls
 *   7: AddI64Exact(0, 2)    -- a + b
 *   8: Or(6, 5)             -- abc_nulls
 *   9: AddI64Exact(7, 4)    -- (a + b) + c
 * </pre>
 */
public class Example3
{
    void main()
    {
        int batchSize = 10;
        long[] aValues   = {0, 2, 3, Long.MAX_VALUE - 1, Long.MAX_VALUE, 6, 7, 8, 9,    0};
        long[] bValues   = {10, 0, 30, 1, 1, 60, 70, 80, 0, 100};
        long[] cValues   = {100, 200, 0, 1, 1, 600, 700, 800, 0, 1000};
        boolean[] aNulls = {true, false, false, false, false, false, false, false, false, true};
        boolean[] bNulls = {false, true, false, false, false, false, false, false, false, false};
        boolean[] cNulls = {false, false, true, false, false, false, false, false, true, false};

        Vector[] inputs = {
                new I64Vector(aValues),    // 0: a values
                new BooleanVector(aNulls), // 1: a nulls
                new I64Vector(bValues),    // 2: b values
                new BooleanVector(bNulls), // 3: b nulls
                new I64Vector(cValues),    // 4: c values
                new BooleanVector(cNulls), // 5: c nulls
        };

        AddI64Exact addAB = new AddI64Exact(0, 2);
        AddI64Exact addABC = new AddI64Exact(7, 4);
        List<Function> expressions = List.of(
                new InputReference(0),  // 0: a values
                new InputReference(1),  // 1: a nulls
                new InputReference(2),  // 2: b values
                new InputReference(3),  // 3: b nulls
                new InputReference(4),  // 4: c values
                new InputReference(5),  // 5: c nulls
                new Or(1, 3),           // 6: ab_nulls
                addAB,                  // 7: a + b
                new Or(6, 5),           // 8: abc_nulls
                addABC);                // 9: (a + b) + c

        Allocator allocator = new Allocator();
        Evaluator evaluator = new Evaluator(expressions, (i, mask) -> inputs[i], allocator);

        Mask inputMask = Mask.sparse(new int[] {2, 3, 4, 5, 6, 7, 8}, batchSize);

        // Step 1: compute combined nulls; only evaluate where all three inputs are non-null
        BooleanVector abcNulls = (BooleanVector) evaluator.evaluate(8, inputMask);
        Mask m0 = inputMask.andNot(abcNulls);

        // Step 2: evaluate a + b; exclude positions with overflow
        evaluator.evaluate(7, m0);
        Mask m1 = addAB.errors() != null ? m0.andNot(addAB.errors()) : m0;

        // Step 3: evaluate (a + b) + c for positions with no a+b overflow
        evaluator.evaluate(9, m1);

        System.out.println("Input mask:       " + inputMask);
        System.out.println("abc nulls:        " + abcNulls);
        System.out.println("a+b:              " + evaluator.evaluate(7, m0));
        System.out.println("a+b overflow:     " + addAB.errors());
        System.out.println("(a+b)+c:          " + evaluator.evaluate(9, m1));
        System.out.println("(a+b)+c overflow: " + addABC.errors());
    }
}
