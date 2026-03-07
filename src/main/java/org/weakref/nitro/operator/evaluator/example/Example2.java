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
 * Reuse computation: {@code a + b} evaluated additively over two overlapping masks.
 * <p>
 * Positions in the overlap are computed exactly once. The evaluator tracks which
 * positions have already been evaluated and automatically skips them on the second call.
 *
 * <pre>
 *   0: InputReference(0)    -- a values
 *   1: InputReference(1)    -- a nulls
 *   2: InputReference(2)    -- b values
 *   3: InputReference(3)    -- b nulls
 *   4: Or(1, 3)             -- nulls
 *   5: AddI64Exact(0, 2)    -- a + b
 * </pre>
 */
public class Example2
{
    void main()
    {
        int batchSize = 10;
        long[] aValues   = {0, 2, 3, Long.MAX_VALUE - 1, Long.MAX_VALUE, 6, 7, 8, 9, 0};
        long[] bValues   = {10, 0, 30, 1, 1, 60, 70, 80, 0, 100};
        boolean[] aNulls = {true, false, false, false, false, false, false, false, false, true};
        boolean[] bNulls = {false, true, false, false, false, false, false, false, true, false};

        Vector[] inputs = {
                new I64Vector(aValues),    // 0: a values
                new BooleanVector(aNulls), // 1: a nulls
                new I64Vector(bValues),    // 2: b values
                new BooleanVector(bNulls), // 3: b nulls
        };

        AddI64Exact addExact = new AddI64Exact(0, 2);
        List<Function> expressions = List.of(
                new InputReference(0),  // 0: a values
                new InputReference(1),  // 1: a nulls
                new InputReference(2),  // 2: b values
                new InputReference(3),  // 3: b nulls
                new Or(1, 3),           // 4: nulls
                addExact);              // 5: a + b

        Allocator allocator = new Allocator();
        Evaluator evaluator = new Evaluator(expressions, (i, mask) -> inputs[i], allocator);

        Mask inputMask1 = Mask.sparse(new int[] {1, 2, 3, 4, 5}, batchSize);
        Mask inputMask2 = Mask.sparse(new int[] {4, 5, 6, 7, 8}, batchSize);

        // Evaluate a + b for inputMask1, excluding nulls
        BooleanVector nulls = (BooleanVector) evaluator.evaluate(4, inputMask1).values();
        evaluator.evaluate(5, inputMask1.andNot(nulls));
        System.out.println("a+b after mask1: " + evaluator.evaluate(5, inputMask1.andNot(nulls)).values());

        // Extend to inputMask2; the evaluator computes only positions not yet evaluated
        nulls = (BooleanVector) evaluator.evaluate(4, inputMask2).values();
        evaluator.evaluate(5, inputMask2.andNot(nulls));
        System.out.println("a+b after mask2: " + evaluator.evaluate(5, inputMask1.or(inputMask2)).values());
        System.out.println("Errors:          " + evaluator.evaluate(5, inputMask1.or(inputMask2)).errors());
    }
}
