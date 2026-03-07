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
import org.weakref.nitro.operator.evaluator.functions.AddI64;
import org.weakref.nitro.operator.evaluator.functions.If;
import org.weakref.nitro.operator.evaluator.functions.InputReference;
import org.weakref.nitro.operator.evaluator.functions.SubtractI64;

import java.util.List;

/**
 * Demonstrates the expression evaluator API:
 * <ol>
 *   <li>Basic computation: {@code a + b}</li>
 *   <li>CSE: shared sub-expression evaluated only once</li>
 *   <li>Additive evaluation: second mask only computes new positions</li>
 *   <li>Conditional: {@code IF(condition, a + b, a - b)}</li>
 * </ol>
 *
 * Expression index layout used throughout:
 * <pre>
 *   0: InputReference(0)  -- input column a
 *   1: InputReference(1)  -- input column b
 *   2: InputReference(2)  -- input column condition (boolean)
 *   3: AddI64(0, 1)       -- a + b
 *   4: SubtractI64(0, 1)  -- a - b
 *   5: If(2, 3, 4)        -- IF(condition, a + b, a - b)
 * </pre>
 */
public class Example4
{
    void main()
    {
        long[] aValues = {1, 2, 3, 4, 5, 6, 7, 8};
        long[] bValues = {10, 20, 30, 40, 50, 60, 70, 80};
        boolean[] condition = {true, false, true, false, true, false, true, false};

        Vector a = new I64Vector(aValues);
        Vector b = new I64Vector(bValues);
        Vector cond = new BooleanVector(condition);

        Vector[] inputs = {a, b, cond};

        List<Function> expressions = List.of(
                new InputReference(0),    // 0: a
                new InputReference(1),    // 1: b
                new InputReference(2),    // 2: condition
                new AddI64(0, 1),         // 3: a + b
                new SubtractI64(0, 1),    // 4: a - b
                new If(2, 3, 4));         // 5: IF(condition, a+b, a-b)

        Allocator allocator = new Allocator();
        Evaluator evaluator = new Evaluator(expressions, (index, mask) -> inputs[index], allocator);

        Mask all = Mask.all(8);
        Mask evens = Mask.sparse(new int[] {0, 2, 4, 6}, 8);
        Mask odds = Mask.sparse(new int[] {1, 3, 5, 7}, 8);

        // --- Demo 1: basic computation ---
        System.out.println("=== Demo 1: a + b over all positions ===");
        Vector result = evaluator.evaluate(3, all).values();
        System.out.println("a + b: " + result);
        evaluator.reset();

        // --- Demo 2: CSE (shared sub-expression used by two callers) ---
        System.out.println("\n=== Demo 2: CSE — a+b used in two places, evaluated once ===");
        // Evaluate IF result (uses a+b and a-b internally)
        evaluator.evaluate(5, all);
        // Requesting a+b separately should be a no-op (already computed)
        evaluator.evaluate(3, all);
        System.out.println("IF result: " + evaluator.evaluate(5, all).values());
        evaluator.reset();

        // --- Demo 3: additive evaluation ---
        System.out.println("\n=== Demo 3: additive — evens then odds ===");
        Vector addResult = evaluator.evaluate(3, evens).values();
        System.out.println("a+b after evens: " + addResult);
        addResult = evaluator.evaluate(3, odds).values();
        System.out.println("a+b after odds:  " + addResult);
        evaluator.reset();

        // --- Demo 4: conditional IF ---
        System.out.println("\n=== Demo 4: IF(condition, a+b, a-b) ===");
        I64Vector ab = new I64Vector(aValues.length);
        I64Vector aMb = new I64Vector(aValues.length);
        for (int i = 0; i < aValues.length; i++) {
            ab.values()[i] = aValues[i] + bValues[i];
            aMb.values()[i] = aValues[i] - bValues[i];
        }
        System.out.println("condition: " + cond);
        System.out.println("a+b:       " + ab);
        System.out.println("a-b:       " + aMb);
        Vector ifResult = evaluator.evaluate(5, all).values();
        System.out.println("IF result: " + ifResult);
        evaluator.reset();
    }
}
