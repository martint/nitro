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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;

import java.util.List;

public class Evaluator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("Evaluator");
    private final Input input;
    private final Allocator allocator;

    private final List<Operation> operations;
    private final Vector[] buffers;
    private final Mask[] masks;
    private final EvaluationContext context;

    public Evaluator(List<Operation> operations, Input input, Allocator allocator)
    {
        this.input = input;
        this.allocator = allocator;
        this.operations = operations;

        this.buffers = new Vector[operations.size()];
        this.masks = new Mask[operations.size()];

        context = new EvaluationContext()
        {
            @Override
            public Vector evaluate(int operation, int output, Mask mask)
            {
                return Evaluator.this.evaluate(operation, output, mask);
            }

            @Override
            public Vector input(int operation, Mask mask)
            {
                return input.get(operation, mask);
            }

            @Override
            public Allocator allocator()
            {
                return allocator;
            }
        };
    }

    private Vector evaluate(int index, int output, Mask mask)
    {
        if (index < 0 || index >= operations.size()) {
            throw new IllegalArgumentException("Invalid expression ordinal: " + index);
        }

        evaluateRecursive(index, mask);
        return buffers[output];
    }

    private void evaluateRecursive(int index, Mask mask)
    {
        if (mask.none()) {
            return;
        }

        // Check if we already evaluated this expression for this mask
        Mask alreadyEvaluated = masks[index];
        if (alreadyEvaluated != null && alreadyEvaluated.containsAll(mask)) {
            return;
        }

        Mask positionsToEvaluate;
        if (alreadyEvaluated == null) {
            positionsToEvaluate = mask;
        }
        else {
            positionsToEvaluate = mask.difference(alreadyEvaluated);

            if (positionsToEvaluate.none()) {
                return;
            }
        }

        Operation operation = operations.get(index);
        operation.apply(index, positionsToEvaluate, buffers, context);

        if (alreadyEvaluated == null) {
            masks[index] = positionsToEvaluate;
        }
        else {
            masks[index] = alreadyEvaluated.union(positionsToEvaluate);
        }
    }

    public void close()
    {
        allocator.release(ALLOCATION_CONTEXT);
    }

//    public static void main()
//    {
//        Allocator allocator = new Allocator();
//
//        List<Vector> inputs = List.of(
//                new I64VectorWithNulls(new boolean[] {false, false, false, false}, new long[] {1, 2, 3, 4}),
//                new I64VectorWithNulls(new boolean[] {false, false, false, false}, new long[] {10, 20, 30, 40}),
//                new BooleanVector(new boolean[] {true, true, true, true}));
//
//        List<Operation> operations = List.of(
//                new InputReference(0), // 0
//                new InputReference(1), // 1
//                new InputReference(2), // 2
//                new AddI64(0, 1), // 3
//                new If(2, 3, 0));  // 4
//
//        Evaluator evaluator = new Evaluator(operations, (index, mask) -> inputs.get(index), allocator);
//
//        Vector evens = evaluator.evaluate(4, 4, Mask.sparse(new int[] {0, 2}, 2));
//        Vector odds = evaluator.evaluate(4, 4, Mask.sparse(new int[] {1, 3}, 2));
//        Vector all = evaluator.evaluate(4, 4, Mask.all(4));
//
//        System.out.println(evens);
//        System.out.println(odds);
//        System.out.println(all);
//    }
}
