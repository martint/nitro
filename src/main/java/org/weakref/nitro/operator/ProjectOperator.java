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
package org.weakref.nitro.operator;

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAllocator;
import org.weakref.nitro.function.Function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ProjectOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("ProjectOperator");
    private final Allocator allocator;

    private final Execution execution;

    private final Operator source;
    private Mask mask;

    private final List<Vector[]> inputs;
    private final Vector[] buffers;
    private final boolean[] filled;

    public ProjectOperator(Allocator allocator, Execution execution, Operator source)
    {
        this.allocator = allocator;
        this.source = source;
        this.execution = execution;

        inputs = new ArrayList<>();
        buffers = new Vector[execution.operations().size()];
        filled = new boolean[execution.operations().size()];

        for (int i = 0; i < execution.operations.size(); i++) {
            inputs.add(new Vector[execution.operations.get(i).inputs().size()]);
        }
    }

    @Override
    public int columnCount()
    {
        return execution.outputs().size();
    }

    @Override
    public Mask next()
    {
        Arrays.fill(filled, false);
        mask = source.next();
        return mask;
    }

    @Override
    public boolean hasNext()
    {
        return source.hasNext();
    }

    @Override
    public void constrain(Mask mask)
    {
        source.constrain(mask);
        this.mask = mask;
    }

    @Override
    public Vector column(int column)
    {
        int operation = execution.outputs().get(column);
        if (operation < 0) {
            return source.column(-(operation + 1));
        }

        evaluateRecursive(operation);

        return buffers[operation];
    }

    private void evaluateRecursive(int operation)
    {
        if (filled[operation]) {
            return;
        }

        filled[operation] = true;

        Invocation invocation = execution.operations.get(operation);
        Vector[] arguments = this.inputs.get(operation);
        for (int i = 0; i < invocation.inputs().size(); i++) {
            int input = invocation.inputs().get(i);
            if (input < 0) {
                arguments[i] = source.column(-(input + 1));
            }
            else {
                evaluateRecursive(input);
                arguments[i] = buffers[input];
            }
        }

        buffers[operation] = allocator.reallocateIfNecessary(ALLOCATION_CONTEXT, buffers[operation], mask.maxPosition() + 1, invocation.allocator()::allocate);
        invocation.operation.apply(buffers[operation], arguments, mask);
    }

    @Override
    public void close()
    {
        source.close();
        allocator.release(ALLOCATION_CONTEXT);
    }

    /**
     * The evaluation plan contains a flattened representation of the expression graph.
     * Each invocation represents an operation to be executed, along with a descriptor of its inputs,
     * which consists of indexes with the following meaning:
     * <ul>
     *  <li>if >= 0, the index of the corresponding operation that produces the input to this operation
     *  <li>if < 0, an index into the columns of the source operator, which can be calculated as -(index + 1)
     * </ul>
     *
     * The outputs is a list of indexes that indicate how the outputs of the projection are computed, with
     * the same meaning as above.
     */
    public record Execution(List<Invocation> operations, List<Integer> outputs) {}

    public record Invocation(Function operation, List<Integer> inputs, VectorAllocator allocator) {}
}
