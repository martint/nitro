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

import org.weakref.nitro.data.LongVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;

import java.util.Arrays;
import java.util.List;

public class ProjectOperator
        implements Operator
{
    private final Operator source;
    private final List<Integer> inputs;
    private final List<LongToLongFunction> projections;
    private final Vector[] results;

    private final boolean[] filled;
    private Mask mask;

    public ProjectOperator(List<Integer> inputs, List<LongToLongFunction> projections, Operator source)
    {
        this.source = source;
        this.inputs = inputs;
        this.projections = projections;
        results = new Vector[projections.size()];
        filled = new boolean[projections.size()];
    }

    @Override
    public int columnCount()
    {
        return projections.size();
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
        if (filled[column] || mask.none()) {
            return results[column];
        }

        filled[column] = true;

        // TODO: ensure current buffers have the right size for the batch
        ensureCapacity(column, mask.count());

        LongVector input = (LongVector) source.column(inputs.get(column));
        LongVector output = (LongVector) results[column];

        LongToLongFunction projection = projections.get(column);
        for (int position : mask) {
            applyProjection(input, position, projection, output);
        }

        return results[column];
    }

    private void ensureCapacity(int column, int count)
    {
        LongVector block = (LongVector) results[column];

        if (block == null || block.values().length < count) {
            results[column] = new LongVector(new boolean[count], new long[count]);
        }
    }

    private static void applyProjection(LongVector input, int position, LongToLongFunction projection, LongVector output)
    {
        boolean isNull = input.nulls()[position];
        output.nulls()[position] = isNull;
        output.values()[position] = isNull ? 0 : projection.apply(input.values()[position]);
    }

    public interface LongToLongFunction
    {
        long apply(long value);
    }
}
