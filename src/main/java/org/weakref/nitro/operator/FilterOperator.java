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

import java.util.function.LongPredicate;

public class FilterOperator
        implements Operator
{
    private final Operator source;
    private final int filterColumn;
    private final LongPredicate filter;

    private Mask mask;

    // reusable mask buffer
    private int[] maskPositions;

    public FilterOperator(int filterColumn, LongPredicate filter, Operator source)
    {
        this.source = source;
        this.filterColumn = filterColumn;
        this.filter = filter;
    }

    @Override
    public int columnCount()
    {
        return source.columnCount();
    }

    @Override
    public Mask next()
    {
        mask = source.next();

        doFilter(); // TODO: lazy? -- but for that, we need to be able to return the mask separately from the call to nextBatch

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
        this.mask = mask; // TODO: combine masks?
        source.constrain(mask);
    }

    @Override
    public Vector column(int column)
    {
        return source.column(column);
    }

    private void doFilter()
    {
        LongVector filterColumn = (LongVector) source.column(this.filterColumn);
        long[] values = filterColumn.values();
        boolean[] nulls = filterColumn.nulls();

        ensureCapacity(mask.count());
        int maskSize = 0;
        for (int position : mask) {
            if (!nulls[position] && filter.test(values[position])) {
                maskPositions[maskSize] = position;
                maskSize++;
            }
        }

        mask = Mask.sparse(maskPositions, maskSize);
        source.constrain(mask);
    }

    @Deprecated // TODO: move to Mask
    private void ensureCapacity(int size)
    {
        if (maskPositions == null || maskPositions.length < size) {
            maskPositions = new int[size];
        }
    }
}
