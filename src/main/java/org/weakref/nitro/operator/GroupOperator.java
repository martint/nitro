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

import java.util.HashMap;
import java.util.Map;

public class GroupOperator
        implements Operator
{
    private final int groupByColumn;
    private final Operator source;

    private final Map<Long, Long> groups = new HashMap<>(); // TODO: more efficient map
    private boolean filled;
    private Mask mask;
    private LongVector result;

    public GroupOperator(int groupByColumn, Operator source)
    {
        this.groupByColumn = groupByColumn;
        this.source = source;
    }

    @Override
    public int columnCount()
    {
        return source.columnCount() + 1;
    }

    @Override
    public Mask next()
    {
        filled = false;
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
    }

    @Override
    public Vector column(int column)
    {
        if (column == 0) {
            doGroupingIfNeeded();
            return result;
        }
        return source.column(column - 1);
    }

    private void doGroupingIfNeeded()
    {
        if (!filled && !mask.none()) {
            ensureCapacity(mask.count());

            LongVector column = (LongVector) source.column(groupByColumn);

            for (int position : mask) {
                if (column.nulls()[position]) {
                    result.nulls()[position] = true;
                }
                else {
                    long value = column.values()[position];
                    long group = groups.size();

                    Long existing = groups.putIfAbsent(value, group);
                    if (existing != null) {
                        group = existing;
                    }

                    result.values()[position] = group;
                    result.nulls()[position] = false;
                }
            }
        }
    }

    private void ensureCapacity(int count)
    {
        if (result == null || result.values().length < count) {
            result = new LongVector(new boolean[count], new long[count]);
        }
    }
}
