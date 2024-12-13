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
import org.weakref.nitro.operator.aggregation.Accumulator;

import java.util.Arrays;
import java.util.List;

import static java.lang.Math.toIntExact;

public class GroupedAggregationOperator
        implements Operator
{
    private final int groupColumn;
    private final List<Accumulator> aggregations;
    private final Operator source;
    private final Vector[] result;
    private boolean done;

    public GroupedAggregationOperator(int groupColumn, List<Accumulator> aggregations, Operator source)
    {
        this.groupColumn = groupColumn;
        this.aggregations = aggregations;
        this.source = source;

        result = new LongVector[aggregations.size()];
    }

    @Override
    public int columnCount()
    {
        return aggregations.size();
    }

    @Override
    public boolean hasNext()
    {
        return !done;
    }

    @Override
    public Mask next()
    {
        Vector[] states = new LongVector[aggregations.size()];

        int maxGroup = -1;
        while (source.hasNext()) {
            Mask mask = source.next();
            LongVector group = (LongVector) source.column(groupColumn);

            long previousMaxGroup = maxGroup;
            for (int position : mask) {
                maxGroup = toIntExact(Math.max(maxGroup, group.values()[position]));
            }

            int newCapacity = computeCapacity(maxGroup + 1);
            for (int i = 0; i < aggregations.size(); i++) {
                Accumulator accumulator = aggregations.get(i);

                states[i] = ensureCapacity(states[i], newCapacity);
                accumulator.initialize(states[i], toIntExact(previousMaxGroup + 1), toIntExact(maxGroup - previousMaxGroup));
                accumulator.accumulate(states[i], group, mask, source::column);
            }
        }

        int newCapacity = computeCapacity(maxGroup + 1);
        for (int i = 0; i < result.length; i++) {
            result[i] = ensureCapacity(result[i], newCapacity);
        }

        for (int i = 0; i < result.length; i++) {
            result[i] = aggregations.get(i).result(maxGroup, states[i], result[i]);
        }

        done = true;

        return Mask.all(maxGroup + 1);
    }

    private static int computeCapacity(int size)
    {
        return (int) (size + size * (1 + 1.0 / (Math.log(size + 1) - 6)));
    }

    private static Vector ensureCapacity(Vector state, int capacity)
    {
        LongVector vector = (LongVector) state;

        if (vector == null) {
            vector = new LongVector(new boolean[capacity], new long[capacity]);
        }
        else if (vector.values().length < capacity) {
            vector = new LongVector(
                    Arrays.copyOf(vector.nulls(), capacity),
                    Arrays.copyOf(vector.values(), capacity));
        }

        return vector;
    }

    @Override
    public void constrain(Mask mask)
    {
        // Nothing to do. All output is already computed
    }

    @Override
    public Vector column(int column)
    {
        return result[column];
    }
}
