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
import org.weakref.nitro.operator.aggregation.Accumulator;

import java.util.List;

public class AggregationOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("AggregationOperator");
    private final Allocator allocator;

    private final Operator source;
    private final List<Accumulator> aggregations;

    private final Vector[] results;
    private Mask mask = Mask.all(1);
    private boolean filled;
    private boolean done;

    public AggregationOperator(Allocator allocator, List<Accumulator> aggregations, Operator source)
    {
        this.allocator = allocator;
        this.source = source;
        this.aggregations = aggregations;

        results = new Vector[aggregations.size()];
        for (int i = 0; i < results.length; i++) {
            results[i] = allocator.allocate(ALLOCATION_CONTEXT, 1);
        }
    }

    @Override
    public int columnCount()
    {
        return aggregations.size();
    }

    @Override
    public Mask next()
    {
        done = true;
        return mask;
    }

    @Override
    public boolean hasNext()
    {
        return !done;
    }

    @Override
    public void constrain(Mask mask)
    {
        this.mask = mask;
    }

    @Override
    public Vector column(int column)
    {
        doAggregationIfNeeded();
        return results[column];
    }

    private void doAggregationIfNeeded()
    {
        if (!filled && !mask.none()) {
            filled = true;

            Vector[] state = new Vector[aggregations.size()];
            for (int i = 0; i < state.length; i++) {
                state[i] = allocator.allocate(ALLOCATION_CONTEXT, 1);
                aggregations.get(i).initialize(state[i], 0, 1);
            }

            while (source.hasNext()) {
                Mask mask = source.next();

                for (int aggregation = 0; aggregation < aggregations.size(); aggregation++) {
                    Accumulator accumulator = aggregations.get(aggregation);
                    accumulator.accumulate(state[aggregation], 0, mask, source::column);
                    results[aggregation] = accumulator.result(1, state[aggregation], results[aggregation]);
                }
            }
        }
    }

    @Override
    public void close()
    {
        source.close();
        allocator.release(ALLOCATION_CONTEXT);
    }
}
