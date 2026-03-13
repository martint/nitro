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
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.aggregation.Accumulator;
import org.weakref.nitro.operator.aggregation.StreamAccessors;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;

import static java.lang.Math.toIntExact;

public class GroupedAggregationOperator
        implements BatchOperator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("GroupedAggregationOperator");
    private final Allocator allocator;

    private final int groupColumn;
    private final List<Accumulator> aggregations;
    private final BatchOperator source;
    private final Vector[] result;
    private boolean done;

    public GroupedAggregationOperator(Allocator allocator, int groupColumn, List<Accumulator> aggregations, BatchOperator source)
    {
        this.allocator = allocator;
        this.groupColumn = groupColumn;
        this.aggregations = aggregations;
        this.source = source;

        result = new Vector[aggregations.size()];
    }

    @Override
    public int outputCount()
    {
        return aggregations.size();
    }

    @Override
    public boolean hasNext()
    {
        return !done;
    }

    private Mask computeResults()
    {
        Vector[] states = new Vector[aggregations.size()];

        long maxGroup = -1;
        while (source.hasNext()) {
            Batch batch = source.nextBatch();
            Mask mask = batch.borrowMask();
            I64Vector group = (I64Vector) batch.output(groupColumn).borrow(Stream.VALUES);

            long previousMaxGroup = maxGroup;
            if (mask.all()) {
                for (int position = 0; position <= mask.maxPosition(); position++) {
                    maxGroup = Math.max(maxGroup, group.values()[position]);
                }
            }
            else {
                for (int position : mask) {
                    maxGroup = Math.max(maxGroup, group.values()[position]);
                }
            }

            int newCapacity = Allocator.computeCapacity(toIntExact(maxGroup + 1));
            for (int i = 0; i < aggregations.size(); i++) {
                Accumulator accumulator = aggregations.get(i);

                states[i] = allocator.allocateOrGrow(ALLOCATION_CONTEXT, states[i], newCapacity, accumulator::allocate);
                accumulator.initialize(states[i], toIntExact(previousMaxGroup + 1), toIntExact(maxGroup - previousMaxGroup));
                accumulator.accumulate(states[i], group, mask, StreamAccessors.forBatch(batch));
            }
        }

        int newCapacity = Allocator.computeCapacity(toIntExact(maxGroup + 1));
        for (int i = 0; i < result.length; i++) {
            result[i] = allocator.allocateOrGrow(ALLOCATION_CONTEXT, result[i], newCapacity, aggregations.get(i)::allocate);
            result[i] = aggregations.get(i).result(toIntExact(maxGroup), states[i], result[i]);
        }

        done = true;

        // TODO: reuse mask if possible
        return Mask.all(toIntExact(maxGroup + 1));
    }

    @Override
    public Batch nextBatch()
    {
        Mask batchMask = computeResults();
        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            int output = outputIndex;
            outputs[outputIndex] = Output.lazyValues(() -> resultVector(output));
        }
        return new Batch(batchMask, outputs);
    }

    @Override
    public void constrain(Mask mask)
    {
        // Nothing to do. All output is already computed
    }

    private Vector resultVector(int output)
    {
        return result[output];
    }

    @Override
    public void close()
    {
        source.close();
        allocator.release(ALLOCATION_CONTEXT);
    }
}
