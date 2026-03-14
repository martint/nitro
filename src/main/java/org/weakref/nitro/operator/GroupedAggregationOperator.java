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
import org.weakref.nitro.operator.aggregation.Accumulator;
import org.weakref.nitro.operator.aggregation.StreamAccessors;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;

import static java.lang.Math.toIntExact;

public class GroupedAggregationOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("GroupedAggregationOperator");
    private final Allocator allocator;

    private final int groupColumn;
    private final List<Accumulator> aggregations;
    private final Operator source;
    private final Streams[] result;
    private boolean done;

    public GroupedAggregationOperator(Allocator allocator, int groupColumn, List<Accumulator> aggregations, Operator source)
    {
        this.allocator = allocator;
        this.groupColumn = groupColumn;
        this.aggregations = aggregations;
        this.source = source;

        result = new Streams[aggregations.size()];
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
        Streams[] states = new Streams[aggregations.size()];

        long maxGroup = -1;
        while (source.hasNext()) {
            Batch batch = source.next();
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

                states[i] = states[i] == null
                        ? accumulator.allocate(allocator, ALLOCATION_CONTEXT, newCapacity)
                        : accumulator.grow(allocator, ALLOCATION_CONTEXT, states[i], newCapacity);
                accumulator.initialize(states[i], toIntExact(previousMaxGroup + 1), toIntExact(maxGroup - previousMaxGroup));
                accumulator.accumulate(states[i], group, mask, StreamAccessors.forBatch(batch));
            }
        }

        for (int i = 0; i < result.length; i++) {
            result[i] = aggregations.get(i).result(toIntExact(maxGroup), states[i], result[i], allocator, ALLOCATION_CONTEXT);
        }

        done = true;

        return allocator.allocateAllMask(ALLOCATION_CONTEXT, toIntExact(maxGroup + 1));
    }

    @Override
    public Batch next()
    {
        Mask batchMask = computeResults();
        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            int output = outputIndex;
            outputs[outputIndex] = resultOutput(output);
        }
        return new Batch(batchMask, takenMask -> allocator.transfer(ALLOCATION_CONTEXT, takenMask), outputs);
    }

    @Override
    public void constrain(Mask mask)
    {
        // Nothing to do. All output is already computed
    }

    private Output resultOutput(int output)
    {
        return new Output(result[output].asMap().keySet(), result[output]::get, (stream, vector) -> allocator.transfer(ALLOCATION_CONTEXT, vector));
    }

    @Override
    public void close()
    {
        source.close();
        allocator.release(ALLOCATION_CONTEXT);
    }
}
