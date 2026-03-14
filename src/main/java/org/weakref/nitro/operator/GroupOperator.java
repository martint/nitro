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

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

public class GroupOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("GroupOperator");
    private final Allocator allocator;

    private final int groupByColumn;
    private final Operator source;

    private final Long2LongMap groups = new Long2LongOpenHashMap();
    private boolean filled;
    private Batch currentBatch;
    private Mask mask;
    private I64Vector result;

    public GroupOperator(Allocator allocator, int groupByColumn, Operator source)
    {
        this.allocator = allocator;
        this.groupByColumn = groupByColumn;
        this.source = source;

        groups.defaultReturnValue(-1);
    }

    @Override
    public int outputCount()
    {
        return source.outputCount() + 1;
    }

    @Override
    public Batch next()
    {
        filled = false;
        currentBatch = source.next();
        mask = currentBatch.borrowMask();

        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            if (outputIndex == 0) {
                outputs[outputIndex] = Output.lazyValues(this::groupIds);
            }
            else {
                Output sourceOutput = currentBatch.output(outputIndex - 1);
                outputs[outputIndex] = new Output(sourceOutput.streams(), sourceOutput::borrow);
            }
        }
        return new Batch(mask, outputs);
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

    private Vector groupIds()
    {
        doGroupingIfNeeded();
        return result;
    }

    private void doGroupingIfNeeded()
    {
        if (!filled && !mask.none()) {
            filled = true;
            result = allocator.reallocateIfNecessary(ALLOCATION_CONTEXT, result, I64Vector.class, mask.count(), I64Vector::new);

            // TODO: support arbitrary types
            long[] values = values(currentBatch.output(groupByColumn).borrow(Stream.VALUES));

            for (int position : mask) {
                // TODO: handle nulls
                long value = values[position];
                long group = groups.size();

                long existing = groups.putIfAbsent(value, group);
                if (existing != -1) {
                    group = existing;
                }

                result.values()[position] = group;
            }
        }
    }

    @Override
    public void close()
    {
        source.close();
        allocator.release(ALLOCATION_CONTEXT);
    }

    private static long[] values(Vector vector)
    {
        return ((I64Vector) vector).values();
    }
}
