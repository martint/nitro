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
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Row;

import java.util.List;

public class ConstantTableOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("ConstantTableOperator");
    private final Allocator allocator;

    private final I64Vector[] columns;
    private final BooleanVector[] nulls;
    private final int count;
    private boolean done;

    public ConstantTableOperator(Allocator allocator, int columnCount, List<Row> rows)
    {
        this.allocator = allocator;
        columns = new I64Vector[columnCount];
        nulls = new BooleanVector[columnCount];
        for (int i = 0; i < columns.length; i++) {
            columns[i] = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, rows.size(), I64Vector::new);
            nulls[i] = allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rows.size(), BooleanVector::new);
        }

        for (int position = 0; position < rows.size(); position++) {
            Row row = rows.get(position);
            Long[] values = row.values();
            for (int column = 0; column < values.length; column++) {
                if (values[column] == null) {
                    nulls[column].values()[position] = true;
                }
                else {
                    nulls[column].values()[position] = false;
                    columns[column].values()[position] = values[column];
                }
            }
        }

        this.count = rows.size();
    }

    @Override
    public int outputCount()
    {
        return columns.length;
    }

    @Override
    public boolean hasNext()
    {
        return !done;
    }

    @Override
    public Batch next()
    {
        done = true;
        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            Streams streams = Streams.ofValuesAndNulls(columns[outputIndex], nulls[outputIndex]);
            outputs[outputIndex] = new Output(streams.asMap().keySet(), streams::get, (stream, vector) -> allocator.transfer(ALLOCATION_CONTEXT, vector));
        }
        return new Batch(allocator.allocateAllMask(ALLOCATION_CONTEXT, count), takenMask -> allocator.transfer(ALLOCATION_CONTEXT, takenMask), outputs);
    }

    @Override
    public void constrain(Mask mask)
    {
    }

    @Override
    public void close()
    {
        allocator.release(ALLOCATION_CONTEXT);
    }
}
