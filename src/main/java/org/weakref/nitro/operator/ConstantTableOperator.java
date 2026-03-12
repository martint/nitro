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
import org.weakref.nitro.data.I64VectorWithNulls;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.data.Vector;

import java.util.List;

public class ConstantTableOperator
        implements Operator, BatchOperator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("ConstantTableOperator");
    private final Allocator allocator;

    private final Vector[] columns;
    private final int count;
    private boolean done;

    public ConstantTableOperator(Allocator allocator, int columnCount, List<Row> rows)
    {
        this.allocator = allocator;
        columns = new Vector[columnCount];
        for (int i = 0; i < columns.length; i++) {
            columns[i] = allocator.allocate(ALLOCATION_CONTEXT, rows.size(), I64VectorWithNulls::new);
        }

        for (int position = 0; position < rows.size(); position++) {
            Row row = rows.get(position);
            Long[] values = row.values();
            for (int column = 0; column < values.length; column++) {
                I64VectorWithNulls vector = (I64VectorWithNulls) columns[column];
                if (values[column] == null) {
                    vector.nulls()[position] = true;
                }
                else {
                    vector.nulls()[position] = false;
                    vector.values()[position] = values[column];
                }
            }
        }

        this.count = rows.size();
    }

    @Override
    public int columnCount()
    {
        return columns.length;
    }

    @Override
    public int outputCount()
    {
        return columnCount();
    }

    @Override
    public boolean hasNext()
    {
        return !done;
    }

    @Override
    public Mask next()
    {
        done = true;
        return Mask.all(count);
    }

    @Override
    public Batch nextBatch()
    {
        Mask batchMask = next();
        Output[] outputs = new Output[columnCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            outputs[outputIndex] = Output.values(columns[outputIndex]);
        }
        return new Batch(batchMask, outputs);
    }

    @Override
    public void constrain(Mask mask)
    {
    }

    @Override
    public Vector column(int column)
    {
        return columns[column];
    }

    @Override
    public void close()
    {
        allocator.release(ALLOCATION_CONTEXT);
    }
}
