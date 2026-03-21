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
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.ArrayList;
import java.util.List;

public class OutputOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("OutputOperator");
    private final Allocator allocator;

    private final Operator source;
    private boolean done;
    private long rowCount;

    public OutputOperator(Allocator allocator, Operator source)
    {
        this.allocator = allocator;
        this.source = source;
    }

    @Override
    public int outputCount()
    {
        return 1;
    }

    @Override
    public Batch next()
    {
        done = true;

        while (source.hasNext()) {
            try (Batch batch = source.next()) {
                Mask mask = batch.borrowMask();

                List<Output> columns = new ArrayList<>();
                for (int i = 0; i < source.outputCount(); i++) {
                    columns.add(batch.output(i));
                }

                for (int position : mask) {
                    outputRow(columns, position);
                }
            }
        }

        return new Batch(
                allocator.allocateAllMask(ALLOCATION_CONTEXT, 1),
                takenMask -> allocator.transfer(ALLOCATION_CONTEXT, takenMask),
                new Output(
                        java.util.Set.of(Stream.VALUES),
                        stream -> resultVector(),
                        (stream, vector) -> allocator.transfer(ALLOCATION_CONTEXT, vector)));
    }

    @Override
    public boolean hasNext()
    {
        return !done;
    }

    @Override
    public void constrain(Mask mask)
    {
    }

    private Vector resultVector()
    {
        I64Vector result = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, 1, I64Vector::new);
        result.values()[0] = rowCount;
        return result;
    }

    private void outputRow(List<Output> columns, int position)
    {
        rowCount++;
        for (int column = 0; column < columns.size(); column++) {
            Output block = columns.get(column);
            Vector values = block.borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) block.borrowOrNull(Stream.NULLS);

            if (nulls != null && nulls.values()[position]) {
                System.out.print("null");
            }
            else {
                System.out.print(formatValue(values, position));
            }

            if (column < columns.size() - 1) {
                System.out.print("\t");
            }
        }
        System.out.println();
    }

    @Override
    public void close()
    {
        source.close();
        allocator.release(ALLOCATION_CONTEXT);
    }

    private static Object formatValue(Vector values, int position)
    {
        return switch (values) {
            case I32Vector vector -> vector.values()[position];
            case I64Vector vector -> vector.values()[position];
            default -> throw new IllegalArgumentException("Unsupported output vector type: " + values.getClass().getSimpleName());
        };
    }
}
