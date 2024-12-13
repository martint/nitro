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

import java.util.ArrayList;
import java.util.List;

public class OutputOperator
        implements Operator
{
    private final Operator source;
    private boolean done;
    private long rowCount;

    public OutputOperator(Operator source)
    {
        this.source = source;
    }

    @Override
    public int columnCount()
    {
        return 1;
    }

    @Override
    public Mask next()
    {
        done = true;

        while (source.hasNext()) {
            Mask mask = source.next();

            List<Vector> columns = new ArrayList<>();
            for (int i = 0; i < source.columnCount(); i++) {
                columns.add(source.column(i));
            }

            for (int position : mask) {
                outputRow(columns, position);
            }
        }

        return Mask.all(1);
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

    @Override
    public Vector column(int column)
    {
        return new LongVector(new boolean[1], new long[] {rowCount});
    }

    private void outputRow(List<Vector> columns, int position)
    {
        rowCount++;
        for (int column = 0; column < columns.size(); column++) {
            LongVector block = (LongVector) columns.get(column);

            if (block.nulls()[position]) {
                System.out.print("null");
            }
            else {
                System.out.print(block.values()[position]);
            }

            if (column < columns.size() - 1) {
                System.out.print("\t");
            }
        }
        System.out.println();
    }
}
