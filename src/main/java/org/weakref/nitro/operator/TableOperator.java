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

import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;

import java.util.Arrays;
import java.util.List;

public class TableOperator
        implements Operator
{
    private final int columns;
    private final List<Page> pages;

    private int currentPage = -1;

    public TableOperator(int columns, List<Page> pages)
    {
        this.columns = columns;
        this.pages = pages;
    }

    @Override
    public int outputCount()
    {
        return columns;
    }

    @Override
    public boolean hasNext()
    {
        return currentPage < pages.size() - 1;
    }

    @Override
    public Batch next()
    {
        currentPage++;
        Page page = pages.get(currentPage);
        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            outputs[outputIndex] = Output.of(page.columns()[outputIndex]);
        }
        return new Batch(page.mask(), outputs);
    }

    @Override
    public void constrain(Mask mask)
    {
    }

    @Override
    public void close()
    {
    }

    public record Page(int rows, Streams[] columns, Mask mask)
    {
        public Page
        {
            columns = Arrays.copyOf(columns, columns.length);
        }

        public static Page values(int rows, Vector[] columns, Mask mask)
        {
            Streams[] streams = new Streams[columns.length];
            for (int index = 0; index < columns.length; index++) {
                streams[index] = Streams.ofValues(columns[index]);
            }
            return new Page(rows, streams, mask);
        }
    }
}
