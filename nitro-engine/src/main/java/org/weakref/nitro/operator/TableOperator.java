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

import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;

import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class TableOperator
        implements Operator
{
    private final Schema outputSchema;
    private final List<Page> pages;
    private final boolean retainedBatches;

    private int currentPage = -1;

    public TableOperator(int columns, List<Page> pages)
    {
        this(Schema.unspecified(columns), pages, false);
    }

    public TableOperator(Schema outputSchema, List<Page> pages)
    {
        this(outputSchema, pages, false);
    }

    /**
     * Creates a table over pages whose streams remain valid for the lifetime of this operator.
     * <p>
     * This capability allows buffering operators to retain the supplied vectors instead of copying
     * them. The caller remains responsible for keeping the pages' underlying storage alive until
     * every consumer of this operator has closed.
     */
    public static TableOperator retained(Schema outputSchema, List<Page> pages)
    {
        return new TableOperator(outputSchema, pages, true);
    }

    private TableOperator(Schema outputSchema, List<Page> pages, boolean retainedBatches)
    {
        this.outputSchema = requireNonNull(outputSchema, "outputSchema is null");
        this.pages = pages;
        this.retainedBatches = retainedBatches;
    }

    @Override
    public int outputCount()
    {
        return outputSchema.size();
    }

    @Override
    public Schema outputSchema()
    {
        return outputSchema;
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
        // Pages may be retained and consumed by multiple operators concurrently. Vectors are immutable for that
        // lifetime, but a Batch mask is deliberately mutable (filters and join-build pruning constrain it in place),
        // so every consumer must receive an independent selection.
        return new Batch(page.mask().copy(), outputs);
    }

    @Override
    public void constrain(Mask mask)
    {
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        return retainedBatches;
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
