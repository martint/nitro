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
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Host-driven single-row cardinality state. Offered batches remain caller-owned and are copied
 * before {@link #addInput(Batch)} returns.
 */
public final class EnforceSingleRowSession
        implements Operator
{
    private final Allocator.Context allocationContext =
            new Allocator.Context("EnforceSingleRowSession", EnforceSingleRowSession.class);
    private final Allocator allocator;
    private final int inputColumns;
    private final List<TableOperator.Page> pages = new ArrayList<>();
    private final EnforceSingleRowOperator operator;

    private int rowCount;
    private boolean finished;
    private boolean closed;

    public EnforceSingleRowSession(Allocator allocator, Schema inputSchema)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        inputColumns = requireNonNull(inputSchema, "inputSchema is null").size();
        operator = new EnforceSingleRowOperator(allocator, new TableOperator(inputSchema, pages));
    }

    public void addInput(Batch batch)
    {
        requireNonNull(batch, "batch is null");
        checkOpen();
        if (finished) {
            throw new IllegalStateException("EnforceSingleRow input is finished");
        }
        Mask mask = batch.borrowMask();
        if (mask.none()) {
            return;
        }
        if (rowCount != 0 || mask.count() > 1) {
            throw new EnforceSingleRowOperator.MultipleRowsException();
        }
        rowCount += mask.count();
        Streams[] columns = new Streams[inputColumns];
        for (int outputIndex = 0; outputIndex < columns.length; outputIndex++) {
            Output output = batch.output(outputIndex);
            Streams.Builder borrowed = Streams.builder();
            for (Stream stream : output.streams()) {
                borrowed.put(stream, output.borrow(stream));
            }
            columns[outputIndex] = allocator.copyStreams(allocationContext, borrowed.build(), mask);
        }
        pages.add(new TableOperator.Page(mask.count(), columns, Mask.all(mask.count())));
    }

    public void finishInput()
    {
        checkOpen();
        if (finished) {
            throw new IllegalStateException("EnforceSingleRow input is already finished");
        }
        finished = true;
    }

    @Override
    public int outputCount()
    {
        return operator.outputCount();
    }

    @Override
    public Schema outputSchema()
    {
        return operator.outputSchema();
    }

    @Override
    public boolean hasNext()
    {
        checkFinished();
        return operator.hasNext();
    }

    @Override
    public Batch next()
    {
        checkFinished();
        return operator.next();
    }

    @Override
    public void constrain(Mask mask)
    {
        operator.constrain(mask);
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        return true;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        try {
            operator.close();
        }
        finally {
            allocator.release(allocationContext);
        }
    }

    private void checkFinished()
    {
        checkOpen();
        if (!finished) {
            throw new IllegalStateException("EnforceSingleRow input is not finished");
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("EnforceSingleRow session is closed");
        }
    }
}
