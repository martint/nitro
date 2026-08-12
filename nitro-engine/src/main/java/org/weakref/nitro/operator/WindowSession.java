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
import org.weakref.nitro.data.Vector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Host-driven blocking window state. Input batches remain caller-owned and are copied before
 * {@link #addInput(Batch)} returns; output becomes available only after {@link #finishInput()}.
 */
public final class WindowSession
        implements Operator
{
    private final Allocator.Context allocationContext = new Allocator.Context("WindowSession", WindowSession.class);
    private final Allocator allocator;
    private final int inputColumns;
    private final boolean inputFullyOrdered;
    private final List<TableOperator.Page> pages = new ArrayList<>();
    private final WindowOperator window;
    private boolean finished;
    private boolean closed;

    public WindowSession(
            Allocator allocator,
            Schema inputSchema,
            int[] partitionColumns,
            int[] orderingColumns,
            boolean[] descending,
            List<RunningWindowFunction> functions,
            Schema windowSchema,
            OperatorResources resources)
    {
        this(
                allocator,
                inputSchema,
                partitionColumns,
                orderingColumns,
                descending,
                functions,
                windowSchema,
                resources,
                WindowInputOrder.unordered());
    }

    public WindowSession(
            Allocator allocator,
            Schema inputSchema,
            int[] partitionColumns,
            int[] orderingColumns,
            boolean[] descending,
            List<RunningWindowFunction> functions,
            Schema windowSchema,
            OperatorResources resources,
            WindowInputOrder inputOrder)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        inputColumns = requireNonNull(inputSchema, "inputSchema is null").size();
        inputFullyOrdered = requireNonNull(inputOrder, "inputOrder is null").isFullyOrdered(orderingColumns.length);
        window = new WindowOperator(
                allocator,
                TableOperator.retained(inputSchema, pages),
                partitionColumns,
                orderingColumns,
                descending,
                functions,
                windowSchema,
                resources,
                inputOrder);
    }

    public void addInput(Batch batch)
    {
        requireNonNull(batch, "batch is null");
        checkOpen();
        if (finished) {
            throw new IllegalStateException("Window input is finished");
        }
        Mask mask = batch.borrowMask();
        if (mask.none()) {
            return;
        }
        int[] selectedPositions = null;
        if (!inputFullyOrdered) {
            selectedPositions = allocator.primitiveArrays().borrowInts(mask.count());
            int selectedIndex = 0;
            for (int position : mask) {
                selectedPositions[selectedIndex++] = position;
            }
        }
        try {
            Streams[] columns = new Streams[inputColumns];
            for (int outputIndex = 0; outputIndex < columns.length; outputIndex++) {
                Output output = batch.output(outputIndex);
                Streams.Builder borrowed = Streams.builder();
                for (Stream stream : output.streams()) {
                    borrowed.put(stream, output.borrow(stream));
                }
                Streams streams = borrowed.build();
                // Unordered windows repeatedly traverse input while sorting, finding partitions, and evaluating
                // functions, so flatten selected rows once. Fully ordered windows traverse each page sequentially;
                // preserve an owned dictionary/RLE representation and avoid materializing it again on output.
                columns[outputIndex] = inputFullyOrdered
                        ? allocator.copyStreams(allocationContext, streams, mask)
                        : allocator.copyStreams(allocationContext, streams, selectedPositions);
            }
            pages.add(new TableOperator.Page(mask.count(), columns, Mask.all(mask.count())));
        }
        finally {
            allocator.primitiveArrays().release(selectedPositions);
        }
    }

    public void finishInput()
    {
        checkOpen();
        if (finished) {
            throw new IllegalStateException("Window input is already finished");
        }
        coalescePages();
        finished = true;
    }

    private void coalescePages()
    {
        if (inputFullyOrdered || pages.size() < 2 || !haveConsistentStreams()) {
            return;
        }
        int rows = 0;
        for (TableOperator.Page page : pages) {
            rows = Math.addExact(rows, page.rows());
        }
        Streams[] columns = new Streams[inputColumns];
        for (int columnIndex = 0; columnIndex < inputColumns; columnIndex++) {
            Streams schema = pages.getFirst().columns()[columnIndex];
            Streams.Builder result = Streams.builder();
            for (Stream stream : schema.streams()) {
                Vector[] segments = new Vector[pages.size()];
                for (int pageIndex = 0; pageIndex < pages.size(); pageIndex++) {
                    segments[pageIndex] = pages.get(pageIndex).columns()[columnIndex].get(stream);
                }
                result.put(stream, segments[0].materializeRows(allocator, allocationContext, segments));
            }
            columns[columnIndex] = result.build();
        }
        Set<Vector> released = Collections.newSetFromMap(new IdentityHashMap<>());
        for (TableOperator.Page page : pages) {
            for (Streams column : page.columns()) {
                for (Vector vector : column.asMap().values()) {
                    if (released.add(vector)) {
                        allocator.release(allocationContext, vector);
                    }
                }
            }
        }
        pages.clear();
        pages.add(new TableOperator.Page(rows, columns, Mask.all(rows)));
    }

    private boolean haveConsistentStreams()
    {
        for (int columnIndex = 0; columnIndex < inputColumns; columnIndex++) {
            var streams = pages.getFirst().columns()[columnIndex].streams();
            for (int pageIndex = 1; pageIndex < pages.size(); pageIndex++) {
                if (!pages.get(pageIndex).columns()[columnIndex].streams().equals(streams)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public int outputCount()
    {
        return window.outputCount();
    }

    @Override
    public Schema outputSchema()
    {
        return window.outputSchema();
    }

    @Override
    public boolean hasNext()
    {
        checkFinished();
        return window.hasNext();
    }

    @Override
    public Batch next()
    {
        checkFinished();
        return window.next();
    }

    @Override
    public void constrain(Mask mask)
    {
        window.constrain(mask);
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
            window.close();
        }
        finally {
            allocator.release(allocationContext);
        }
    }

    private void checkFinished()
    {
        checkOpen();
        if (!finished) {
            throw new IllegalStateException("Window input is not finished");
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("Window session is closed");
        }
    }
}
