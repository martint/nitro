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
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.addExact;
import static java.util.Objects.requireNonNull;

/**
 * Random-access storage for a sequence of Nitro batches which preserves their physical encodings when ownership
 * permits it.
 *
 * <p>A retained source remains responsible for its vector storage, so this buffer keeps every source batch open
 * until close. A source which may reuse a batch on advance is compacted into allocator-owned storage first. This is
 * the ownership boundary needed by partition operators whose logical rows can span several source batches.
 */
final class EncodedRowBuffer
        implements RowPositionIndex, AutoCloseable
{
    private final Allocator allocator;
    private final Allocator.Context allocationContext;
    private final int columnCount;
    private final List<Page> pages = new ArrayList<>();

    private int size;
    private int cachedPageIndex = -1;
    private int cachedPageStart;
    private int cachedPageEnd;
    private boolean loaded;
    private boolean closed;

    EncodedRowBuffer(Allocator allocator, Allocator.Context allocationContext, int columnCount)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.allocationContext = requireNonNull(allocationContext, "allocationContext is null");
        if (columnCount < 0) {
            throw new IllegalArgumentException("columnCount is negative");
        }
        this.columnCount = columnCount;
    }

    void load(Operator source)
    {
        requireNonNull(source, "source is null");
        checkOpen();
        if (loaded) {
            throw new IllegalStateException("row buffer is already loaded");
        }
        if (source.outputCount() != columnCount) {
            throw new IllegalArgumentException("source column count does not match row buffer");
        }
        loaded = true;

        boolean retain = source.supportsRetainedBatches();
        try {
            while (source.hasNext()) {
                Batch batch = source.next();
                if (batch.borrowMask().none()) {
                    batch.close();
                    continue;
                }
                if (retain) {
                    appendRetained(batch);
                }
                else {
                    appendCopied(batch);
                }
            }
        }
        catch (RuntimeException | Error failure) {
            try {
                close();
            }
            catch (RuntimeException | Error closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private void appendRetained(Batch batch)
    {
        try {
            Mask mask = batch.borrowMask();
            Streams[] columns = borrowColumns(batch);
            int start = size;
            size = addExact(size, mask.selectedCount());
            pages.add(new Page(start, size, columns, mask, batch));
        }
        catch (RuntimeException | Error failure) {
            try {
                batch.close();
            }
            catch (RuntimeException | Error closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private void appendCopied(Batch batch)
    {
        try (batch) {
            Mask mask = batch.borrowMask();
            Streams[] columns = new Streams[columnCount];
            for (int column = 0; column < columnCount; column++) {
                columns[column] = allocator.copyStreams(allocationContext, borrowStreams(batch.output(column)), mask);
            }
            int start = size;
            size = addExact(size, mask.selectedCount());
            pages.add(new Page(start, size, columns, Mask.all(mask.selectedCount()), null));
        }
    }

    private Streams[] borrowColumns(Batch batch)
    {
        Streams[] columns = new Streams[columnCount];
        for (int column = 0; column < columnCount; column++) {
            columns[column] = borrowStreams(batch.output(column));
        }
        return columns;
    }

    private static Streams borrowStreams(Output output)
    {
        Streams.Builder streams = Streams.builder();
        for (Stream stream : output.streams()) {
            streams.put(stream, output.borrow(stream));
        }
        return streams.build();
    }

    @Override
    public int size()
    {
        checkOpen();
        return size;
    }

    @Override
    public Streams column(int column, int position)
    {
        if (column < 0 || column >= columnCount) {
            throw new IndexOutOfBoundsException("column is outside row buffer");
        }
        return page(position).columns()[column];
    }

    @Override
    public int sourcePosition(int position)
    {
        Page page = page(position);
        return page.mask().position(position - page.start());
    }

    @Override
    public boolean sharesSource(int leftPosition, int rightPosition)
    {
        return pageIndex(leftPosition) == pageIndex(rightPosition);
    }

    private Page page(int position)
    {
        return pages.get(pageIndex(position));
    }

    private int pageIndex(int position)
    {
        checkOpen();
        if (position < 0 || position >= size) {
            throw new IndexOutOfBoundsException("position is outside row buffer");
        }
        if (cachedPageIndex >= 0 && position >= cachedPageStart && position < cachedPageEnd) {
            return cachedPageIndex;
        }

        int low = 0;
        int high = pages.size() - 1;
        while (low <= high) {
            int middle = (low + high) >>> 1;
            Page page = pages.get(middle);
            if (position < page.start()) {
                high = middle - 1;
            }
            else if (position >= page.end()) {
                low = middle + 1;
            }
            else {
                cachedPageIndex = middle;
                cachedPageStart = page.start();
                cachedPageEnd = page.end();
                return middle;
            }
        }
        throw new AssertionError("row buffer page is missing");
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        RuntimeException closeFailure = null;
        for (Page page : pages) {
            if (page.retainedBatch() == null) {
                continue;
            }
            try {
                page.retainedBatch().close();
            }
            catch (RuntimeException failure) {
                if (closeFailure == null) {
                    closeFailure = failure;
                }
                else {
                    closeFailure.addSuppressed(failure);
                }
            }
        }
        pages.clear();
        allocator.release(allocationContext);
        if (closeFailure != null) {
            throw closeFailure;
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("row buffer is closed");
        }
    }

    private record Page(int start, int end, Streams[] columns, Mask mask, Batch retainedBatch) {}
}
