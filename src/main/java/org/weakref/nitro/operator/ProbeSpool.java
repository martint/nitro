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

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.ArrayList;
import java.util.List;

/**
 * Bounded replay buffer used to derive complete probe-key filters before a large inner build is read.
 */
final class ProbeSpool
        implements Operator
{
    private final Allocator allocator;
    private final Allocator.Context allocationContext = new Allocator.Context("ProbeSpool");
    private final PrimitiveArrayPool arrayPool;
    private final Operator source;
    private final int outputCount;
    private final List<TableOperator.Page> pages = new ArrayList<>();

    private int nextPage;
    private int bufferedRows;
    private boolean prepared;

    ProbeSpool(Allocator allocator, Operator source, int outputCount)
    {
        this.allocator = allocator;
        this.arrayPool = allocator.primitiveArrays();
        this.source = source;
        this.outputCount = outputCount;
    }

    /** Returns complete per-key sets, or null when the bounded prefix did not contain the whole input. */
    LongSet[] prepare(int[] keyColumns, int maxRows)
    {
        if (prepared) {
            throw new IllegalStateException("Probe spool is already prepared");
        }
        prepared = true;
        LongSet[] values = new LongSet[keyColumns.length];
        for (int index = 0; index < values.length; index++) {
            values[index] = new LongOpenHashSet(Math.min(maxRows, 1 << 16));
        }

        boolean supported = true;
        while (bufferedRows <= maxRows && source.hasNext()) {
            try (Batch batch = source.next()) {
                Mask mask = batch.borrowMask();
                if (mask.none()) {
                    continue;
                }
                if (supported) {
                    supported = collectKeys(batch, mask, keyColumns, values);
                }
                pages.add(copyPage(batch, mask));
                bufferedRows += mask.count();
            }
            if (bufferedRows > maxRows) {
                return null;
            }
        }
        return supported && !source.hasNext() ? values : null;
    }

    int bufferedRows()
    {
        return bufferedRows;
    }

    private boolean collectKeys(Batch batch, Mask mask, int[] keyColumns, LongSet[] values)
    {
        VectorAccess.LongValues[] keyValues = new VectorAccess.LongValues[keyColumns.length];
        VectorAccess.BooleanValues[] keyNulls = new VectorAccess.BooleanValues[keyColumns.length];
        try {
            for (int keyIndex = 0; keyIndex < keyColumns.length; keyIndex++) {
                Output output = batch.output(keyColumns[keyIndex]);
                keyValues[keyIndex] = VectorAccess.longValues(output.borrow(Stream.VALUES));
                Vector nulls = output.borrowOrNull(Stream.NULLS);
                keyNulls[keyIndex] = nulls == null ? null : VectorAccess.booleanValues(nulls);
            }
        }
        catch (IllegalArgumentException _) {
            return false;
        }
        for (int position : mask) {
            boolean hasNull = false;
            for (VectorAccess.BooleanValues nulls : keyNulls) {
                hasNull |= nulls != null && nulls.value(position);
            }
            if (hasNull) {
                continue;
            }
            for (int keyIndex = 0; keyIndex < keyValues.length; keyIndex++) {
                values[keyIndex].add(keyValues[keyIndex].value(position));
            }
        }
        return true;
    }

    private TableOperator.Page copyPage(Batch batch, Mask mask)
    {
        int count = mask.count();
        int[] positions = arrayPool.borrowInts(count);
        try {
            int index = 0;
            for (int position : mask) {
                positions[index++] = position;
            }
            Streams[] columns = new Streams[outputCount];
            for (int outputIndex = 0; outputIndex < outputCount; outputIndex++) {
                Output output = batch.output(outputIndex);
                Streams.Builder copy = Streams.builder();
                for (Stream stream : output.streams()) {
                    copy.put(stream, allocator.copyVector(allocationContext, output.borrow(stream), positions));
                }
                columns[outputIndex] = copy.build();
            }
            return new TableOperator.Page(count, columns, allocator.allocateAllMask(allocationContext, count));
        }
        finally {
            arrayPool.release(positions);
        }
    }

    @Override
    public int outputCount()
    {
        return outputCount;
    }

    @Override
    public boolean hasNext()
    {
        return nextPage < pages.size() || source.hasNext();
    }

    @Override
    public Batch next()
    {
        if (nextPage < pages.size()) {
            TableOperator.Page page = pages.get(nextPage++);
            Output[] outputs = new Output[outputCount];
            for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
                outputs[outputIndex] = Output.of(page.columns()[outputIndex]);
            }
            return new Batch(page.mask(), outputs);
        }
        return source.next();
    }

    @Override
    public void constrain(Mask mask) {}

    @Override
    public void pushDynamicFilter(DynamicFilter filter)
    {
        source.pushDynamicFilter(filter);
    }

    @Override
    public void close()
    {
        source.close();
        allocator.release(allocationContext);
    }
}
