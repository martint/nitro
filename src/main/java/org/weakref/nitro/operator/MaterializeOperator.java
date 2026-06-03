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
import org.weakref.nitro.operator.evaluator.ir.Stream;

public final class MaterializeOperator
        implements Operator
{
    private final Allocator allocator;
    private final Allocator.Context allocationContext = new Allocator.Context("MaterializeOperator");
    private final int outputCount;
    private Operator source;
    private TableOperator materialized;

    public MaterializeOperator(Allocator allocator, Operator source)
    {
        this.allocator = allocator;
        this.source = source;
        this.outputCount = source.outputCount();
    }

    @Override
    public int outputCount()
    {
        return outputCount;
    }

    @Override
    public boolean hasNext()
    {
        if (materialized == null) {
            materialized = materialize();
        }
        return materialized.hasNext();
    }

    @Override
    public Batch next()
    {
        if (!hasNext()) {
            throw new IllegalStateException("No more batches");
        }
        return materialized.next();
    }

    @Override
    public void constrain(Mask mask)
    {
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        return true;
    }

    @Override
    public boolean supportsConstrainedReborrow()
    {
        // constrain() is a no-op, so a downstream constrain + re-borrow would re-read the full,
        // differently-indexed output. Cannot satisfy a constrained re-borrow.
        return false;
    }

    @Override
    public void close()
    {
        if (materialized != null) {
            materialized.close();
            materialized = null;
        }
        if (source != null) {
            source.close();
            source = null;
        }
        allocator.release(allocationContext);
    }

    private TableOperator materialize()
    {
        java.util.List<TableOperator.Page> pages = new java.util.ArrayList<>();
        try {
            while (source.hasNext()) {
                try (Batch batch = source.next()) {
                    Mask mask = batch.borrowMask();
                    if (mask.none()) {
                        continue;
                    }

                    Streams[] columns = new Streams[outputCount];
                    for (int outputIndex = 0; outputIndex < columns.length; outputIndex++) {
                        columns[outputIndex] = copyOutputStreams(batch.output(outputIndex));
                    }
                    pages.add(new TableOperator.Page(mask.count(), columns, allocator.copyMask(allocationContext, mask)));
                }
            }
        }
        finally {
            source.close();
            source = null;
        }
        return new TableOperator(outputCount, pages);
    }

    private Streams copyOutputStreams(Output output)
    {
        Streams.Builder builder = Streams.builder();
        for (Stream stream : output.streams()) {
            builder.put(stream, allocator.copyVector(allocationContext, output.borrow(stream)));
        }
        return builder.build();
    }
}
