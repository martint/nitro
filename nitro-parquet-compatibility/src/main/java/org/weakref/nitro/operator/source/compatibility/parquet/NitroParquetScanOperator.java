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
package org.weakref.nitro.operator.source.compatibility.parquet;

import org.weakref.nitro.core.batch.ColumnView;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.VectorColumnCapability;
import org.weakref.nitro.data.VectorColumnGeneration;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.DynamicFilter;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Output;
import org.weakref.nitro.operator.StaticDomainFilter;
import org.weakref.nitro.operator.StaticFilterEnforcement;
import org.weakref.nitro.operator.source.AllocatedSelectionOperatorIngress;
import org.weakref.nitro.operator.source.BatchSourceOperator;
import org.weakref.nitro.operator.source.ColumnViewOperatorIngress;
import org.weakref.nitro.operator.source.ColumnViewSourceOperatorIngress;
import org.weakref.nitro.operator.source.LongDomainRuntimeFilterSourceIngress;
import org.weakref.nitro.parquet.NitroParquetBatchSource;
import org.weakref.nitro.parquet.NitroParquetScanResources;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/// Pull-operator composition adapter for the native Nitro Parquet batch source.
public final class NitroParquetScanOperator
        implements Operator
{
    private final BatchSourceOperator delegate;

    public NitroParquetScanOperator(
            NitroParquetScanResources resources,
            Allocator allocator,
            List<Path> paths,
            List<String> columns)
    {
        requireNonNull(allocator, "allocator is null");
        Schema schema = Schema.unspecified(requireNonNull(columns, "columns is null"));
        this.delegate = new BatchSourceOperator(
                new NitroParquetBatchSource(resources, allocator, paths, schema),
                new ColumnViewSourceOperatorIngress(
                        schema,
                        new AllocatedSelectionOperatorIngress(
                                allocator,
                                new Allocator.Context("NitroParquetScanOperatorIngress")),
                        new LongDomainRuntimeFilterSourceIngress(),
                        field -> new CompatibilityVectorColumnIngress(field.type())));
    }

    @Override
    public int outputCount()
    {
        return delegate.outputCount();
    }

    @Override
    public Schema outputSchema()
    {
        return delegate.outputSchema();
    }

    @Override
    public boolean hasNext()
    {
        return delegate.hasNext();
    }

    @Override
    public Batch next()
    {
        return delegate.next();
    }

    @Override
    public void constrain(org.weakref.nitro.data.Mask mask)
    {
        delegate.constrain(mask);
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        return delegate.supportsRetainedBatches();
    }

    @Override
    public boolean supportsStableBatchBorrow()
    {
        return delegate.supportsStableBatchBorrow();
    }

    @Override
    public boolean supportsConstrainedReborrow()
    {
        return delegate.supportsConstrainedReborrow();
    }

    @Override
    public long exactOutputRows()
    {
        return delegate.exactOutputRows();
    }

    @Override
    public void pushDynamicFilter(DynamicFilter filter)
    {
        delegate.pushDynamicFilter(filter);
    }

    @Override
    public StaticFilterEnforcement pushStaticFilter(StaticDomainFilter filter)
    {
        return delegate.pushStaticFilter(filter);
    }

    @Override
    public boolean supportsDynamicFilterPushdown(int column)
    {
        return delegate.supportsDynamicFilterPushdown(column);
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    private record CompatibilityVectorColumnIngress(TypeBinding type)
            implements ColumnViewOperatorIngress
    {
        private CompatibilityVectorColumnIngress
        {
            requireNonNull(type, "type is null");
        }

        @Override
        public Output output(Supplier<ColumnView> column)
        {
            requireNonNull(column, "column is null");
            ColumnView view = column.get();
            Set<Stream> streams = requireNonNull(view.streams(), "column returned null streams");
            VectorColumnGeneration generation = view.capability(VectorColumnCapability.VECTOR_GENERATION)
                    .orElseThrow(() -> new IllegalArgumentException("column does not expose a Nitro vector generation"));
            return new Output(
                    streams,
                    generation::borrow,
                    (stream, mask) -> mask == null ? generation.borrow(stream) : generation.borrow(stream, mask),
                    generation::tryBorrowMask,
                    (stream, _) -> generation.take(stream),
                    (_, _) -> {},
                    (_, existing, positions, sourceStart, sourceCount, outputStart, size, assumeClear) ->
                            generation.copyPositions(existing, positions, sourceStart, sourceCount, outputStart, size, assumeClear),
                    generation::copySinglePosition);
        }
    }
}
