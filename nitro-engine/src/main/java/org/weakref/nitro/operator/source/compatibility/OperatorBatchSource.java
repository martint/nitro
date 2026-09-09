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
package org.weakref.nitro.operator.source.compatibility;

import org.weakref.nitro.core.batch.SourceBatch;
import org.weakref.nitro.core.source.BatchSource;
import org.weakref.nitro.core.source.OrdinalSourceColumnHandle;
import org.weakref.nitro.core.source.RuntimeFilter;
import org.weakref.nitro.core.source.RuntimeFilterAcceptance;
import org.weakref.nitro.core.source.SourceCapability;
import org.weakref.nitro.core.source.SourceColumnHandle;
import org.weakref.nitro.core.source.SourcePoll;
import org.weakref.nitro.core.source.SourceProtocol;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.StaticDomainFilter;

import java.util.EnumSet;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/// Transitional adapter that exposes an existing native source through the new source port.
public final class OperatorBatchSource
        implements BatchSource
{
    private final Operator source;
    private final Schema schema;
    private final SourceColumnHandle[] columns;
    private final Set<SourceCapability> capabilities;
    private final Set<SourceCapability> constrainedReborrowCapabilities;
    private boolean closed;

    public OperatorBatchSource(Operator source)
    {
        this(source, source.outputSchema());
    }

    public OperatorBatchSource(Operator source, Schema schema)
    {
        this.source = requireNonNull(source, "source is null");
        this.schema = requireNonNull(schema, "schema is null");
        if (schema.size() != source.outputCount()) {
            throw new IllegalArgumentException("schema size does not match source output count");
        }
        this.columns = new SourceColumnHandle[schema.size()];
        EnumSet<SourceCapability> capabilities = EnumSet.of(SourceCapability.LAZY_COLUMNS, SourceCapability.SELECTION_PUSHDOWN);
        for (int column = 0; column < columns.length; column++) {
            columns[column] = new OrdinalSourceColumnHandle(column, schema.field(column).type());
            if (source.supportsDynamicFilterPushdown(column)) {
                capabilities.add(SourceCapability.RUNTIME_FILTER);
            }
        }
        if (source.supportsRetainedBatches()) {
            capabilities.add(SourceCapability.RETAINED_BATCHES);
        }
        if (source.supportsStableBatchBorrow()) {
            capabilities.add(SourceCapability.STABLE_BATCH_BORROW);
        }
        this.capabilities = Set.copyOf(capabilities);
        capabilities.add(SourceCapability.CONSTRAINED_REBORROW);
        this.constrainedReborrowCapabilities = Set.copyOf(capabilities);
    }

    /// Exposes one already-owned native batch through the format-neutral source-batch contract.
    public static SourceBatch batch(Schema schema, Batch batch)
    {
        return new NativeSourceBatch(
                requireNonNull(schema, "schema is null"),
                true,
                true,
                requireNonNull(batch, "batch is null"));
    }

    @Override
    public Schema schema()
    {
        return schema;
    }

    @Override
    public SourceColumnHandle column(int outputIndex)
    {
        return columns[outputIndex];
    }

    @Override
    public Set<SourceCapability> capabilities()
    {
        return source.supportsConstrainedReborrow() ? constrainedReborrowCapabilities : capabilities;
    }

    @Override
    public OptionalLong exactRows()
    {
        long rows = source.exactOutputRows();
        return rows < 0 ? OptionalLong.empty() : OptionalLong.of(rows);
    }

    @Override
    public <T> Optional<T> protocol(SourceProtocol<T> protocol)
    {
        if (protocol == NativeOperatorProtocol.NATIVE_OPERATOR) {
            return Optional.of(protocol.valueType().cast((NativeOperatorAccess) () -> source));
        }
        return Optional.empty();
    }

    @Override
    public SourcePoll poll()
    {
        checkOpen();
        if (!source.hasNext()) {
            return SourcePoll.Finished.FINISHED;
        }
        return new SourcePoll.Ready(new NativeSourceBatch(
                schema(),
                source.supportsRetainedBatches(),
                source.supportsStableBatchBorrow(),
                source::next));
    }

    @Override
    public RuntimeFilterAcceptance addRuntimeFilter(RuntimeFilter filter)
    {
        checkOpen();
        requireNonNull(filter, "filter is null");
        int column = columnIndex(filter.column());
        if (column < 0 || !source.supportsDynamicFilterPushdown(column)) {
            return RuntimeFilterAcceptance.REJECTED;
        }
        NativeRuntimeFilterAccess nativeFilter = filter.domain()
                .capability(NativeRuntimeFilterCapability.NATIVE_RUNTIME_FILTER)
                .orElse(null);
        if (nativeFilter == null) {
            if (filter.residualRequired() || filter.approximate()) {
                return RuntimeFilterAcceptance.REJECTED;
            }
            return source.pushStaticFilter(new StaticDomainFilter(column, filter.domain())).enforced()
                    ? RuntimeFilterAcceptance.ENFORCED
                    : RuntimeFilterAcceptance.REJECTED;
        }
        source.pushDynamicFilter(nativeFilter.retarget(column));
        return RuntimeFilterAcceptance.ACCEPTED_WITH_RESIDUAL;
    }

    @Override
    public boolean supportsRuntimeFilter(SourceColumnHandle column)
    {
        int index = columnIndex(requireNonNull(column, "column is null"));
        return index >= 0 && source.supportsDynamicFilterPushdown(index);
    }

    @Override
    public void close()
    {
        if (!closed) {
            closed = true;
            source.close();
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("source is closed");
        }
    }

    private int columnIndex(SourceColumnHandle handle)
    {
        for (int index = 0; index < columns.length; index++) {
            if (columns[index] == handle) {
                return index;
            }
        }
        return -1;
    }
}
