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
package org.weakref.nitro.operator.source;

import org.weakref.nitro.core.batch.SourceBatch;
import org.weakref.nitro.core.source.BatchSource;
import org.weakref.nitro.core.source.RuntimeFilterAcceptance;
import org.weakref.nitro.core.source.SourceCapability;
import org.weakref.nitro.core.source.SourceOutputDemandProtocol;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.ValueDemand;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.DynamicFilter;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.StaticDomainFilter;
import org.weakref.nitro.operator.StaticFilterEnforcement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/// Reusable operator ingress for batches scheduled by an external host.
///
/// The host offers one source batch, drains the operator pipeline rooted at this feed, closes every
/// resulting native batch, and then calls [#finishInput()] before offering the next source batch.
/// The feed and the downstream pipeline remain alive across inputs, preserving compiled code and
/// instance-owned allocation state without making host scheduling part of the pull-operator
/// protocol.
public final class BatchFeedOperator
        implements Operator
{
    private final Schema schema;
    private final SourceOperatorIngress ingress;
    private final Set<SourceCapability> capabilities;
    private final List<PushedFilter> dynamicFilters = new ArrayList<>();

    private SourceBatch sourceBatch;
    private Batch nativeBatch;
    private Batch currentBatch;
    private boolean emitted;
    private boolean closed;

    public BatchFeedOperator(Schema schema, SourceOperatorIngress ingress)
    {
        this(schema, ingress, Set.of());
    }

    public BatchFeedOperator(Schema schema, SourceOperatorIngress ingress, Set<SourceCapability> capabilities)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.ingress = requireNonNull(ingress, "ingress is null");
        this.capabilities = Set.copyOf(requireNonNull(capabilities, "capabilities is null"));
    }

    public BatchFeedOperator(SourceBatch sourceBatch, SourceOperatorIngress ingress)
    {
        this(sourceBatch, ingress, Set.of());
    }

    public BatchFeedOperator(SourceBatch sourceBatch, SourceOperatorIngress ingress, Set<SourceCapability> capabilities)
    {
        this(requireNonNull(sourceBatch, "sourceBatch is null").schema(), ingress, capabilities);
        addInput(sourceBatch);
    }

    /// Offers the next host-owned source batch.
    ///
    /// Ownership transfers to this feed on success. The schema must match the schema supplied at
    /// construction.
    public void addInput(SourceBatch sourceBatch)
    {
        checkOpen();
        if (hasInput()) {
            throw new IllegalStateException("operator already has input");
        }
        sourceBatch = requireNonNull(sourceBatch, "sourceBatch is null");
        if (!compatibleSchema(sourceBatch.schema())) {
            throw new IllegalArgumentException("source batch schema does not match feed schema");
        }
        this.sourceBatch = sourceBatch;
        emitted = false;
    }

    /// Offers the next native batch without adapting or copying it.
    ///
    /// Ownership transfers to this feed on success. The schema supplied at construction is the
    /// contract for the native batch because [Batch] intentionally carries physical columns rather
    /// than logical type metadata.
    public void addInput(Batch batch)
    {
        checkOpen();
        if (hasInput()) {
            throw new IllegalStateException("operator already has input");
        }
        nativeBatch = requireNonNull(batch, "batch is null");
        emitted = false;
    }

    /// Completes the current host input after its adapted batch has been closed.
    public void finishInput()
    {
        checkOpen();
        if (!hasInput() || !emitted) {
            throw new IllegalStateException("operator has no consumed input");
        }
        sourceBatch = null;
        nativeBatch = null;
        currentBatch = null;
        emitted = false;
    }

    @Override
    public int outputCount()
    {
        return schema.size();
    }

    @Override
    public Schema outputSchema()
    {
        return schema;
    }

    @Override
    public boolean hasNext()
    {
        return !closed && hasInput() && !emitted;
    }

    @Override
    public Batch next()
    {
        if (!hasNext()) {
            throw new IllegalStateException("No more rows");
        }
        emitted = true;
        if (nativeBatch != null) {
            currentBatch = nativeBatch;
            return currentBatch;
        }
        try {
            currentBatch = ingress.adapt(sourceBatch);
            return currentBatch;
        }
        catch (RuntimeException | Error failure) {
            sourceBatch.close();
            sourceBatch = null;
            currentBatch = null;
            throw failure;
        }
    }

    @Override
    public void constrain(Mask mask)
    {
        requireNonNull(mask, "mask is null");
        checkOpen();
        if (currentBatch != null) {
            currentBatch.constrain(mask);
            return;
        }
        if (!hasInput()) {
            throw new IllegalStateException("operator has no input");
        }
        if (nativeBatch != null) {
            nativeBatch.constrain(mask);
            return;
        }
        sourceBatch.select(ingress.selection(mask));
    }

    @Override
    public Optional<Map<Integer, ValueDemand>> sourceOutputDemand(Map<Integer, ValueDemand> demandedOutputs)
    {
        requireNonNull(demandedOutputs, "demandedOutputs is null");
        for (int output : demandedOutputs.keySet()) {
            if (output < 0 || output >= schema.size()) {
                throw new IllegalArgumentException("demanded output is outside source schema: " + output);
            }
        }
        return Optional.of(Map.copyOf(demandedOutputs));
    }

    @Override
    public boolean supportsStableBatchBorrow()
    {
        return capabilities.contains(SourceCapability.STABLE_BATCH_BORROW);
    }

    @Override
    public boolean supportsOpenBatchHasNext()
    {
        // The host may offer another batch after this one is drained, so temporary feed depletion is not an
        // authoritative end-of-stream answer for horizon-sensitive downstream operators.
        return false;
    }

    @Override
    public boolean supportsConstrainedReborrow()
    {
        return capabilities.contains(SourceCapability.CONSTRAINED_REBORROW);
    }

    @Override
    public void pushDynamicFilter(DynamicFilter filter)
    {
        checkOpen();
        dynamicFilters.add(new PushedFilter(requireNonNull(filter, "filter is null"), null, null));
    }

    @Override
    public StaticFilterEnforcement pushStaticFilter(DynamicFilter filter)
    {
        checkOpen();
        StaticFilterEnforcement enforcement = StaticFilterEnforcement.pending();
        dynamicFilters.add(new PushedFilter(requireNonNull(filter, "filter is null"), null, enforcement));
        return enforcement;
    }

    @Override
    public StaticFilterEnforcement pushStaticFilter(StaticDomainFilter filter)
    {
        checkOpen();
        StaticFilterEnforcement enforcement = StaticFilterEnforcement.pending();
        dynamicFilters.add(new PushedFilter(null, requireNonNull(filter, "filter is null"), enforcement));
        return enforcement;
    }

    @Override
    public boolean supportsDynamicFilterPushdown(int column)
    {
        return !closed && column >= 0 && column < schema.size();
    }

    /**
     * Applies the filters retained while the host-fed pipeline was assembled to an actual native source.
     *
     * <p>Ordinary dynamic filters remain residual predicates. Exact static filters separately request semantic
     * enforcement; their completion objects are updated only when the source accepts that responsibility.
     */
    public List<RuntimeFilterAcceptance> applyDynamicFilters(BatchSource source)
    {
        checkOpen();
        requireNonNull(source, "source is null");
        if (!compatibleSchema(source.schema())) {
            throw new IllegalArgumentException("source schema does not match feed schema");
        }
        List<RuntimeFilterAcceptance> acceptances = new ArrayList<>(dynamicFilters.size());
        for (PushedFilter pushedFilter : dynamicFilters) {
            DynamicFilter filter = pushedFilter.filter();
            StaticDomainFilter domainFilter = pushedFilter.domainFilter();
            RuntimeFilterAcceptance acceptance;
            int columnIndex = filter == null ? domainFilter.column() : filter.column();
            var column = source.column(columnIndex);
            if (!source.supportsRuntimeFilter(column) ||
                    (filter != null && !ingress.supportsRuntimeFilter(source, column))) {
                acceptance = RuntimeFilterAcceptance.REJECTED;
            }
            else if (domainFilter != null) {
                acceptance = source.addRuntimeFilter(new org.weakref.nitro.core.source.RuntimeFilter(
                        column,
                        domainFilter.domain(),
                        false,
                        false));
            }
            else {
                var runtimeFilter = ingress.runtimeFilter(column, filter);
                if (pushedFilter.enforcement() != null) {
                    runtimeFilter = runtimeFilter.withoutResidual();
                }
                acceptance = source.addRuntimeFilter(runtimeFilter);
            }
            acceptances.add(acceptance);
            if (pushedFilter.enforcement() != null) {
                pushedFilter.enforcement().complete(acceptance);
            }
        }
        return List.copyOf(acceptances);
    }

    /// Applies the leaf-output demand derived by the complete operator pipeline after static-filter acceptance.
    public void applySourceOutputDemand(BatchSource source, Optional<Map<Integer, ValueDemand>> demandedOutputs)
    {
        checkOpen();
        requireNonNull(source, "source is null");
        requireNonNull(demandedOutputs, "demandedOutputs is null");
        if (demandedOutputs.isEmpty()) {
            return;
        }
        Map<Integer, ValueDemand> outputs = demandedOutputs.orElseThrow();
        source.protocol(SourceOutputDemandProtocol.OUTPUT_DEMAND)
                .ifPresent(demand -> demand.retainOutputs(outputs.entrySet().stream()
                        .collect(java.util.stream.Collectors.toUnmodifiableMap(
                                entry -> source.column(entry.getKey()),
                                Map.Entry::getValue))));
    }

    private record PushedFilter(
            DynamicFilter filter,
            StaticDomainFilter domainFilter,
            StaticFilterEnforcement enforcement)
    {
        private PushedFilter
        {
            if ((filter == null) == (domainFilter == null)) {
                throw new IllegalArgumentException("exactly one filter representation is required");
            }
        }
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        if (!emitted && sourceBatch != null) {
            sourceBatch.close();
        }
        if (!emitted && nativeBatch != null) {
            nativeBatch.close();
        }
        sourceBatch = null;
        nativeBatch = null;
        currentBatch = null;
    }

    private boolean hasInput()
    {
        return sourceBatch != null || nativeBatch != null;
    }

    private boolean compatibleSchema(Schema inputSchema)
    {
        if (inputSchema.size() != schema.size()) {
            return false;
        }
        for (int column = 0; column < schema.size(); column++) {
            if (inputSchema.field(column).nullable() != schema.field(column).nullable() ||
                    !inputSchema.field(column).type().identity().equals(schema.field(column).type().identity())) {
                return false;
            }
        }
        return true;
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("operator is closed");
        }
    }
}
