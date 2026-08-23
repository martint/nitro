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
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.operator.aggregation.PhysicalAggregationProgram;

import java.util.Arrays;
import java.util.List;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * A streaming grouping session for a physical aggregation with keys and no aggregate state.
 *
 * <p>The session applies SQL grouping null semantics, retaining one representative for each
 * null-bearing key. Distinct rows are copied into owned output batches, so the caller remains free
 * to close every input immediately after {@link #addInput(Batch)}.
 */
public final class KeyOnlyGroupingSession
        implements BatchAggregationSession
{
    private static final int[] EMPTY_POSITIONS = new int[0];

    private final Allocator allocator;
    private final Allocator.Context allocationContext;
    private final OperatorResources operatorResources;
    private final int[] groupByColumns;
    private final List<Integer> groupByColumnList;
    private final List<TypeBinding> keyTypes;
    private final Vector[] values;
    private final Vector[] nulls;
    private final InitialAggregationBatchBuilder outputBuilder;
    private final DistinctKeySetPolicy distinctKeySetPolicy;
    private final PartialAggregationControl partialAggregationControl;
    private final MutableAggregationPhaseMetrics phaseMetrics = new MutableAggregationPhaseMetrics();

    private DistinctKeySet distinctKeySet;
    private int[] distinctPositions = EMPTY_POSITIONS;
    private Batch pendingOutput;
    private long aggregatedInputBytes;
    private long aggregatedInputRows;
    private long aggregatedOutputRows;
    private boolean finished;
    private boolean closed;

    public KeyOnlyGroupingSession(
            Allocator allocator,
            Schema inputSchema,
            List<Integer> groupByColumns,
            List<Integer> groupedColumns,
            OperatorResources operatorResources)
    {
        this(allocator, inputSchema, groupByColumns, groupedColumns, operatorResources, null);
    }

    public KeyOnlyGroupingSession(
            Allocator allocator,
            Schema inputSchema,
            List<Integer> groupByColumns,
            List<Integer> groupedColumns,
            OperatorResources operatorResources,
            PartialAggregationControl partialAggregationControl)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        requireNonNull(inputSchema, "inputSchema is null");
        this.operatorResources = requireNonNull(operatorResources, "operatorResources is null");
        this.groupByColumnList = List.copyOf(requireNonNull(groupByColumns, "groupByColumns is null"));
        this.groupByColumns = this.groupByColumnList.stream()
                .mapToInt(Integer::intValue)
                .toArray();
        if (this.groupByColumns.length == 0) {
            throw new IllegalArgumentException("groupByColumns is empty");
        }
        this.keyTypes = Arrays.stream(this.groupByColumns)
                .mapToObj(column -> inputSchema.field(column).type())
                .toList();
        this.values = new Vector[this.groupByColumns.length];
        this.nulls = new Vector[this.groupByColumns.length];
        this.distinctKeySetPolicy = operatorResources.distinctKeySetPolicy();
        this.partialAggregationControl = partialAggregationControl;
        this.allocationContext = new Allocator.Context(
                "KeyOnlyGroupingSession",
                operatorResources.grouping().markDistinctMaskPool());
        this.outputBuilder = new InitialAggregationBatchBuilder(
                allocator,
                inputSchema,
                groupedColumns,
                new PhysicalAggregationProgram(List.of(), List.of()),
                operatorResources);
    }

    @Override
    public Schema outputSchema()
    {
        return outputBuilder.outputSchema();
    }

    @Override
    public void addInput(Batch batch)
    {
        addInput(batch, 0, false);
    }

    @Override
    public void addInput(Batch batch, long inputBytes)
    {
        addInput(batch, inputBytes, false);
    }

    @Override
    public InputOwnership addInputWithOwnership(Batch batch, long inputBytes)
    {
        return addInput(batch, inputBytes, true);
    }

    private InputOwnership addInput(Batch batch, long inputBytes, boolean mayRetainInput)
    {
        checkAcceptingInput();
        requireNonNull(batch, "batch is null");
        if (inputBytes < 0) {
            throw new IllegalArgumentException("inputBytes is negative");
        }
        Mask inputMask = batch.borrowMask();
        if (inputMask.none()) {
            return InputOwnership.CALLER;
        }
        if (partialAggregationControl != null) {
            boolean aggregationEnabled = partialAggregationControl.aggregationEnabled();
            int sampleSize = partialAggregationControl.inputCardinalitySampleSize();
            if (sampleSize > 0) {
                PartialAggregationInputStatistics inputStatistics = GroupingCardinalitySampler.sample(
                        batch,
                        groupByColumnList,
                        sampleSize,
                        allocator.primitiveArrays(),
                        false);
                if (inputStatistics.sampledRows() > 0) {
                    aggregationEnabled = partialAggregationControl.aggregationEnabled(inputStatistics);
                }
            }
            if (!aggregationEnabled) {
                pendingOutput = mayRetainInput ? outputBuilder.buildRetaining(batch) : null;
                InputOwnership ownership = pendingOutput == null ? InputOwnership.CALLER : InputOwnership.SESSION;
                if (pendingOutput == null) {
                    pendingOutput = outputBuilder.build(batch, inputMask);
                }
                partialAggregationControl.onPassthroughFlush(inputBytes, inputMask.count());
                return ownership;
            }
        }
        int selectedCount;
        Mask candidateMask = inputMask;
        try {
            for (int key = 0; key < groupByColumns.length; key++) {
                Output output = batch.output(groupByColumns[key]);
                values[key] = output.borrow(Stream.VALUES);
                nulls[key] = output.borrowOrNull(Stream.NULLS);
            }
            if (distinctKeySet == null) {
                distinctKeySet = DistinctKeySet.create(
                        values,
                        true,
                        keyTypes,
                        allocator,
                        allocationContext,
                        allocator.primitiveArrays(),
                        operatorResources.codeGeneration(),
                        distinctKeySetPolicy,
                        operatorResources.adaptiveLongGroupingPolicy(),
                        operatorResources.flatKeyTablePolicy());
            }
            candidateMask = dictionaryDomainCandidates(inputMask);
            ensureDistinctPositionCapacity(candidateMask.selectedCount());
            distinctKeySet.reserveAdditional(candidateMask.selectedCount());
            long start = System.nanoTime();
            try {
                selectedCount = distinctKeySet.addBatch(values, nulls, candidateMask, distinctPositions);
            }
            finally {
                phaseMetrics.recordGrouping(System.nanoTime() - start);
            }
        }
        finally {
            if (candidateMask != inputMask) {
                allocator.release(allocationContext, candidateMask);
            }
            Arrays.fill(values, null);
            Arrays.fill(nulls, null);
        }
        if (partialAggregationControl != null) {
            aggregatedInputBytes = Math.addExact(aggregatedInputBytes, inputBytes);
            aggregatedInputRows = Math.addExact(aggregatedInputRows, inputMask.count());
            aggregatedOutputRows = Math.addExact(aggregatedOutputRows, selectedCount);
        }
        if (selectedCount == 0) {
            return InputOwnership.CALLER;
        }
        if (selectedCount == inputMask.selectedCount()) {
            pendingOutput = mayRetainInput ? outputBuilder.buildRetaining(batch) : null;
            if (pendingOutput != null) {
                return InputOwnership.SESSION;
            }
            pendingOutput = outputBuilder.build(batch, inputMask);
            return InputOwnership.CALLER;
        }
        Mask distinctMask = allocator.allocateSparseMask(
                allocationContext,
                distinctPositions,
                selectedCount,
                inputMask.size());
        try {
            boolean retainInput = mayRetainInput &&
                    (long) selectedCount * 100 >=
                            (long) inputMask.selectedCount() * distinctKeySetPolicy.keyOnlySparseRetentionMinPercent();
            pendingOutput = retainInput ? outputBuilder.buildRetaining(batch, distinctMask) : null;
            if (pendingOutput != null) {
                return InputOwnership.SESSION;
            }
            pendingOutput = outputBuilder.build(batch, distinctMask);
        }
        finally {
            allocator.release(allocationContext, distinctMask);
        }
        return InputOwnership.CALLER;
    }

    /**
     * Reduces a low-cardinality dictionary key to logical representatives before hashing. The representative remains
     * a position in the original batch, so arbitrary key types, nested dictionaries, output copying, and SQL null
     * semantics continue through the ordinary distinct machinery. A nullable dictionary entry may contribute both a
     * null and a non-null representative; no relationship between the values and null encodings is assumed.
     */
    private Mask dictionaryDomainCandidates(Mask inputMask)
    {
        if (!distinctKeySetPolicy.keyOnlyDictionaryDomain() ||
                groupByColumns.length != 1 ||
                !(values[0] instanceof DictionaryVector dictionary)) {
            return inputMask;
        }

        int domainSize = dictionary.values().length();
        boolean singleNullState = VectorAccess.isAllFalseNulls(nulls[0]) || VectorAccess.isAllTrueNulls(nulls[0]);
        long maximumCandidates = (long) domainSize * (singleNullState ? 1 : 2);
        if (maximumCandidates * distinctKeySetPolicy.keyOnlyDictionaryDomainMinimumReduction() > inputMask.selectedCount()) {
            return inputMask;
        }

        ensureDistinctPositionCapacity(toIntExact(maximumCandidates));
        byte[] seen = allocator.primitiveArrays().borrowBytes(domainSize);
        Arrays.fill(seen, 0, domainSize, (byte) 0);
        VectorAccess.BooleanValues nullValues = singleNullState ? null : VectorAccess.booleanValues(nulls[0]);
        int[] ids = dictionary.ids();
        int candidateCount = 0;
        try {
            for (int position : inputMask) {
                int id = ids[position];
                byte state = nullValues != null && nullValues.value(position) ? (byte) 2 : (byte) 1;
                if ((seen[id] & state) == 0) {
                    seen[id] |= state;
                    distinctPositions[candidateCount++] = position;
                    if (candidateCount == maximumCandidates) {
                        break;
                    }
                }
            }
            return allocator.allocateSparseMask(allocationContext, distinctPositions, candidateCount, inputMask.size());
        }
        finally {
            allocator.primitiveArrays().release(seen);
        }
    }

    private void ensureDistinctPositionCapacity(int capacity)
    {
        if (distinctPositions.length >= capacity) {
            return;
        }
        int[] previous = distinctPositions;
        distinctPositions = allocator.primitiveArrays().borrowInts(capacity);
        allocator.primitiveArrays().release(previous);
    }

    @Override
    public boolean hasOutput()
    {
        return pendingOutput != null;
    }

    @Override
    public Batch getOutput()
    {
        checkOpen();
        if (pendingOutput == null) {
            throw new IllegalStateException("key-only grouping session has no output");
        }
        Batch output = pendingOutput;
        pendingOutput = null;
        return output;
    }

    @Override
    public long retainedBytes()
    {
        return distinctKeySet == null ? 0 : distinctKeySet.retainedBytes();
    }

    @Override
    public AggregationPhaseMetrics phaseMetrics()
    {
        return phaseMetrics.snapshot();
    }

    @Override
    public void flush()
    {
        checkAcceptingInput();
        if (partialAggregationControl == null) {
            throw new UnsupportedOperationException("key-only grouping session is not adaptive");
        }
        finishAggregatedCohort();
    }

    @Override
    public Batch finish()
    {
        checkOpen();
        if (finished) {
            throw new IllegalStateException("key-only grouping session is already finished");
        }
        if (pendingOutput != null) {
            throw new IllegalStateException("key-only grouping session has pending output");
        }
        finished = true;
        finishAggregatedCohort();
        return outputBuilder.empty();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        if (pendingOutput != null) {
            pendingOutput.close();
            pendingOutput = null;
        }
        if (distinctKeySet != null) {
            distinctKeySet.releaseBuffers();
            distinctKeySet = null;
        }
        allocator.primitiveArrays().release(distinctPositions);
        distinctPositions = EMPTY_POSITIONS;
        Arrays.fill(values, null);
        Arrays.fill(nulls, null);
        allocator.release(allocationContext);
    }

    private void checkAcceptingInput()
    {
        checkOpen();
        if (finished) {
            throw new IllegalStateException("key-only grouping session is finished");
        }
        if (pendingOutput != null) {
            throw new IllegalStateException("key-only grouping session has pending output");
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("key-only grouping session is closed");
        }
    }

    private void finishAggregatedCohort()
    {
        if (partialAggregationControl == null || aggregatedInputRows == 0) {
            return;
        }
        partialAggregationControl.onAggregatedFlush(
                aggregatedInputBytes,
                aggregatedInputRows,
                aggregatedOutputRows);
        aggregatedInputBytes = 0;
        aggregatedInputRows = 0;
        aggregatedOutputRows = 0;
        if (distinctKeySet != null) {
            distinctKeySet.releaseBuffers();
            distinctKeySet = null;
        }
        allocator.releasePooledMemory(allocationContext);
    }
}
