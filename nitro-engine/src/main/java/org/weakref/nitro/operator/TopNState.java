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
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

final class TopNState
{
    private final Allocator allocator;
    private final Allocator.Context allocationContext;
    private final int[] orderingColumns;
    private final boolean[] descendingByColumn;
    private final boolean[] orderingColumnFlags;
    private final StructuralComparisonKernel[] comparisonKernels;
    private Streams[][] slotColumns;
    private final Streams[] comparisonColumns;
    private final Vector[] candidateNullVectors;
    private final VectorAccess.BooleanValues[] candidateNullAccessors;
    private final boolean[] candidateNullInitialized;
    private final boolean[] candidateNullAllFalse;
    private final Vector[] candidateValueVectors;
    private final Vector[] candidateBaseValues;
    private final VectorAccess.LongValues[] candidateLongAccessors;
    private final VectorAccess.BinaryRegions[] candidateBinaryAccessors;
    private final boolean[] candidateValueInitialized;
    private final Streams[] schema;
    private final Set<Stream>[] exposedStreams;
    private final JoinBufferSupport buffers;
    private Batch[] pendingBatches;
    private int[] pendingPositions;
    private List<Integer> orderedSlots = List.of();
    private int[] primitiveOrderedSlots;
    private int primitiveOrderedSlotCount;
    private Mask outputMask;
    private Streams[] materialized;
    private Streams[] denseColumns;
    private Batch fallbackBatch;
    private Batch retainedBatch;
    private int[] retainedSourcePositions;
    private Vector[] retainedOrderingValues;
    private Vector[] retainedOrderingNulls;

    @SuppressWarnings("unchecked")
    TopNState(
            int[] orderingColumns,
            boolean[] descendingByColumn,
            JoinBufferPolicy joinBufferPolicy,
            Allocator allocator,
            Allocator.Context allocationContext,
            Schema sourceSchema,
            StructuralTypeKernelFactory structuralTypes,
            int capacity)
    {
        int outputCount = sourceSchema.size();
        this.allocator = allocator;
        this.allocationContext = allocationContext;
        this.orderingColumns = orderingColumns.clone();
        this.descendingByColumn = descendingByColumn.clone();
        this.orderingColumnFlags = new boolean[outputCount];
        for (int orderingColumn : orderingColumns) {
            orderingColumnFlags[orderingColumn] = true;
        }
        this.comparisonKernels = new StructuralComparisonKernel[outputCount];
        for (int orderingColumn : orderingColumns) {
            comparisonKernels[orderingColumn] =
                    structuralTypes.comparison(sourceSchema.field(orderingColumn).type());
        }
        this.slotColumns = new Streams[outputCount][capacity];
        this.comparisonColumns = new Streams[outputCount];
        this.candidateNullVectors = new Vector[outputCount];
        this.candidateNullAccessors = new VectorAccess.BooleanValues[outputCount];
        this.candidateNullInitialized = new boolean[outputCount];
        this.candidateNullAllFalse = new boolean[outputCount];
        this.candidateValueVectors = new Vector[outputCount];
        this.candidateBaseValues = new Vector[outputCount];
        this.candidateLongAccessors = new VectorAccess.LongValues[outputCount];
        this.candidateBinaryAccessors = new VectorAccess.BinaryRegions[outputCount];
        this.candidateValueInitialized = new boolean[outputCount];
        this.schema = new Streams[outputCount];
        this.exposedStreams = (Set<Stream>[]) new Set<?>[outputCount];
        this.buffers = new JoinBufferSupport(joinBufferPolicy, allocator, allocationContext);
        this.pendingBatches = new Batch[capacity];
        this.pendingPositions = new int[capacity];
    }

    public void beginBatch()
    {
        // Accessors over RLE streams carry monotonic run-index hints. A pooled vector can be reused
        // by a later batch whose positions start at zero, so reset the accessors at every batch
        // boundary even when the vector identity happens to be unchanged.
        Arrays.fill(candidateNullVectors, null);
        Arrays.fill(candidateNullAccessors, null);
        Arrays.fill(candidateNullInitialized, false);
        Arrays.fill(candidateNullAllFalse, false);
        Arrays.fill(candidateValueVectors, null);
        Arrays.fill(candidateBaseValues, null);
        Arrays.fill(candidateLongAccessors, null);
        Arrays.fill(candidateBinaryAccessors, null);
        Arrays.fill(candidateValueInitialized, false);
    }

    public void ensureCapacity(int requiredCapacity)
    {
        if (pendingBatches.length >= requiredCapacity) {
            return;
        }

        int newCapacity = Math.max(requiredCapacity, Math.max(8, pendingBatches.length * 2));
        for (int outputIndex = 0; outputIndex < slotColumns.length; outputIndex++) {
            slotColumns[outputIndex] = Arrays.copyOf(slotColumns[outputIndex], newCapacity);
        }
        pendingBatches = Arrays.copyOf(pendingBatches, newCapacity);
        pendingPositions = Arrays.copyOf(pendingPositions, newCapacity);
    }

    public void captureSchema(Batch batch, boolean deferSchemaBorrow)
    {
        if (fallbackBatch == null) {
            fallbackBatch = batch;
        }
        for (int outputIndex = 0; outputIndex < exposedStreams.length; outputIndex++) {
            if (exposedStreams[outputIndex] == null) {
                Set<Stream> outputStreams = batch.output(outputIndex).streams();
                exposedStreams[outputIndex] = outputStreams.isEmpty() ? Set.of() : EnumSet.copyOf(outputStreams);
            }
            // When the source can satisfy a constrained re-borrow, defer borrowing a representative
            // schema vector: borrowing a column's VALUES would force materialization of lazy
            // payloads during the build scan. The schema is then derived lazily when output is
            // requested - from the retained slot rows for non-empty results (ensureMaterializedSchema)
            // or from the fallback batch for empty results (ensureEmptySchema).
            //
            // When the source cannot be re-borrowed (e.g. a Parquet scan whose reader advances
            // irreversibly), capture the schema eagerly now while the batch is still live, since the
            // lazy fallback paths could otherwise borrow it after the reader has moved on.
            if (!deferSchemaBorrow && schema[outputIndex] == null) {
                try {
                    schema[outputIndex] = buffers.borrowStreams(batch.output(outputIndex));
                }
                catch (IllegalArgumentException ignored) {
                    // Some projected outputs can produce only null/error streams for the
                    // schema row. In that case, defer schema capture until a retained row is
                    // materialized, which is sufficient for non-empty TopN results.
                }
            }
        }
    }

    public void appendDenseBatch(Batch batch, Mask mask, int outputStart, int capacity)
    {
        captureSchema(batch, true);
        if (denseColumns == null) {
            denseColumns = new Streams[slotColumns.length];
        }
        int copied = mask.count();
        for (int outputIndex = 0; outputIndex < denseColumns.length; outputIndex++) {
            denseColumns[outputIndex] = buffers.copyAndCompact(
                    batch.output(outputIndex),
                    mask,
                    0,
                    denseColumns[outputIndex],
                    outputStart,
                    copied,
                    capacity);
            schema[outputIndex] = denseColumns[outputIndex];
        }
        if (fallbackBatch != batch) {
            releaseFallbackBatch();
        }
        fallbackBatch = null;
    }

    public void appendDenseOrderingBatch(Batch batch, Mask mask, int outputStart, int capacity)
    {
        captureSchema(batch, true);
        if (denseColumns == null) {
            denseColumns = new Streams[slotColumns.length];
        }
        int copied = mask.count();
        for (int orderingColumn : orderingColumns) {
            denseColumns[orderingColumn] = buffers.copyAndCompact(
                    batch.output(orderingColumn),
                    mask,
                    0,
                    denseColumns[orderingColumn],
                    outputStart,
                    copied,
                    capacity);
            schema[orderingColumn] = denseColumns[orderingColumn];
        }
    }

    public boolean supportsDenseOrdering(Batch batch)
    {
        for (int orderingColumn : orderingColumns) {
            Vector values = OperatorVectorSupport.flatten(batch.output(orderingColumn).borrow(Stream.VALUES));
            if (!(values instanceof I64Vector || values instanceof I32Vector || values instanceof F64Vector)) {
                return false;
            }
        }
        return true;
    }

    public int compareOrderingValue(Batch batch, int position, int slot, boolean compactCandidate)
    {
        for (int orderingIndex = 0; orderingIndex < orderingColumns.length; orderingIndex++) {
            int orderingColumn = orderingColumns[orderingIndex];
            Output output = batch.output(orderingColumn);
            Streams slotOrdering = denseColumns == null ? slotColumns[orderingColumn][slot] : denseColumns[orderingColumn];
            int slotPosition = denseColumns == null ? 0 : slot;
            Output.PositionAccessor positionAccessor = compactCandidate ? output.positionAccessor() : null;
            Streams currentOrdering = null;
            if (compactCandidate && positionAccessor == null) {
                // Prefer the producer's compact single-position path for very large lazy results.
                // Borrowing VALUES here would materialize every group merely to compare one candidate.
                comparisonColumns[orderingColumn] = buffers.copyPosition(output, comparisonColumns[orderingColumn], position);
                currentOrdering = comparisonColumns[orderingColumn];
            }
            // NULLS LAST regardless of sort direction (matches Trino/SQL default); direction flips only the
            // comparison of non-null values.
            boolean currentNull = compactCandidate
                    ? positionAccessor == null
                            ? OperatorVectorSupport.isNull(currentOrdering.getOrNull(Stream.NULLS), 0)
                            : positionAccessor.isNull(position)
                    : candidateIsNull(orderingColumn, output, position);
            boolean slotNull = OperatorVectorSupport.isNull(slotOrdering.getOrNull(Stream.NULLS), slotPosition);
            int comparison;
            if (currentNull || slotNull) {
                if (currentNull == slotNull) {
                    continue;
                }
                return currentNull ? -1 : 1;
            }
            if (compactCandidate && positionAccessor != null) {
                comparison = positionAccessor.compareNonNull(position, slotOrdering.values(), slotPosition);
            }
            else if (compactCandidate) {
                comparison = comparisonKernels[orderingColumn].compare(
                        currentOrdering.values(),
                        currentOrdering.getOrNull(Stream.NULLS),
                        0,
                        slotOrdering.values(),
                        slotOrdering.getOrNull(Stream.NULLS),
                        slotPosition);
            }
            else {
                comparison = comparisonKernels[orderingColumn].allowsLegacyPhysicalShortcuts()
                        ? tryCompareDirectOrderingValue(orderingColumn, output, position, slotOrdering, slotPosition)
                        : Integer.MIN_VALUE;
                if (comparison == Integer.MIN_VALUE) {
                    comparisonColumns[orderingColumn] = buffers.copyPosition(output, comparisonColumns[orderingColumn], position);
                    currentOrdering = comparisonColumns[orderingColumn];
                    comparison = comparisonKernels[orderingColumn].compare(
                            currentOrdering.values(),
                            currentOrdering.getOrNull(Stream.NULLS),
                            0,
                            slotOrdering.values(),
                            slotOrdering.getOrNull(Stream.NULLS),
                            slotPosition);
                }
            }
            comparison = descendingByColumn[orderingIndex] ? comparison : -comparison;
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    public void prepareRetainedBatchOrdering(Batch batch)
    {
        retainedOrderingValues = new Vector[slotColumns.length];
        retainedOrderingNulls = new Vector[slotColumns.length];
        for (int orderingColumn : orderingColumns) {
            Output output = batch.output(orderingColumn);
            retainedOrderingValues[orderingColumn] = output.borrow(Stream.VALUES);
            retainedOrderingNulls[orderingColumn] = output.borrowOrNull(Stream.NULLS);
        }
    }

    public int compareRetainedBatchPositions(int leftPosition, int rightPosition)
    {
        for (int orderingIndex = 0; orderingIndex < orderingColumns.length; orderingIndex++) {
            int orderingColumn = orderingColumns[orderingIndex];
            Vector nulls = retainedOrderingNulls[orderingColumn];
            boolean leftNull = OperatorVectorSupport.isNull(nulls, leftPosition);
            boolean rightNull = OperatorVectorSupport.isNull(nulls, rightPosition);
            if (leftNull || rightNull) {
                if (leftNull == rightNull) {
                    continue;
                }
                return leftNull ? -1 : 1;
            }
            Vector values = retainedOrderingValues[orderingColumn];
            int comparison = comparisonKernels[orderingColumn].compare(
                    values,
                    nulls,
                    leftPosition,
                    values,
                    nulls,
                    rightPosition);
            comparison = descendingByColumn[orderingIndex] ? comparison : -comparison;
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    private boolean candidateIsNull(int orderingColumn, Output output, int position)
    {
        if (!candidateNullInitialized[orderingColumn]) {
            Vector nulls = output.borrowOrNull(Stream.NULLS);
            candidateNullVectors[orderingColumn] = nulls;
            candidateNullAllFalse[orderingColumn] = VectorAccess.isAllFalseNulls(nulls);
            if (!candidateNullAllFalse[orderingColumn]) {
                candidateNullAccessors[orderingColumn] = VectorAccess.booleanValues(nulls);
            }
            candidateNullInitialized[orderingColumn] = true;
        }
        Vector nulls = candidateNullVectors[orderingColumn];
        if (nulls == null || candidateNullAllFalse[orderingColumn]) {
            return false;
        }
        return candidateNullAccessors[orderingColumn].value(position);
    }

    private int tryCompareDirectOrderingValue(int orderingColumn, Output output, int position, Streams slotOrdering, int slotPosition)
    {
        if (!candidateValueInitialized[orderingColumn]) {
            candidateValueVectors[orderingColumn] = output.borrow(Stream.VALUES);
            candidateBaseValues[orderingColumn] = OperatorVectorSupport.flatten(candidateValueVectors[orderingColumn]);
            candidateValueInitialized[orderingColumn] = true;
        }
        Vector currentValues = candidateValueVectors[orderingColumn];
        Vector slotValues = slotOrdering.values();

        Vector flattenedCurrent = candidateBaseValues[orderingColumn];
        Vector flattenedSlot = OperatorVectorSupport.flatten(slotValues);
        if ((flattenedCurrent instanceof I64Vector || flattenedCurrent instanceof I32Vector) &&
                (flattenedSlot instanceof I64Vector || flattenedSlot instanceof I32Vector)) {
            if (candidateLongAccessors[orderingColumn] == null) {
                candidateLongAccessors[orderingColumn] = VectorAccess.longValues(currentValues);
                candidateBinaryAccessors[orderingColumn] = null;
            }
            return Long.compare(
                    candidateLongAccessors[orderingColumn].value(position),
                    OperatorVectorSupport.longValue(slotValues, slotPosition));
        }
        if (flattenedCurrent instanceof BinaryVector && flattenedSlot instanceof BinaryVector) {
            if (candidateBinaryAccessors[orderingColumn] == null) {
                candidateBinaryAccessors[orderingColumn] = VectorAccess.binaryRegions(currentValues);
                candidateLongAccessors[orderingColumn] = null;
            }
            VectorAccess.BinaryRegions current = candidateBinaryAccessors[orderingColumn];
            byte[] data = current.data(position);
            int offset = current.offset(position);
            int length = current.length(position);
            return -OperatorVectorSupport.binaryCompare(slotValues, slotPosition, data, offset, length);
        }
        return Integer.MIN_VALUE;
    }

    public int compareSlots(int leftSlot, int rightSlot)
    {
        for (int orderingIndex = 0; orderingIndex < orderingColumns.length; orderingIndex++) {
            int orderingColumn = orderingColumns[orderingIndex];
            if (denseColumns != null) {
                Streams ordering = denseColumns[orderingColumn];
                Vector nulls = ordering.getOrNull(Stream.NULLS);
                boolean leftNull = OperatorVectorSupport.isNull(nulls, leftSlot);
                boolean rightNull = OperatorVectorSupport.isNull(nulls, rightSlot);
                if (leftNull || rightNull) {
                    if (leftNull == rightNull) {
                        continue;
                    }
                    return leftNull ? -1 : 1;
                }
                int comparison = comparisonKernels[orderingColumn].compare(
                        ordering.values(),
                        nulls,
                        leftSlot,
                        ordering.values(),
                        nulls,
                        rightSlot);
                comparison = descendingByColumn[orderingIndex] ? comparison : -comparison;
                if (comparison != 0) {
                    return comparison;
                }
                continue;
            }
            Streams leftOrdering = slotColumns[orderingColumn][leftSlot];
            Streams rightOrdering = slotColumns[orderingColumn][rightSlot];
            // NULLS LAST regardless of sort direction (matches Trino/SQL); direction flips only non-null values.
            boolean leftNull = OperatorVectorSupport.isNull(leftOrdering.getOrNull(Stream.NULLS), 0);
            boolean rightNull = OperatorVectorSupport.isNull(rightOrdering.getOrNull(Stream.NULLS), 0);
            if (leftNull || rightNull) {
                if (leftNull == rightNull) {
                    continue;
                }
                return leftNull ? -1 : 1;
            }
            int comparison = comparisonKernels[orderingColumn].compare(
                    leftOrdering.values(),
                    leftOrdering.getOrNull(Stream.NULLS),
                    0,
                    rightOrdering.values(),
                    rightOrdering.getOrNull(Stream.NULLS),
                    0);
            comparison = descendingByColumn[orderingIndex] ? comparison : -comparison;
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    public boolean hasSingleFixedWidthNonNullOrdering(int slotCount)
    {
        if (orderingColumns.length != 1) {
            return false;
        }
        int orderingColumn = orderingColumns[0];
        if (!comparisonKernels[orderingColumn].allowsLegacyPhysicalShortcuts()) {
            return false;
        }
        if (denseColumns != null) {
            Streams ordering = denseColumns[orderingColumn];
            if (!VectorAccess.isAllFalseNulls(ordering.getOrNull(Stream.NULLS))) {
                return false;
            }
            Vector values = OperatorVectorSupport.flatten(ordering.values());
            return values instanceof I64Vector || values instanceof I32Vector || values instanceof F64Vector;
        }
        for (int slot = 0; slot < slotCount; slot++) {
            Streams ordering = slotColumns[orderingColumn][slot];
            if (OperatorVectorSupport.isNull(ordering.getOrNull(Stream.NULLS), 0)) {
                return false;
            }
            Vector values = OperatorVectorSupport.flatten(ordering.values());
            if (!(values instanceof I64Vector || values instanceof I32Vector || values instanceof F64Vector)) {
                return false;
            }
        }
        return true;
    }

    public long singleFixedWidthSortKey(int slot)
    {
        Vector values = denseColumns == null
                ? slotColumns[orderingColumns[0]][slot].values()
                : denseColumns[orderingColumns[0]].values();
        int position = denseColumns == null ? 0 : slot;
        return switch (OperatorVectorSupport.flatten(values)) {
            case I64Vector ignored -> OperatorVectorSupport.longValue(values, position) ^ Long.MIN_VALUE;
            case I32Vector ignored -> (OperatorVectorSupport.longValue(values, position) ^ Integer.MIN_VALUE) & 0xFFFF_FFFFL;
            case F64Vector ignored -> {
                long bits = Double.doubleToLongBits(OperatorVectorSupport.doubleValue(values, position));
                yield bits < 0 ? ~bits : bits ^ Long.MIN_VALUE;
            }
            default -> throw new IllegalStateException("Ordering value is not fixed width");
        };
    }

    public boolean singleOrderingDescending()
    {
        return descendingByColumn[0];
    }

    public void copyRow(Batch batch, int position, int slot)
    {
        for (int orderingColumn : orderingColumns) {
            slotColumns[orderingColumn][slot] = buffers.copyPosition(batch.output(orderingColumn), slotColumns[orderingColumn][slot], position);
        }
        for (int outputIndex = 0; outputIndex < slotColumns.length; outputIndex++) {
            if (isOrderingColumn(outputIndex)) {
                continue;
            }
            slotColumns[outputIndex][slot] = null;
        }
        pendingBatches[slot] = batch;
        pendingPositions[slot] = position;
    }

    public void copyDenseOrderingRow(Batch batch, int position, int slot, int capacity)
    {
        if (denseColumns == null) {
            throw new IllegalStateException("dense TopN columns are not initialized");
        }
        for (int orderingColumn : orderingColumns) {
            denseColumns[orderingColumn] = buffers.copySinglePosition(
                    batch.output(orderingColumn),
                    denseColumns[orderingColumn],
                    capacity,
                    slot,
                    position);
            schema[orderingColumn] = denseColumns[orderingColumn];
        }
    }

    public void copyPayloadRow(Batch batch, int position, int slot)
    {
        for (int outputIndex = 0; outputIndex < slotColumns.length; outputIndex++) {
            if (isOrderingColumn(outputIndex)) {
                continue;
            }
            slotColumns[outputIndex][slot] = buffers.copyPosition(
                    batch.output(outputIndex),
                    slotColumns[outputIndex][slot],
                    position);
            schema[outputIndex] = slotColumns[outputIndex][slot];
        }
    }

    public void deferPayloadRow(Batch batch, int position, int slot)
    {
        for (int outputIndex = 0; outputIndex < slotColumns.length; outputIndex++) {
            if (!isOrderingColumn(outputIndex)) {
                slotColumns[outputIndex][slot] = null;
            }
        }
        pendingBatches[slot] = batch;
        pendingPositions[slot] = position;
    }

    public void flushPendingBatch(Batch batch, List<Integer> retainedSlots)
    {
        int retained = 0;
        int[] retainedPositions = new int[Math.min(retainedSlots.size(), batch.borrowMask().count())];
        for (int slot : retainedSlots) {
            if (pendingBatches[slot] == batch) {
                retainedPositions[retained++] = pendingPositions[slot];
            }
        }
        if (retained > 0 && retained < batch.borrowMask().count()) {
            Arrays.sort(retainedPositions, 0, retained);
            batch.constrain(allocator.allocateSparseMask(
                    allocationContext,
                    Arrays.copyOf(retainedPositions, retained),
                    batch.borrowMask().size()));
        }
        for (int slot : retainedSlots) {
            if (pendingBatches[slot] != batch) {
                continue;
            }
            for (int outputIndex = 0; outputIndex < slotColumns.length; outputIndex++) {
                if (isOrderingColumn(outputIndex)) {
                    continue;
                }
                slotColumns[outputIndex][slot] = buffers.copyPosition(batch.output(outputIndex), slotColumns[outputIndex][slot], pendingPositions[slot]);
                if (schema[outputIndex] == null) {
                    schema[outputIndex] = slotColumns[outputIndex][slot];
                }
            }
            pendingBatches[slot] = null;
        }
    }

    public void flushPendingBatch(Batch batch, int[] retainedSlots, int start, int end)
    {
        for (int index = start; index < end; index++) {
            int slot = retainedSlots[index];
            if (pendingBatches[slot] != batch) {
                continue;
            }
            for (int outputIndex = 0; outputIndex < slotColumns.length; outputIndex++) {
                if (isOrderingColumn(outputIndex)) {
                    continue;
                }
                slotColumns[outputIndex][slot] = buffers.copyPosition(batch.output(outputIndex), slotColumns[outputIndex][slot], pendingPositions[slot]);
                if (schema[outputIndex] == null) {
                    schema[outputIndex] = slotColumns[outputIndex][slot];
                }
            }
            pendingBatches[slot] = null;
        }
    }

    public void setOrderedSlots(List<Integer> orderedSlots)
    {
        this.orderedSlots = orderedSlots;
        this.primitiveOrderedSlots = null;
        this.primitiveOrderedSlotCount = 0;
        this.outputMask = allocator.allocateAllMask(allocationContext, orderedSlots.size());
        this.materialized = new Streams[schema.length];
    }

    public void setOrderedSlots(int[] orderedSlots, int count)
    {
        this.orderedSlots = List.of();
        this.primitiveOrderedSlots = orderedSlots;
        this.primitiveOrderedSlotCount = count;
        this.outputMask = allocator.allocateAllMask(allocationContext, count);
        this.materialized = new Streams[schema.length];
    }

    public void setRetainedBatchPositions(Batch batch, int[] sourcePositions)
    {
        retainedBatch = requireNonNull(batch, "batch is null");
        retainedSourcePositions = requireNonNull(sourcePositions, "sourcePositions is null");
        // Comparisons are complete. Constraining the batch may invalidate and release the
        // ordering vectors, so do not retain stale references to them during output.
        retainedOrderingValues = null;
        retainedOrderingNulls = null;
        int[] constrainedPositions = sourcePositions.clone();
        Arrays.sort(constrainedPositions);
        batch.constrain(allocator.allocateSparseMask(
                allocationContext,
                constrainedPositions,
                batch.borrowMask().size()));
        orderedSlots = List.of();
        primitiveOrderedSlots = null;
        primitiveOrderedSlotCount = 0;
        outputMask = allocator.allocateAllMask(allocationContext, sourcePositions.length);
        materialized = new Streams[schema.length];
    }

    public void constrain(Mask mask)
    {
        outputMask = mask;
        materialized = new Streams[schema.length];
    }

    public Streams output(int index)
    {
        Streams output = materialized[index];
        if (output != null) {
            return output;
        }

        if (retainedBatch != null) {
            output = materializeRetainedBatchColumn(index);
        }
        else if (orderedSlotCount() == 0) {
            Streams columnSchema = ensureEmptySchema(index);
            output = buffers.emptyLike(columnSchema);
        }
        else if (denseColumns != null && denseColumns[index] != null) {
            output = materializeDenseSortedColumn(index);
        }
        else {
            ensurePendingOutputMaterialized(index);
            Streams columnSchema = ensureMaterializedSchema(index);
            output = materializeColumn(columnSchema, index);
        }
        materialized[index] = output;
        return output;
    }

    private Streams materializeRetainedBatchColumn(int outputIndex)
    {
        Output input = retainedBatch.output(outputIndex);
        if (outputMask.all()) {
            Streams specialized = input.copyPositions(
                    null,
                    retainedSourcePositions,
                    0,
                    retainedSourcePositions.length,
                    0,
                    retainedSourcePositions.length,
                    true);
            if (specialized != null) {
                return specialized;
            }
            return buffers.copyPositions(
                    input,
                    null,
                    retainedSourcePositions,
                    retainedSourcePositions.length,
                    0,
                    retainedSourcePositions.length);
        }
        Streams result = null;
        for (int outputPosition : outputMask) {
            result = buffers.copySinglePosition(
                    input,
                    result,
                    outputMask.size(),
                    outputPosition,
                    retainedSourcePositions[outputPosition]);
        }
        return result == null ? buffers.emptyLike(buffers.borrowStreams(input)) : result;
    }

    public Set<Stream> outputStreams(int index)
    {
        Set<Stream> streams = exposedStreams[index];
        if (streams == null) {
            throw new IllegalStateException("TopN did not observe source output streams");
        }
        return streams;
    }

    public void releaseFallbackBatch()
    {
        if (fallbackBatch != null) {
            fallbackBatch.close();
            fallbackBatch = null;
        }
    }

    public void discardFallbackBatch()
    {
        fallbackBatch = null;
    }

    public boolean shouldKeepBatchForEmptySchema(Batch batch, boolean queueEmpty)
    {
        return queueEmpty && batch == fallbackBatch && hasMissingSchema();
    }

    private boolean hasMissingSchema()
    {
        for (Streams streams : schema) {
            if (streams == null) {
                return true;
            }
        }
        return false;
    }

    private void ensurePendingOutputMaterialized(int outputIndex)
    {
        if (isOrderingColumn(outputIndex)) {
            return;
        }
        constrainPendingBatches();
        for (int index = 0; index < orderedSlotCount(); index++) {
            int slot = orderedSlot(index);
            Batch batch = pendingBatches[slot];
            if (batch == null) {
                continue;
            }
            slotColumns[outputIndex][slot] = buffers.copyPosition(batch.output(outputIndex), slotColumns[outputIndex][slot], pendingPositions[slot]);
            if (schema[outputIndex] == null) {
                schema[outputIndex] = slotColumns[outputIndex][slot];
            }
        }
    }

    private void constrainPendingBatches()
    {
        IdentityHashMap<Batch, List<Integer>> retainedPositionsByBatch = new IdentityHashMap<>();
        for (int index = 0; index < orderedSlotCount(); index++) {
            int slot = orderedSlot(index);
            Batch batch = pendingBatches[slot];
            if (batch == null) {
                continue;
            }
            retainedPositionsByBatch.computeIfAbsent(batch, _ -> new ArrayList<>())
                    .add(pendingPositions[slot]);
        }
        for (Map.Entry<Batch, List<Integer>> entry : retainedPositionsByBatch.entrySet()) {
            int[] positions = entry.getValue().stream()
                    .mapToInt(Integer::intValue)
                    .sorted()
                    .toArray();
            entry.getKey().constrain(allocator.allocateSparseMask(allocationContext, positions, entry.getKey().borrowMask().size()));
        }
    }

    private Streams ensureEmptySchema(int outputIndex)
    {
        Streams columnSchema = schema[outputIndex];
        if (columnSchema != null) {
            return columnSchema;
        }
        if (fallbackBatch != null) {
            schema[outputIndex] = buffers.borrowStreams(fallbackBatch.output(outputIndex));
            return schema[outputIndex];
        }
        throw new IllegalStateException("TopN did not observe source output schema");
    }

    private Streams ensureMaterializedSchema(int outputIndex)
    {
        Streams columnSchema = schema[outputIndex];
        if (columnSchema != null) {
            return columnSchema;
        }
        int firstOutputPosition = outputMask == null || outputMask.none() ? 0 : outputMask.position(0);
        columnSchema = slotColumns[outputIndex][orderedSlot(firstOutputPosition)];
        if (columnSchema == null) {
            throw new IllegalStateException("TopN output column was not materialized: " + outputIndex);
        }
        schema[outputIndex] = columnSchema;
        return columnSchema;
    }

    private Streams materializeColumn(Streams columnSchema, int outputIndex)
    {
        if (outputMask == null || outputMask.all()) {
            return materializeDenseColumn(columnSchema, outputIndex);
        }

        Streams result = null;
        for (int outputPosition : outputMask) {
            int slot = orderedSlot(outputPosition);
            result = buffers.copySinglePosition(
                    result,
                    slotColumns[outputIndex][slot],
                    outputMask.size(),
                    outputPosition,
                    0);
        }
        return result == null ? buffers.emptyLike(columnSchema) : result;
    }

    private Streams materializeDenseSortedColumn(int outputIndex)
    {
        Streams.Builder result = Streams.builder();
        for (Map.Entry<Stream, Vector> entry : denseColumns[outputIndex].asMap().entrySet()) {
            result.put(entry.getKey(), DictionaryVector.wrap(
                    primitiveOrderedSlots,
                    primitiveOrderedSlotCount,
                    entry.getValue()));
        }
        return result.build();
    }

    private Streams materializeDenseColumn(Streams columnSchema, int outputIndex)
    {
        Streams.Builder result = Streams.builder();
        for (Map.Entry<Stream, Vector> entry : columnSchema.asMap().entrySet()) {
            Vector[] rows = new Vector[orderedSlotCount()];
            for (int rowIndex = 0; rowIndex < orderedSlotCount(); rowIndex++) {
                rows[rowIndex] = slotColumns[outputIndex][orderedSlot(rowIndex)].get(entry.getKey());
            }
            result.put(entry.getKey(), buffers.materializeStream(entry.getValue(), rows));
        }
        return result.build();
    }

    private int orderedSlotCount()
    {
        return primitiveOrderedSlots == null ? orderedSlots.size() : primitiveOrderedSlotCount;
    }

    private int orderedSlot(int index)
    {
        return primitiveOrderedSlots == null ? orderedSlots.get(index) : primitiveOrderedSlots[index];
    }

    private boolean isOrderingColumn(int outputIndex)
    {
        return orderingColumnFlags[outputIndex];
    }
}
