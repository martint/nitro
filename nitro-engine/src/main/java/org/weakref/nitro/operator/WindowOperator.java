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

import org.weakref.nitro.core.execution.ExecutionDiagnostics;
import org.weakref.nitro.core.function.aggregation.AggregationWindowFrameBounds;
import org.weakref.nitro.core.function.aggregation.PrimitiveRangeConsumer;
import org.weakref.nitro.core.function.aggregation.PrimitiveRangeContribution;
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeOrderKeyBinder;
import org.weakref.nitro.core.type.UnorderedPlacement;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public final class WindowOperator
        implements Operator, RangeOutputSource
{
    private final WindowOperatorPolicy policy;
    private final Allocator allocator;
    private final PrimitiveArrayPool arrayPool;
    private final Allocator.Context allocationContext = new Allocator.Context("WindowOperator");
    private final Operator source;
    private final int[] partitionColumns;
    private final int[] orderingColumns;
    private final boolean[] descendingByColumn;
    private final boolean[] nullsFirstByColumn;
    private final List<RunningWindowFunction> windowFunctions;
    private final WindowInputOrder inputOrder;
    private final boolean lazyOutputs;
    private final Schema fullOutputSchema;
    // Public output ordinal -> full [source..., window functions...] channel. Execution retains the full input layout.
    private int[] outputChannels;
    private Schema outputSchema;
    private final StructuralComparisonKernel[] comparisonKernels;
    private final StructuralTypeKernelFactory structuralTypes;
    private final boolean allowsLegacyOrderingShortcuts;
    private final PartitionPositionIndex partitionPositionIndex = new PartitionPositionIndex();

    private Streams[] sourceSchema;
    private List<TableOperator.Page> pages;
    // Retained source batches keep borrowed or asynchronously leased input vectors alive until the blocking window
    // has finished with them. Taking individual vectors would sever their enclosing lifetime contract.
    private final List<Batch> retainedInputBatches = new ArrayList<>();
    // Multi-page order is one packed page/position long per row. This avoids one RowReference object per row
    // and makes the retained ordering footprint exact and host-visible.
    private long[] rowReferences;
    // A retained upstream aggregation commonly produces one large page. Keep its sort order as
    // primitive positions; this also keeps comparator traffic in two compact int arrays.
    private int[] singlePageOrder;
    private boolean singlePage;
    private boolean singlePageIdentityOrder;
    private int singlePageRowCount;
    private int[] batchPositions;
    private int[] recycledLazyBatchPositions;
    private final int[] radixCounts = new int[256];
    private Streams[] windowOutputs;
    private boolean[] forwardBatchRangeFunctions;
    private int forwardPartitionStart;
    private int forwardPartitionEnd;
    private WindowFrame.Cursor[] forwardFrameCursors;
    private AggregationWindowFrameBounds[] forwardFrameBounds;
    private ExecutionDiagnostics diagnostics = (_, _) -> {};
    private int currentOutputPosition;
    private boolean loaded;

    public WindowOperator(Allocator allocator, Operator source, int[] partitionColumns, int[] orderingColumns, boolean[] descendingByColumn, List<RunningWindowFunction> windowFunctions)
    {
        this(
                allocator,
                source,
                partitionColumns,
                orderingColumns,
                descendingByColumn,
                descendingByColumn.clone(),
                windowFunctions,
                Schema.unspecified(windowFunctions.size()),
                EngineResources.from(allocator).operatorResources());
    }

    public WindowOperator(
            Allocator allocator,
            Operator source,
            int[] partitionColumns,
            int[] orderingColumns,
            boolean[] descendingByColumn,
            List<RunningWindowFunction> windowFunctions,
            Schema windowSchema)
    {
        this(
                allocator,
                source,
                partitionColumns,
                orderingColumns,
                descendingByColumn,
                descendingByColumn.clone(),
                windowFunctions,
                windowSchema,
                EngineResources.from(allocator).operatorResources());
    }

    public WindowOperator(
            Allocator allocator,
            Operator source,
            int[] partitionColumns,
            int[] orderingColumns,
            boolean[] descendingByColumn,
            List<RunningWindowFunction> windowFunctions,
            Schema windowSchema,
            OperatorResources resources)
    {
        this(
                allocator,
                source,
                partitionColumns,
                orderingColumns,
                descendingByColumn,
                descendingByColumn.clone(),
                windowFunctions,
                windowSchema,
                requireNonNull(resources, "resources is null").windowPolicy(),
                resources.codeGeneration().structuralTypes(),
                WindowInputOrder.unordered());
    }

    public WindowOperator(
            Allocator allocator,
            Operator source,
            int[] partitionColumns,
            int[] orderingColumns,
            boolean[] descendingByColumn,
            boolean[] nullsFirstByColumn,
            List<RunningWindowFunction> windowFunctions,
            Schema windowSchema,
            OperatorResources resources)
    {
        this(
                allocator,
                source,
                partitionColumns,
                orderingColumns,
                descendingByColumn,
                nullsFirstByColumn,
                windowFunctions,
                windowSchema,
                requireNonNull(resources, "resources is null").windowPolicy(),
                resources.codeGeneration().structuralTypes(),
                WindowInputOrder.unordered());
    }

    public WindowOperator(
            Allocator allocator,
            Operator source,
            int[] partitionColumns,
            int[] orderingColumns,
            boolean[] descendingByColumn,
            List<RunningWindowFunction> windowFunctions,
            Schema windowSchema,
            OperatorResources resources,
            WindowInputOrder inputOrder)
    {
        this(
                allocator,
                source,
                partitionColumns,
                orderingColumns,
                descendingByColumn,
                descendingByColumn.clone(),
                windowFunctions,
                windowSchema,
                requireNonNull(resources, "resources is null").windowPolicy(),
                resources.codeGeneration().structuralTypes(),
                inputOrder);
    }

    public WindowOperator(
            Allocator allocator,
            Operator source,
            int[] partitionColumns,
            int[] orderingColumns,
            boolean[] descendingByColumn,
            boolean[] nullsFirstByColumn,
            List<RunningWindowFunction> windowFunctions,
            Schema windowSchema,
            OperatorResources resources,
            WindowInputOrder inputOrder)
    {
        this(
                allocator,
                source,
                partitionColumns,
                orderingColumns,
                descendingByColumn,
                nullsFirstByColumn,
                windowFunctions,
                windowSchema,
                requireNonNull(resources, "resources is null").windowPolicy(),
                resources.codeGeneration().structuralTypes(),
                inputOrder);
    }

    public WindowOperator(
            Allocator allocator,
            Operator source,
            int[] partitionColumns,
            int[] orderingColumns,
            boolean[] descendingByColumn,
            List<RunningWindowFunction> windowFunctions,
            Schema windowSchema,
            WindowOperatorPolicy policy)
    {
        this(
                allocator,
                source,
                partitionColumns,
                orderingColumns,
                descendingByColumn,
                descendingByColumn.clone(),
                windowFunctions,
                windowSchema,
                policy,
                new StructuralTypeKernelFactory(),
                WindowInputOrder.unordered());
    }

    private WindowOperator(
            Allocator allocator,
            Operator source,
            int[] partitionColumns,
            int[] orderingColumns,
            boolean[] descendingByColumn,
            boolean[] nullsFirstByColumn,
            List<RunningWindowFunction> windowFunctions,
            Schema windowSchema,
            WindowOperatorPolicy policy,
            StructuralTypeKernelFactory structuralTypes,
            WindowInputOrder inputOrder)
    {
        if (orderingColumns.length != descendingByColumn.length || orderingColumns.length != nullsFirstByColumn.length) {
            throw new IllegalArgumentException("Ordering columns, directions, and null placements must have the same length");
        }
        if (windowFunctions.isEmpty()) {
            throw new IllegalArgumentException("windowFunctions is empty");
        }
        windowSchema = requireNonNull(windowSchema, "windowSchema is null");
        if (windowSchema.size() != windowFunctions.size()) {
            throw new IllegalArgumentException("windowSchema size must match windowFunctions size");
        }
        this.policy = requireNonNull(policy, "policy is null");
        this.allocator = allocator;
        this.arrayPool = allocator.primitiveArrays();
        this.source = source;
        this.partitionColumns = partitionColumns.clone();
        this.orderingColumns = orderingColumns.clone();
        this.descendingByColumn = descendingByColumn.clone();
        this.nullsFirstByColumn = nullsFirstByColumn.clone();
        this.windowFunctions = List.copyOf(windowFunctions);
        this.inputOrder = requireNonNull(inputOrder, "inputOrder is null");
        this.fullOutputSchema = outputSchema(source.outputSchema(), windowSchema);
        this.outputSchema = fullOutputSchema;
        this.outputChannels = new int[fullOutputSchema.size()];
        java.util.Arrays.setAll(outputChannels, index -> index);
        this.structuralTypes = requireNonNull(structuralTypes, "structuralTypes is null");
        this.comparisonKernels = comparisonKernels(
                source.outputSchema(),
                this.partitionColumns,
                this.orderingColumns,
                this.nullsFirstByColumn,
                this.structuralTypes);
        this.allowsLegacyOrderingShortcuts = allowsLegacyOrderingShortcuts(
                this.comparisonKernels, this.partitionColumns, this.orderingColumns);
        // A single-function window normally exposes a narrow result whose consumers read every stream, leaving
        // nothing for lazy output to eliminate. Multiple cooperating functions create the wider filter/project
        // boundary where downstream operators can consume function results without gathering every source lane.
        this.lazyOutputs = policy.lazyOutputs() &&
                (windowFunctions.size() > 1 || inputOrder.isFullyOrdered(orderingColumns.length));
    }

    private static StructuralComparisonKernel[] comparisonKernels(
            Schema sourceSchema,
            int[] partitionColumns,
            int[] orderingColumns,
            boolean[] nullsFirstByColumn,
            StructuralTypeKernelFactory structuralTypes)
    {
        StructuralComparisonKernel[] kernels = new StructuralComparisonKernel[sourceSchema.size()];
        for (int column : partitionColumns) {
            kernels[column] = structuralTypes.comparison(sourceSchema.field(column).type());
        }
        for (int index = 0; index < orderingColumns.length; index++) {
            int column = orderingColumns[index];
            kernels[column] = structuralTypes.comparison(
                    sourceSchema.field(column).type(),
                    nullsFirstByColumn[index] ? UnorderedPlacement.FIRST : UnorderedPlacement.LAST);
        }
        return kernels;
    }

    private static boolean allowsLegacyOrderingShortcuts(
            StructuralComparisonKernel[] kernels,
            int[] partitionColumns,
            int[] orderingColumns)
    {
        for (int column : partitionColumns) {
            if (!kernels[column].allowsLegacyPhysicalShortcuts()) {
                return false;
            }
        }
        for (int column : orderingColumns) {
            if (!kernels[column].allowsLegacyPhysicalShortcuts()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int outputCount()
    {
        return outputChannels.length;
    }

    @Override
    public Schema outputSchema()
    {
        return outputSchema;
    }

    private static Schema outputSchema(Schema sourceSchema, Schema windowSchema)
    {
        List<Field> fields = new ArrayList<>(sourceSchema.fields());
        fields.addAll(windowSchema.fields());
        return new Schema(fields);
    }

    /**
     * Selects and orders the window's public outputs using full output ordinals ({@code source columns} followed by
     * {@code window function results}). Source columns required by partitioning, ordering, frames, or functions remain
     * available to execution when omitted, but are not exposed or gathered into public output batches.
     */
    public WindowOperator withOutputs(int... outputChannels)
    {
        requireNonNull(outputChannels, "outputChannels is null");
        int[] selected = outputChannels.clone();
        List<Field> fields = new ArrayList<>(selected.length);
        for (int outputChannel : selected) {
            if (outputChannel < 0 || outputChannel >= fullOutputSchema.size()) {
                throw new IllegalArgumentException("Window output column is out of bounds: " + outputChannel);
            }
            fields.add(fullOutputSchema.field(outputChannel));
        }
        this.outputChannels = selected;
        this.outputSchema = new Schema(fields);
        if (loaded) {
            forwardBatchRangeFunctions = selectedForwardBatchRangeFunctions();
        }
        return this;
    }

    public WindowOperator withDiagnostics(ExecutionDiagnostics diagnostics)
    {
        if (loaded) {
            throw new IllegalStateException("Window diagnostics must be set before input is loaded");
        }
        this.diagnostics = requireNonNull(diagnostics, "diagnostics is null");
        return this;
    }

    @Override
    public boolean hasNext()
    {
        if (!loaded) {
            load();
        }
        return currentOutputPosition < rowCount();
    }

    @Override
    public Batch next()
    {
        if (!loaded) {
            load();
        }
        int remainingRows = rowCount() - currentOutputPosition;
        if (remainingRows <= 0) {
            throw new IllegalStateException("No window rows remain: currentOutputPosition=%s, rowCount=%s, singlePage=%s"
                    .formatted(currentOutputPosition, rowCount(), singlePage));
        }
        int batchSize = Math.min(policy.maxBatchRows(), remainingRows);
        if (!singlePage && inputOrder.isFullyOrdered(orderingColumns.length)) {
            int page = pageIndex(rowReferences[currentOutputPosition]);
            int rowsInPage = 1;
            while (rowsInPage < batchSize && pageIndex(rowReferences[currentOutputPosition + rowsInPage]) == page) {
                rowsInPage++;
            }
            batchSize = rowsInPage;
        }
        Streams[] forwardBatchOutputs = materializeForwardBatchOutputs(currentOutputPosition, batchSize);
        if (lazyOutputs) {
            return lazyBatch(currentOutputPosition, batchSize, forwardBatchOutputs);
        }
        if (singlePage) {
            ensureBatchPositions(batchSize);
            if (singlePageIdentityOrder) {
                for (int index = 0; index < batchSize; index++) {
                    batchPositions[index] = currentOutputPosition + index;
                }
            }
            else {
                System.arraycopy(singlePageOrder, currentOutputPosition, batchPositions, 0, batchSize);
            }
        }
        Output[] outputs = new Output[outputCount()];
        boolean[] claimedForwardOutput = new boolean[windowFunctions.size()];
        for (int outputIndex = 0; outputIndex < outputChannels.length; outputIndex++) {
            int outputChannel = outputChannels[outputIndex];
            Streams batchStreams = outputChannel < source.outputCount()
                    ? singlePage
                            ? materializeSinglePageSourceColumnBatch(outputChannel, batchSize)
                            : materializeSourceColumnBatch(outputChannel, currentOutputPosition, batchSize)
                    : outputBatchStreams(
                            outputChannel - source.outputCount(),
                            currentOutputPosition,
                            batchSize,
                            forwardBatchOutputs,
                            claimedForwardOutput);
            outputs[outputIndex] = new Output(
                    batchStreams.streams(),
                    batchStreams::get,
                    (stream, vector) -> allocator.transfer(allocationContext, vector),
                    (stream, vector) -> allocator.release(allocationContext, vector));
        }
        currentOutputPosition += batchSize;
        Mask outputMask = allocator.allocateRangeMask(allocationContext, 0, batchSize);
        return new Batch(
                outputMask,
                _ -> {},
                takenMask -> allocator.transfer(allocationContext, takenMask),
                batchMask -> allocator.release(allocationContext, batchMask),
                () -> {},
                outputs);
    }

    @Override
    public boolean drainTo(RangeInputSink sink)
    {
        requireNonNull(sink, "sink is null");
        if (loaded || currentOutputPosition != 0 || !supportsDirectRanges() || !sink.supportsRangeInput(outputSchema)) {
            return false;
        }
        load();
        while (currentOutputPosition < rowCount()) {
            int batchSize = directBatchSize();
            Streams[] generated = materializeForwardBatchOutputs(currentOutputPosition, batchSize);
            try {
                sink.addRange(batchSize, (column, stream) -> {
                    if (column < 0 || column >= outputChannels.length) {
                        throw new IndexOutOfBoundsException("window range output: " + column);
                    }
                    int function = outputChannels[column] - source.outputCount();
                    return generated[function].getOrNull(stream);
                });
            }
            finally {
                releaseGeneratedOutputs(generated);
            }
            currentOutputPosition += batchSize;
        }
        return true;
    }

    @Override
    public boolean drainPrimitiveTo(PrimitiveRangeInputSink sink)
    {
        requireNonNull(sink, "sink is null");
        if (loaded || currentOutputPosition != 0 || !supportsDirectRanges()) {
            return false;
        }
        List<PrimitiveRangeContribution> contributions = new ArrayList<>(outputChannels.length);
        for (int outputChannel : outputChannels) {
            PrimitiveRangeContribution contribution = windowFunctions.get(outputChannel - source.outputCount()).primitiveRangeContribution();
            if (contribution == null) {
                return false;
            }
            contributions.add(contribution);
        }
        if (!sink.supportsPrimitiveRangeInput(contributions)) {
            return false;
        }
        PrimitiveRangeInput input = requireNonNull(sink.bindPrimitiveRangeInput(contributions), "primitive range input is null");
        load();
        while (currentOutputPosition < rowCount()) {
            int batchSize = directBatchSize();
            emitForwardPrimitiveOutputs(currentOutputPosition, batchSize, input);
            input.addCardinality(batchSize);
            currentOutputPosition += batchSize;
        }
        return true;
    }

    private void emitForwardPrimitiveOutputs(int batchStart, int batchSize, PrimitiveRangeInput input)
    {
        PrimitiveRangeConsumer[] consumers = new PrimitiveRangeConsumer[windowFunctions.size()];
        for (int output = 0; output < outputChannels.length; output++) {
            int function = outputChannels[output] - source.outputCount();
            consumers[function] = combine(consumers[function], input.output(output));
        }
        int batchEnd = batchStart + batchSize;
        int position = batchStart;
        while (position < batchEnd) {
            if (position == forwardPartitionEnd) {
                forwardPartitionStart = position;
                forwardPartitionEnd = partitionEnd(position);
                partitionPositionIndex.reset(forwardPartitionStart, forwardPartitionEnd);
                bindForwardFrameCursors();
            }
            int rangeEnd = Math.min(forwardPartitionEnd, batchEnd);
            for (int functionIndex = 0; functionIndex < windowFunctions.size(); functionIndex++) {
                PrimitiveRangeConsumer consumer = consumers[functionIndex];
                if (consumer == null) {
                    continue;
                }
                int boundsOwner = functionIndex;
                for (int previous = 0; previous < functionIndex; previous++) {
                    if (forwardBatchRangeFunctions[previous] && forwardFrameCursors[previous] == forwardFrameCursors[functionIndex]) {
                        boundsOwner = previous;
                        break;
                    }
                }
                if (boundsOwner == functionIndex) {
                    forwardFrameBounds[functionIndex] = forwardFrameCursors[functionIndex].bind(
                            position - forwardPartitionStart,
                            rangeEnd - position);
                }
                else {
                    forwardFrameBounds[functionIndex] = forwardFrameBounds[boundsOwner];
                }
                windowFunctions.get(functionIndex).emitForwardPrimitiveRange(
                        allocator,
                        allocationContext,
                        partitionPositionIndex,
                        position - forwardPartitionStart,
                        rangeEnd - forwardPartitionStart,
                        forwardFrameBounds[functionIndex],
                        consumer);
            }
            position = rangeEnd;
        }
    }

    private static PrimitiveRangeConsumer combine(PrimitiveRangeConsumer first, PrimitiveRangeConsumer second)
    {
        if (first == null) {
            return second;
        }
        if (second == null) {
            return first;
        }
        return new PrimitiveRangeConsumer()
        {
            @Override
            public void addLong(long value)
            {
                first.addLong(value);
                second.addLong(value);
            }

            @Override
            public void addRepeatedLong(long value, long count)
            {
                first.addRepeatedLong(value, count);
                second.addRepeatedLong(value, count);
            }

            @Override
            public void addNull()
            {
                first.addNull();
                second.addNull();
            }
        };
    }

    private boolean supportsDirectRanges()
    {
        for (int outputChannel : outputChannels) {
            int function = outputChannel - source.outputCount();
            if (function < 0 || !windowFunctions.get(function).supportsForwardBatchRangeMaterialization()) {
                return false;
            }
        }
        return true;
    }

    private int directBatchSize()
    {
        int batchSize = Math.min(policy.maxBatchRows(), rowCount() - currentOutputPosition);
        if (!singlePage && inputOrder.isFullyOrdered(orderingColumns.length)) {
            int page = pageIndex(rowReferences[currentOutputPosition]);
            int rowsInPage = 1;
            while (rowsInPage < batchSize && pageIndex(rowReferences[currentOutputPosition + rowsInPage]) == page) {
                rowsInPage++;
            }
            batchSize = rowsInPage;
        }
        return batchSize;
    }

    private void releaseGeneratedOutputs(Streams[] outputs)
    {
        Set<Vector> released = Collections.newSetFromMap(new IdentityHashMap<>());
        for (int outputChannel : outputChannels) {
            Streams streams = outputs[outputChannel - source.outputCount()];
            for (Vector vector : streams.asMap().values()) {
                if (released.add(vector)) {
                    allocator.release(allocationContext, vector);
                }
            }
        }
    }

    private Batch lazyBatch(int startPosition, int batchSize, Streams[] forwardBatchOutputs)
    {
        LazyBatchState batchState = new LazyBatchState(startPosition, batchSize);
        Output[] outputs = new Output[outputCount()];
        boolean[] claimedForwardOutput = new boolean[windowFunctions.size()];
        for (int outputIndex = 0; outputIndex < outputChannels.length; outputIndex++) {
            int outputChannel = outputChannels[outputIndex];
            boolean sourceOutput = outputChannel < source.outputCount();
            int physicalOutput = sourceOutput ? outputChannel : outputChannel - source.outputCount();
            Streams streams = sourceOutput
                    ? sourceSchema[physicalOutput]
                    : forwardBatchRangeFunctions[physicalOutput]
                            ? claimForwardOutput(forwardBatchOutputs[physicalOutput], physicalOutput, claimedForwardOutput)
                            : windowOutputs[physicalOutput];
            outputs[outputIndex] = new Output(
                    streams.streams(),
                    sourceOutput
                            ? stream -> batchState.materializeSourceStream(physicalOutput, stream)
                            : forwardBatchRangeFunctions[physicalOutput]
                                    ? streams::get
                                    : stream -> batchState.materializeWindowStream(physicalOutput, stream),
                    (stream, vector) -> allocator.transfer(allocationContext, vector),
                    (stream, vector) -> allocator.release(allocationContext, vector));
        }
        currentOutputPosition += batchSize;
        Mask outputMask = allocator.allocateRangeMask(allocationContext, 0, batchSize);
        return new Batch(
                outputMask,
                _ -> {},
                takenMask -> allocator.transfer(allocationContext, takenMask),
                batchMask -> allocator.release(allocationContext, batchMask),
                batchState::close,
                outputs);
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
        // Output is fully computed and constrain() is a no-op, so a downstream constrain + re-borrow
        // would re-read the full, differently-indexed output. Cannot satisfy a constrained re-borrow.
        return false;
    }

    @Override
    public void close()
    {
        windowFunctions.forEach(function -> function.reportDiagnostics(diagnostics));
        source.close();
        closeRetainedInputBatches();
        partitionPositionIndex.close();
        allocator.release(allocationContext);
        arrayPool.release(singlePageOrder);
        singlePageOrder = null;
        arrayPool.release(rowReferences);
        rowReferences = null;
        singlePage = false;
        singlePageIdentityOrder = false;
        singlePageRowCount = 0;
        arrayPool.release(batchPositions);
        batchPositions = null;
        arrayPool.release(recycledLazyBatchPositions);
        recycledLazyBatchPositions = null;
    }

    private void load()
    {
        if (pages == null) {
            pages = new ArrayList<>();
            sourceSchema = new Streams[source.outputCount()];
        }
        while (source.hasNext()) {
            try (Batch batch = source.next()) {
                Mask mask = batch.borrowMask();
                for (int outputIndex = 0; outputIndex < source.outputCount(); outputIndex++) {
                    if (sourceSchema[outputIndex] == null) {
                        sourceSchema[outputIndex] = emptyStreamsLike(batch.output(outputIndex));
                    }
                }
                if (mask.none()) {
                    continue;
                }
                // A blocking window repeatedly traverses its retained pages while sorting, finding partitions,
                // evaluating functions, and materializing output. Retaining a sparse batch preserves the source's
                // larger physical address space and makes every one of those passes indirect. Compact sparse input
                // once, as WindowSession historically did; retain only dense batches whose logical and physical
                // positions already coincide.
                if (source.supportsRetainedBatches() && mask.all()) {
                    Batch retainedBatch = batch.transferOwnership();
                    Streams[] retainedColumns = new Streams[source.outputCount()];
                    try {
                        for (int outputIndex = 0; outputIndex < retainedColumns.length; outputIndex++) {
                            retainedColumns[outputIndex] = borrowedStreams(retainedBatch.output(outputIndex));
                        }
                        pages.add(new TableOperator.Page(mask.count(), retainedColumns, retainedBatch.borrowMask()));
                        retainedInputBatches.add(retainedBatch);
                    }
                    catch (RuntimeException | Error failure) {
                        retainedBatch.close();
                        throw failure;
                    }
                    continue;
                }
                Streams[] columns = new Streams[source.outputCount()];
                for (int outputIndex = 0; outputIndex < columns.length; outputIndex++) {
                    columns[outputIndex] = allocator.copyStreams(allocationContext, borrowedStreams(batch.output(outputIndex)), mask);
                }
                pages.add(new TableOperator.Page(mask.count(), columns, Mask.all(mask.count())));
            }
        }
        coalesceUnorderedPages();
        loaded = true;
        windowOutputs = new Streams[windowFunctions.size()];
        forwardBatchRangeFunctions = selectedForwardBatchRangeFunctions();
        if (pages.size() == 1) {
            singlePage = true;
            singlePageRowCount = pages.getFirst().mask().count();
            singlePageIdentityOrder = inputOrder.isFullyOrdered(orderingColumns.length) || canReuseIdentityOrder(pages.getFirst());
            if (!singlePageIdentityOrder) {
                singlePageOrder = selectedPositions(pages.getFirst());
                stableSortSinglePagePositions(singlePageOrder);
                accountRetainedArrays();
            }
            if (policy.fusedFunctions() && windowFunctions.size() > 1 && !hasBatchRangeCapableFunctions()) {
                materializeSinglePageWindows();
            }
            else {
                for (int functionIndex = 0; functionIndex < windowFunctions.size(); functionIndex++) {
                    if (!windowFunctions.get(functionIndex).supportsForwardBatchRangeMaterialization()) {
                        windowOutputs[functionIndex] = materializeSinglePageWindow(windowFunctions.get(functionIndex));
                    }
                }
            }
        }
        else {
            rowReferences = rowReferences(pages);
            if (!inputOrder.isFullyOrdered(orderingColumns.length)) {
                stableSortRowReferences(rowReferences);
            }
            accountRetainedArrays();
            for (int functionIndex = 0; functionIndex < windowFunctions.size(); functionIndex++) {
                if (!windowFunctions.get(functionIndex).supportsForwardBatchRangeMaterialization()) {
                    windowOutputs[functionIndex] = inputOrder.isFullyOrdered(orderingColumns.length)
                            ? materializeOrderedWindow(windowFunctions.get(functionIndex))
                            : materializeWindow(windowFunctions.get(functionIndex));
                }
            }
        }
    }

    private boolean[] selectedForwardBatchRangeFunctions()
    {
        boolean[] selected = new boolean[windowFunctions.size()];
        for (int outputChannel : outputChannels) {
            int functionIndex = outputChannel - source.outputCount();
            if (functionIndex >= 0 && windowFunctions.get(functionIndex).supportsForwardBatchRangeMaterialization()) {
                selected[functionIndex] = true;
            }
        }
        forwardFrameCursors = new WindowFrame.Cursor[windowFunctions.size()];
        forwardFrameBounds = new AggregationWindowFrameBounds[windowFunctions.size()];
        return selected;
    }

    private boolean hasForwardBatchRangeFunctions()
    {
        for (boolean function : forwardBatchRangeFunctions) {
            if (function) {
                return true;
            }
        }
        return false;
    }

    private boolean hasBatchRangeCapableFunctions()
    {
        for (RunningWindowFunction function : windowFunctions) {
            if (function.supportsForwardBatchRangeMaterialization()) {
                return true;
            }
        }
        return false;
    }

    private Streams[] materializeForwardBatchOutputs(int batchStart, int batchSize)
    {
        if (!hasForwardBatchRangeFunctions()) {
            return null;
        }
        Streams[] outputs = new Streams[windowFunctions.size()];
        int batchEnd = batchStart + batchSize;
        int position = batchStart;
        while (position < batchEnd) {
            if (position == forwardPartitionEnd) {
                forwardPartitionStart = position;
                forwardPartitionEnd = partitionEnd(position);
                partitionPositionIndex.reset(forwardPartitionStart, forwardPartitionEnd);
                bindForwardFrameCursors();
            }
            int rangeEnd = Math.min(forwardPartitionEnd, batchEnd);
            for (int functionIndex = 0; functionIndex < windowFunctions.size(); functionIndex++) {
                if (forwardBatchRangeFunctions[functionIndex]) {
                    int boundsOwner = functionIndex;
                    for (int previous = 0; previous < functionIndex; previous++) {
                        if (forwardBatchRangeFunctions[previous] && forwardFrameCursors[previous] == forwardFrameCursors[functionIndex]) {
                            boundsOwner = previous;
                            break;
                        }
                    }
                    if (boundsOwner == functionIndex) {
                        forwardFrameBounds[functionIndex] = forwardFrameCursors[functionIndex].bind(
                                position - forwardPartitionStart,
                                rangeEnd - position);
                    }
                    else {
                        forwardFrameBounds[functionIndex] = forwardFrameBounds[boundsOwner];
                    }
                    outputs[functionIndex] = windowFunctions.get(functionIndex).materializeForwardBatchRange(
                            allocator,
                            allocationContext,
                            outputs[functionIndex],
                            partitionPositionIndex,
                            position - forwardPartitionStart,
                            rangeEnd - forwardPartitionStart,
                            position - batchStart,
                            batchSize,
                            forwardFrameBounds[functionIndex]);
                }
            }
            position = rangeEnd;
        }
        return outputs;
    }

    private void bindForwardFrameCursors()
    {
        for (int functionIndex = 0; functionIndex < windowFunctions.size(); functionIndex++) {
            if (!forwardBatchRangeFunctions[functionIndex]) {
                continue;
            }
            int shared = -1;
            Object identity = windowFunctions.get(functionIndex).frameTraversalIdentity();
            for (int previous = 0; previous < functionIndex; previous++) {
                if (forwardBatchRangeFunctions[previous] && java.util.Objects.equals(windowFunctions.get(previous).frameTraversalIdentity(), identity)) {
                    shared = previous;
                    break;
                }
            }
            forwardFrameCursors[functionIndex] = shared >= 0
                    ? forwardFrameCursors[shared]
                    : windowFunctions.get(functionIndex).bindForwardBatchBounds(partitionPositionIndex);
        }
    }

    private int partitionEnd(int position)
    {
        if (partitionColumns.length == 0) {
            return rowCount();
        }
        int end = position + 1;
        while (end < rowCount() && samePartitionAt(end - 1, end)) {
            end++;
        }
        return end;
    }

    private boolean samePartitionAt(int left, int right)
    {
        if (singlePage) {
            int leftPosition = singlePageIdentityOrder ? left : singlePageOrder[left];
            int rightPosition = singlePageIdentityOrder ? right : singlePageOrder[right];
            return samePartition(pages.getFirst().columns(), leftPosition, rightPosition);
        }
        return samePartition(rowReferences[left], rowReferences[right]);
    }

    /**
     * Sorting and window evaluation revisit an unordered input several times. Normalize compatible pages into one
     * dense physical domain once so those passes use compact positions and output can borrow contiguous source
     * ranges instead of gathering every column through packed page/position references.
     */
    private void coalesceUnorderedPages()
    {
        if (inputOrder.isFullyOrdered(orderingColumns.length) || pages.size() < 2 || !haveConsistentStreams()) {
            return;
        }

        int rows = 0;
        for (TableOperator.Page page : pages) {
            rows = Math.addExact(rows, page.rows());
        }
        // A compact physical domain can represent a very large logical window (for example an RLE or dictionary
        // stream spanning a billion rows). Flattening that input would discard the encoding and can consume the
        // entire query memory budget. The engine-selected bound admits the locality optimization only when its
        // dense row domain is bounded independently of the concrete vector or logical type.
        if (rows > policy.maxCoalescedRows()) {
            return;
        }

        Streams[] columns = new Streams[source.outputCount()];
        for (int columnIndex = 0; columnIndex < columns.length; columnIndex++) {
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
            allocator.release(allocationContext, page.mask());
        }
        pages.clear();
        pages.add(new TableOperator.Page(rows, columns, Mask.all(rows)));
        closeRetainedInputBatches();
    }

    private boolean haveConsistentStreams()
    {
        for (int columnIndex = 0; columnIndex < source.outputCount(); columnIndex++) {
            Set<Stream> streams = pages.getFirst().columns()[columnIndex].streams();
            for (int pageIndex = 1; pageIndex < pages.size(); pageIndex++) {
                if (!pages.get(pageIndex).columns()[columnIndex].streams().equals(streams)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Evaluate cooperating functions over one shared ordered-row and partition traversal.  Function state and
     * output vectors remain independent; only order lookup and partition-boundary discovery are shared.
     */
    private void materializeSinglePageWindows()
    {
        Streams[] columns = pages.getFirst().columns();
        for (int functionIndex = 0; functionIndex < windowFunctions.size(); functionIndex++) {
            RunningWindowFunction function = windowFunctions.get(functionIndex);
            windowOutputs[functionIndex] = function.emptyOutput(
                    allocator,
                    allocationContext,
                    singlePageRowCount);
            function.reset();
        }

        int partitionStart = 0;
        int previousPosition = -1;
        boolean identityOrder = singlePageIdentityOrder;
        int[] order = singlePageOrder;
        for (int outputPosition = 0; outputPosition < singlePageRowCount; outputPosition++) {
            int inputPosition = identityOrder ? outputPosition : order[outputPosition];
            if (previousPosition >= 0 && !samePartition(columns, previousPosition, inputPosition)) {
                for (int functionIndex = 0; functionIndex < windowFunctions.size(); functionIndex++) {
                    RunningWindowFunction function = windowFunctions.get(functionIndex);
                    windowOutputs[functionIndex] = finishPartition(
                            function,
                            windowOutputs[functionIndex],
                            partitionStart,
                            outputPosition,
                            singlePageRowCount);
                    function.reset();
                }
                partitionStart = outputPosition;
            }
            for (int functionIndex = 0; functionIndex < windowFunctions.size(); functionIndex++) {
                windowOutputs[functionIndex] = windowFunctions.get(functionIndex).append(
                        allocator,
                        allocationContext,
                        windowOutputs[functionIndex],
                        columns,
                        inputPosition,
                        outputPosition,
                        singlePageRowCount);
            }
            previousPosition = inputPosition;
        }
        if (singlePageRowCount > 0) {
            for (int functionIndex = 0; functionIndex < windowFunctions.size(); functionIndex++) {
                RunningWindowFunction function = windowFunctions.get(functionIndex);
                windowOutputs[functionIndex] = finishPartition(
                        function,
                        windowOutputs[functionIndex],
                        partitionStart,
                        singlePageRowCount,
                        singlePageRowCount);
            }
        }
    }

    private int rowCount()
    {
        return singlePage ? singlePageRowCount : rowReferences.length;
    }

    private int[] selectedPositions(TableOperator.Page page)
    {
        int[] positions = arrayPool.borrowInts(page.mask().count());
        if (page.mask().all()) {
            for (int position = 0; position < positions.length; position++) {
                positions[position] = position;
            }
        }
        else {
            for (int index = 0; index < positions.length; index++) {
                positions[index] = page.mask().position(index);
            }
        }
        return positions;
    }

    /** Stable bottom-up merge sort over primitive row positions. */
    private void stableSortSinglePagePositions(int[] positions)
    {
        int length = positions.length;
        if (length < 2) {
            return;
        }
        if (policy.radixSort() && tryStableRadixSortSinglePagePositions(positions)) {
            return;
        }
        Streams[] columns = pages.getFirst().columns();
        StructuralComparisonKernel.PositionComparison[] partitionComparisons =
                bindSinglePagePartitionComparisons(columns);
        StructuralComparisonKernel.PositionComparison[] orderingComparisons =
                bindNullFreeSinglePageOrderingComparisons(columns);
        int[] scratch = arrayPool.borrowInts(length);
        try {
            int[] source = positions;
            int[] target = scratch;
            for (int width = 1; width < length; width = width > length / 2 ? length : width * 2) {
                for (int start = 0; start < length; start += 2 * width) {
                    int middle = Math.min(start + width, length);
                    int end = Math.min(start + 2 * width, length);
                    int left = start;
                    int right = middle;
                    int output = start;
                    while (left < middle && right < end) {
                        if (compareSinglePagePositions(
                                partitionComparisons,
                                orderingComparisons,
                                columns,
                                source[left],
                                source[right]) <= 0) {
                            target[output++] = source[left++];
                        }
                        else {
                            target[output++] = source[right++];
                        }
                    }
                    while (left < middle) {
                        target[output++] = source[left++];
                    }
                    while (right < end) {
                        target[output++] = source[right++];
                    }
                }
                int[] swap = source;
                source = target;
                target = swap;
            }
            if (source != positions) {
                System.arraycopy(source, 0, positions, 0, length);
            }
        }
        finally {
            arrayPool.release(scratch);
        }
    }

    /**
     * Reuse a dense producer's physical order only after proving that every row is monotonic under the window's
     * complete PARTITION BY and ORDER BY semantics. Restrict admission to the same flat integer family handled by
     * the radix sorter so ordinary unordered inputs retain their established path. A small distributed sample
     * cheaply rejects disorder before the exact pass; the sample is never used as proof.
     */
    private boolean canReuseIdentityOrder(TableOperator.Page page)
    {
        int length = page.mask().count();
        if (!allowsLegacyOrderingShortcuts ||
                !policy.reuseOrderedInput() ||
                !page.mask().all() ||
                length < policy.reuseOrderedInputMinRows()) {
            return false;
        }
        FlatIntegerOrderKey[] keys = flatIntegerOrderKeys(page.columns());
        if (keys == null) {
            return false;
        }

        boolean ordered = isIdentityOrderMonotonic(length, keys);
        if (policy.debugReuseOrderedInput()) {
            System.err.printf("WindowOperator reuseOrderedInput=%s rows=%d partitions=%d ordering=%d%n",
                    ordered, length, partitionColumns.length, orderingColumns.length);
        }
        return ordered;
    }

    private boolean isIdentityOrderMonotonic(int length, FlatIntegerOrderKey[] keys)
    {
        int intervals = Math.min(policy.reuseOrderedInputSamples(), length - 1);
        for (int sample = 1; sample <= intervals; sample++) {
            int right = (int) ((long) sample * (length - 1) / intervals);
            if (compareFlatIntegerOrder(keys, right - 1, right) > 0) {
                return false;
            }
        }
        for (int right = 1; right < length; right++) {
            if (compareFlatIntegerOrder(keys, right - 1, right) > 0) {
                return false;
            }
        }
        return true;
    }

    private FlatIntegerOrderKey[] flatIntegerOrderKeys(Streams[] columns)
    {
        FlatIntegerOrderKey[] keys = new FlatIntegerOrderKey[partitionColumns.length + orderingColumns.length];
        int keyIndex = 0;
        for (int column : partitionColumns) {
            FlatIntegerOrderKey key = flatIntegerOrderKey(columns[column], false, false);
            if (key == null) {
                return null;
            }
            keys[keyIndex++] = key;
        }
        for (int index = 0; index < orderingColumns.length; index++) {
            FlatIntegerOrderKey key = flatIntegerOrderKey(
                    columns[orderingColumns[index]],
                    descendingByColumn[index],
                    nullsFirstByColumn[index]);
            if (key == null) {
                return null;
            }
            keys[keyIndex++] = key;
        }
        return keys;
    }

    private static FlatIntegerOrderKey flatIntegerOrderKey(Streams streams, boolean descending, boolean nullsFirst)
    {
        Vector values = streams.values();
        Vector nullVector = streams.getOrNull(Stream.NULLS);
        boolean[] nulls = VectorAccess.isAllFalseNulls(nullVector) ? null :
                nullVector instanceof BooleanVector booleans ? booleans.values() : null;
        if (nulls == null && !VectorAccess.isAllFalseNulls(nullVector)) {
            return null;
        }
        return switch (values) {
            case I64Vector longs -> new FlatIntegerOrderKey(longs.values(), null, nulls, descending, nullsFirst);
            case I32Vector integers -> new FlatIntegerOrderKey(null, integers.values(), nulls, descending, nullsFirst);
            default -> null;
        };
    }

    private static int compareFlatIntegerOrder(FlatIntegerOrderKey[] keys, int left, int right)
    {
        for (FlatIntegerOrderKey key : keys) {
            int comparison;
            if (key.nulls() != null && (key.nulls()[left] || key.nulls()[right])) {
                boolean leftNull = key.nulls()[left];
                boolean rightNull = key.nulls()[right];
                comparison = leftNull == rightNull ? 0 : leftNull == key.nullsFirst() ? -1 : 1;
            }
            else {
                comparison = key.longs() != null
                        ? Long.compare(key.longs()[left], key.longs()[right])
                        : Integer.compare(key.integers()[left], key.integers()[right]);
            }
            if (key.descending() && comparison != 0 && (key.nulls() == null || !key.nulls()[left] && !key.nulls()[right])) {
                comparison = -comparison;
            }
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    /**
     * Stable LSD radix ordering for flat integer window keys. Apply keys from least to most
     * significant (last ORDER BY key through first PARTITION BY key), preserving lexicographic
     * SQL order without comparison-sort random reads. Nullable keys get a final stable null bucket
     * so nulls remain last ascending and first descending, matching OperatorOrderingSemantics.
     */
    private boolean tryStableRadixSortSinglePagePositions(int[] positions)
    {
        if (tryStableRadixSortNormalizedOrderingKey(positions)) {
            return true;
        }
        if (!allowsLegacyOrderingShortcuts) {
            return false;
        }
        Streams[] columns = pages.getFirst().columns();
        if (policy.binaryHashPartitionSort() && orderingColumns.length == 0 && partitionColumns.length == 1 &&
                isBinarySortKey(columns[partitionColumns[0]].values())) {
            stableHashRadixSortBinaryPartition(positions, columns[partitionColumns[0]]);
            return true;
        }
        for (int column : partitionColumns) {
            if (!isFlatIntegerSortKey(columns[column])) {
                return false;
            }
        }
        for (int column : orderingColumns) {
            if (!isFlatIntegerSortKey(columns[column])) {
                return false;
            }
        }

        int[] scratch = arrayPool.borrowInts(positions.length);
        try {
            for (int index = orderingColumns.length - 1; index >= 0; index--) {
                stableRadixSortColumn(
                        positions,
                        scratch,
                        columns[orderingColumns[index]],
                        descendingByColumn[index],
                        nullsFirstByColumn[index]);
            }
            for (int index = partitionColumns.length - 1; index >= 0; index--) {
                stableRadixSortColumn(positions, scratch, columns[partitionColumns[index]], false, false);
            }
        }
        finally {
            arrayPool.release(scratch);
        }
        return true;
    }

    /**
     * Bind one exact provider-owned ordering key per input position, then keep keys and row positions aligned through
     * the radix passes. This avoids repeatedly gathering the retained value vector for every byte of a wide key.
     */
    private boolean tryStableRadixSortNormalizedOrderingKey(int[] positions)
    {
        if (partitionColumns.length != 0 || orderingColumns.length != 1) {
            return false;
        }
        int column = orderingColumns[0];
        Streams streams = pages.getFirst().columns()[column];
        if (!VectorAccess.isAllFalseNulls(streams.getOrNull(Stream.NULLS))) {
            return false;
        }
        TypeOrderKeyBinder binder = source.outputSchema().field(column).type().orderKeyBinder().orElse(null);
        if (binder == null) {
            return false;
        }
        TypeOrderKeyBinder.Bound ordering = binder.bind(streams.values()).orElse(null);
        if (ordering == null) {
            return false;
        }

        int length = positions.length;
        long[] keys = arrayPool.borrowLongs(length);
        long[] scratchKeys = arrayPool.borrowLongs(length);
        int[] scratchPositions = arrayPool.borrowInts(length);
        try {
            long firstKey = 0;
            long varyingBytes = 0;
            for (int index = 0; index < length; index++) {
                long key = ordering.key(positions[index]);
                if (descendingByColumn[0]) {
                    key = ~key;
                }
                keys[index] = key;
                if (index == 0) {
                    firstKey = key;
                }
                else {
                    varyingBytes |= firstKey ^ key;
                }
            }

            int[] sourcePositions = positions;
            int[] targetPositions = scratchPositions;
            long[] sourceKeys = keys;
            long[] targetKeys = scratchKeys;
            for (int byteIndex = 0; byteIndex < Long.BYTES; byteIndex++) {
                int shift = byteIndex * Byte.SIZE;
                if (((varyingBytes >>> shift) & 0xFF) == 0) {
                    continue;
                }
                java.util.Arrays.fill(radixCounts, 0);
                for (int index = 0; index < length; index++) {
                    radixCounts[(int) ((sourceKeys[index] >>> shift) & 0xFF)]++;
                }
                int offset = 0;
                for (int bucket = 0; bucket < radixCounts.length; bucket++) {
                    int count = radixCounts[bucket];
                    radixCounts[bucket] = offset;
                    offset += count;
                }
                for (int index = 0; index < length; index++) {
                    long key = sourceKeys[index];
                    int output = radixCounts[(int) ((key >>> shift) & 0xFF)]++;
                    targetKeys[output] = key;
                    targetPositions[output] = sourcePositions[index];
                }
                int[] positionSwap = sourcePositions;
                sourcePositions = targetPositions;
                targetPositions = positionSwap;
                long[] keySwap = sourceKeys;
                sourceKeys = targetKeys;
                targetKeys = keySwap;
            }
            if (sourcePositions != positions) {
                System.arraycopy(sourcePositions, 0, positions, 0, length);
            }
            return true;
        }
        finally {
            arrayPool.release(keys);
            arrayPool.release(scratchKeys);
            arrayPool.release(scratchPositions);
        }
    }

    private void stableHashRadixSortBinaryPartition(int[] positions, Streams streams)
    {
        Vector values = streams.values();
        Vector nulls = streams.getOrNull(Stream.NULLS);
        int[] hashes = arrayPool.borrowInts(values.length());
        int[] scratch = arrayPool.borrowInts(positions.length);
        try {
            for (int position : positions) {
                if (!OperatorVectorSupport.isNull(nulls, position)) {
                    hashes[position] = OperatorVectorSupport.binaryHash(values, position);
                }
            }
            int[] source = positions;
            int[] target = scratch;
            for (int pass = 0; pass < Integer.BYTES + 1; pass++) {
                java.util.Arrays.fill(radixCounts, 0);
                int shift = pass * Byte.SIZE;
                for (int position : source) {
                    int bucket = pass == Integer.BYTES
                            ? (OperatorVectorSupport.isNull(nulls, position) ? 1 : 0)
                            : (hashes[position] >>> shift) & 0xFF;
                    radixCounts[bucket]++;
                }
                int offset = 0;
                for (int bucket = 0; bucket < radixCounts.length; bucket++) {
                    int count = radixCounts[bucket];
                    radixCounts[bucket] = offset;
                    offset += count;
                }
                for (int position : source) {
                    int bucket = pass == Integer.BYTES
                            ? (OperatorVectorSupport.isNull(nulls, position) ? 1 : 0)
                            : (hashes[position] >>> shift) & 0xFF;
                    target[radixCounts[bucket]++] = position;
                }
                int[] swap = source;
                source = target;
                target = swap;
            }
            if (source != positions) {
                System.arraycopy(source, 0, positions, 0, positions.length);
            }

            // Hash equality is only a candidate for SQL equality. Resolve the rare collision run exactly; an
            // ordinary repeated partition value is detected with one linear equality pass and needs no sorting.
            int start = 0;
            while (start < positions.length) {
                int first = positions[start];
                boolean firstNull = OperatorVectorSupport.isNull(nulls, first);
                int hash = hashes[first];
                int end = start + 1;
                while (end < positions.length) {
                    int position = positions[end];
                    if (OperatorVectorSupport.isNull(nulls, position) != firstNull || hashes[position] != hash) {
                        break;
                    }
                    end++;
                }
                if (!firstNull && end - start > 1) {
                    boolean oneValue = true;
                    for (int index = start + 1; index < end; index++) {
                        if (!OperatorVectorSupport.binaryEquals(values, first, values, positions[index])) {
                            oneValue = false;
                            break;
                        }
                    }
                    if (!oneValue) {
                        stableSortBinaryCollisionRange(positions, scratch, start, end, values);
                    }
                }
                start = end;
            }
        }
        finally {
            arrayPool.release(hashes);
            arrayPool.release(scratch);
        }
    }

    private static void stableSortBinaryCollisionRange(int[] positions, int[] scratch, int start, int end, Vector values)
    {
        for (int width = 1; width < end - start; width *= 2) {
            for (int leftStart = start; leftStart < end; leftStart += 2 * width) {
                int middle = Math.min(leftStart + width, end);
                int rightEnd = Math.min(leftStart + 2 * width, end);
                int left = leftStart;
                int right = middle;
                int output = leftStart;
                while (left < middle && right < rightEnd) {
                    if (OperatorVectorSupport.binaryCompare(values, positions[left], values, positions[right]) <= 0) {
                        scratch[output++] = positions[left++];
                    }
                    else {
                        scratch[output++] = positions[right++];
                    }
                }
                while (left < middle) {
                    scratch[output++] = positions[left++];
                }
                while (right < rightEnd) {
                    scratch[output++] = positions[right++];
                }
                System.arraycopy(scratch, leftStart, positions, leftStart, rightEnd - leftStart);
            }
        }
    }

    private static boolean isBinarySortKey(Vector values)
    {
        return switch (values) {
            case BinaryVector _ -> true;
            case DictionaryVector dictionary -> isBinarySortKey(dictionary.values());
            case RleVector rle -> isBinarySortKey(rle.values());
            default -> false;
        };
    }

    private static boolean isFlatIntegerSortKey(Streams streams)
    {
        Vector values = streams.values();
        Vector nulls = streams.getOrNull(Stream.NULLS);
        return (values instanceof I64Vector || values instanceof I32Vector) &&
                (VectorAccess.isAllFalseNulls(nulls) || nulls instanceof BooleanVector);
    }

    private void stableRadixSortColumn(int[] positions, int[] scratch, Streams streams, boolean descending, boolean nullsFirst)
    {
        Vector values = streams.values();
        Vector nullVector = streams.getOrNull(Stream.NULLS);
        boolean[] nulls = VectorAccess.isAllFalseNulls(nullVector) ? null : ((BooleanVector) nullVector).values();
        int bytes = values instanceof I64Vector ? Long.BYTES : Integer.BYTES;
        long firstKey = 0;
        long varyingBytes = 0;
        boolean first = true;
        for (int position : positions) {
            if (nulls != null && nulls[position]) {
                continue;
            }
            long key = integerSortKey(values, position, descending);
            if (first) {
                firstKey = key;
                first = false;
            }
            else {
                varyingBytes |= firstKey ^ key;
            }
        }
        if (first) {
            return;
        }
        int[] source = positions;
        int[] target = scratch;
        int[] counts = radixCounts;
        for (int byteIndex = 0; byteIndex < bytes; byteIndex++) {
            int shift = byteIndex * Byte.SIZE;
            if (((varyingBytes >>> shift) & 0xFF) == 0) {
                continue;
            }
            java.util.Arrays.fill(counts, 0);
            for (int position : source) {
                int bucket = nulls != null && nulls[position] ? 0 : integerSortByte(values, position, shift, descending);
                counts[bucket]++;
            }
            int offset = 0;
            for (int bucket = 0; bucket < counts.length; bucket++) {
                int count = counts[bucket];
                counts[bucket] = offset;
                offset += count;
            }
            for (int position : source) {
                int bucket = nulls != null && nulls[position] ? 0 : integerSortByte(values, position, shift, descending);
                target[counts[bucket]++] = position;
            }
            int[] swap = source;
            source = target;
            target = swap;
        }
        if (nulls != null) {
            int nullCount = 0;
            for (int position : source) {
                if (nulls[position]) {
                    nullCount++;
                }
            }
            int nullOffset = nullsFirst ? 0 : source.length - nullCount;
            int valueOffset = nullsFirst ? nullCount : 0;
            for (int position : source) {
                if (nulls[position]) {
                    target[nullOffset++] = position;
                }
                else {
                    target[valueOffset++] = position;
                }
            }
            source = target;
        }
        if (source != positions) {
            System.arraycopy(source, 0, positions, 0, positions.length);
        }
    }

    private static int integerSortByte(Vector values, int position, int shift, boolean descending)
    {
        return (int) ((integerSortKey(values, position, descending) >>> shift) & 0xFF);
    }

    private static long integerSortKey(Vector values, int position, boolean descending)
    {
        long sortable = switch (values) {
            case I64Vector longs -> longs.values()[position] ^ Long.MIN_VALUE;
            case I32Vector integers -> (integers.values()[position] ^ Integer.MIN_VALUE) & 0xFFFF_FFFFL;
            default -> throw new IllegalArgumentException("Expected flat integer sort key");
        };
        if (!descending) {
            return sortable;
        }
        return values instanceof I64Vector ? ~sortable : (~sortable) & 0xFFFF_FFFFL;
    }

    private StructuralComparisonKernel.PositionComparison[] bindSinglePagePartitionComparisons(Streams[] columns)
    {
        StructuralComparisonKernel.PositionComparison[] comparisons =
                new StructuralComparisonKernel.PositionComparison[partitionColumns.length];
        for (int index = 0; index < partitionColumns.length; index++) {
            int column = partitionColumns[index];
            Streams streams = columns[column];
            comparisons[index] = comparisonKernels[column].bindComparison(
                    streams.values(), streams.getOrNull(Stream.NULLS),
                    streams.values(), streams.getOrNull(Stream.NULLS));
        }
        return comparisons;
    }

    private StructuralComparisonKernel.PositionComparison[] bindNullFreeSinglePageOrderingComparisons(Streams[] columns)
    {
        StructuralComparisonKernel.PositionComparison[] comparisons =
                new StructuralComparisonKernel.PositionComparison[orderingColumns.length];
        for (int index = 0; index < orderingColumns.length; index++) {
            int column = orderingColumns[index];
            Streams streams = columns[column];
            Vector nulls = streams.getOrNull(Stream.NULLS);
            if (VectorAccess.isAllFalseNulls(nulls)) {
                comparisons[index] = comparisonKernels[column].bindComparison(
                        streams.values(), nulls, streams.values(), nulls);
            }
        }
        return comparisons;
    }

    private int compareSinglePagePositions(
            StructuralComparisonKernel.PositionComparison[] partitionComparisons,
            StructuralComparisonKernel.PositionComparison[] orderingComparisons,
            Streams[] columns,
            int leftPosition,
            int rightPosition)
    {
        for (StructuralComparisonKernel.PositionComparison partitionComparison : partitionComparisons) {
            int comparison = partitionComparison.compare(leftPosition, rightPosition);
            if (comparison != 0) {
                return comparison;
            }
        }
        for (int orderingIndex = 0; orderingIndex < orderingColumns.length; orderingIndex++) {
            StructuralComparisonKernel.PositionComparison bound = orderingComparisons[orderingIndex];
            int comparison;
            if (bound != null) {
                comparison = bound.compare(leftPosition, rightPosition);
                if (descendingByColumn[orderingIndex]) {
                    comparison = -comparison;
                }
            }
            else {
                comparison = compareOrderingColumn(orderingIndex, columns, leftPosition, rightPosition);
            }
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    private Streams materializeSinglePageWindow(RunningWindowFunction function)
    {
        Streams[] columns = pages.getFirst().columns();
        Streams output = function.emptyOutput(allocator, allocationContext, singlePageRowCount);
        function.reset();
        int partitionStart = 0;
        int previousPosition = -1;
        boolean identityOrder = singlePageIdentityOrder;
        int[] order = singlePageOrder;
        for (int outputPosition = 0; outputPosition < singlePageRowCount; outputPosition++) {
            int inputPosition = identityOrder ? outputPosition : order[outputPosition];
            if (previousPosition >= 0 && !samePartition(columns, previousPosition, inputPosition)) {
                output = finishPartition(function, output, partitionStart, outputPosition, singlePageRowCount);
                function.reset();
                partitionStart = outputPosition;
            }
            output = function.append(allocator, allocationContext, output, columns, inputPosition, outputPosition, singlePageRowCount);
            previousPosition = inputPosition;
        }
        if (singlePageRowCount > 0) {
            output = finishPartition(function, output, partitionStart, singlePageRowCount, singlePageRowCount);
        }
        return output;
    }

    private boolean samePartition(Streams[] columns, int leftPosition, int rightPosition)
    {
        for (int partitionColumn : partitionColumns) {
            Streams streams = columns[partitionColumn];
            boolean leftNull = OperatorVectorSupport.isNull(streams.getOrNull(Stream.NULLS), leftPosition);
            boolean rightNull = OperatorVectorSupport.isNull(streams.getOrNull(Stream.NULLS), rightPosition);
            if (leftNull || rightNull) {
                if (leftNull != rightNull) {
                    return false;
                }
                continue;
            }
            if (!comparisonKernels[partitionColumn].identical(
                    streams.values(), streams.getOrNull(Stream.NULLS), leftPosition,
                    streams.values(), streams.getOrNull(Stream.NULLS), rightPosition)) {
                return false;
            }
        }
        return true;
    }

    private int compareColumn(int column, Streams streams, int leftPosition, int rightPosition)
    {
        return comparisonKernels[column].compare(
                streams.values(), streams.getOrNull(Stream.NULLS), leftPosition,
                streams.values(), streams.getOrNull(Stream.NULLS), rightPosition);
    }

    private Streams materializeWindow(RunningWindowFunction function)
    {
        Streams output = function.emptyOutput(allocator, allocationContext, rowReferences.length);
        long previous = -1;
        function.reset();
        int partitionStart = 0;
        for (int outputPosition = 0; outputPosition < rowReferences.length; outputPosition++) {
            long row = rowReferences[outputPosition];
            if (previous != -1 && !samePartition(previous, row)) {
                output = finishPartition(function, output, partitionStart, outputPosition, rowReferences.length);
                function.reset();
                partitionStart = outputPosition;
            }
            output = function.append(
                    allocator,
                    allocationContext,
                    output,
                    pages.get(pageIndex(row)).columns(),
                    pagePosition(row),
                    outputPosition,
                    rowReferences.length);
            previous = row;
        }
        if (rowReferences.length > 0) {
            output = finishPartition(function, output, partitionStart, rowReferences.length, rowReferences.length);
        }
        return output;
    }

    private Streams materializeOrderedWindow(RunningWindowFunction function)
    {
        int outputSize = rowReferences.length;
        Streams output = function.emptyOutput(allocator, allocationContext, outputSize);
        Streams[] previousColumns = null;
        int previousPosition = -1;
        function.reset();
        int partitionStart = 0;
        int outputPosition = 0;
        for (TableOperator.Page page : pages) {
            Streams[] columns = page.columns();
            Mask mask = page.mask();
            StructuralComparisonKernel.PositionEquality[] withinPage = bindPartitionEquality(columns, columns);
            StructuralComparisonKernel.PositionEquality[] acrossPages = previousColumns == null || previousColumns == columns
                    ? withinPage
                    : bindPartitionEquality(previousColumns, columns);
            for (int index = 0; index < mask.count(); index++) {
                int inputPosition = mask.all() ? index : mask.position(index);
                StructuralComparisonKernel.PositionEquality[] partitionEquality = index == 0 ? acrossPages : withinPage;
                if (previousColumns != null && !samePartition(partitionEquality, previousPosition, inputPosition)) {
                    output = finishPartition(function, output, partitionStart, outputPosition, outputSize);
                    function.reset();
                    partitionStart = outputPosition;
                }
                output = function.append(
                        allocator,
                        allocationContext,
                        output,
                        columns,
                        inputPosition,
                        outputPosition,
                        outputSize);
                previousColumns = columns;
                previousPosition = inputPosition;
                outputPosition++;
            }
        }
        if (outputPosition > 0) {
            output = finishPartition(function, output, partitionStart, outputPosition, outputSize);
        }
        return output;
    }

    private Streams finishPartition(
            RunningWindowFunction function,
            Streams output,
            int partitionStart,
            int partitionEnd,
            int outputSize)
    {
        partitionPositionIndex.reset(partitionStart, partitionEnd);
        return function.finishPartition(
                allocator,
                allocationContext,
                output,
                partitionPositionIndex,
                partitionStart,
                outputSize);
    }

    private StructuralComparisonKernel.PositionEquality[] bindPartitionEquality(Streams[] leftColumns, Streams[] rightColumns)
    {
        StructuralComparisonKernel.PositionEquality[] equality =
                new StructuralComparisonKernel.PositionEquality[partitionColumns.length];
        for (int index = 0; index < partitionColumns.length; index++) {
            int column = partitionColumns[index];
            Streams left = leftColumns[column];
            Streams right = rightColumns[column];
            equality[index] = comparisonKernels[column].bindPartitionEquality(
                    left.values(), left.getOrNull(Stream.NULLS), right.values(), right.getOrNull(Stream.NULLS));
        }
        return equality;
    }

    private static boolean samePartition(
            StructuralComparisonKernel.PositionEquality[] equality,
            int leftPosition,
            int rightPosition)
    {
        for (StructuralComparisonKernel.PositionEquality key : equality) {
            if (!key.identical(leftPosition, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    private int compareRows(long left, long right)
    {
        for (int partitionColumn : partitionColumns) {
            int comparison = compareColumn(partitionColumn, left, right);
            if (comparison != 0) {
                return comparison;
            }
        }
        for (int orderingIndex = 0; orderingIndex < orderingColumns.length; orderingIndex++) {
            int comparison = compareOrderingColumn(orderingIndex, left, right);
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    private boolean samePartition(long left, long right)
    {
        for (int partitionColumn : partitionColumns) {
            Streams leftStreams = pages.get(pageIndex(left)).columns()[partitionColumn];
            Streams rightStreams = pages.get(pageIndex(right)).columns()[partitionColumn];
            boolean leftNull = OperatorVectorSupport.isNull(leftStreams.getOrNull(Stream.NULLS), pagePosition(left));
            boolean rightNull = OperatorVectorSupport.isNull(rightStreams.getOrNull(Stream.NULLS), pagePosition(right));
            if (leftNull || rightNull) {
                // PARTITION BY groups all null keys together: two nulls share a partition, a null and a
                // non-null do not.
                if (leftNull != rightNull) {
                    return false;
                }
                continue;
            }
            if (!comparisonKernels[partitionColumn].identical(
                    leftStreams.values(),
                    leftStreams.getOrNull(Stream.NULLS),
                    pagePosition(left),
                    rightStreams.values(),
                    rightStreams.getOrNull(Stream.NULLS),
                    pagePosition(right))) {
                return false;
            }
        }
        return true;
    }

    private int compareOrderingColumn(int orderingIndex, Streams[] columns, int leftPosition, int rightPosition)
    {
        int column = orderingColumns[orderingIndex];
        Streams streams = columns[column];
        return compareOrderingValues(orderingIndex, streams, leftPosition, streams, rightPosition);
    }

    private int compareOrderingColumn(int orderingIndex, long left, long right)
    {
        int column = orderingColumns[orderingIndex];
        Streams leftStreams = pages.get(pageIndex(left)).columns()[column];
        Streams rightStreams = pages.get(pageIndex(right)).columns()[column];
        return compareOrderingValues(
                orderingIndex,
                leftStreams,
                pagePosition(left),
                rightStreams,
                pagePosition(right));
    }

    private int compareOrderingValues(
            int orderingIndex,
            Streams leftStreams,
            int leftPosition,
            Streams rightStreams,
            int rightPosition)
    {
        boolean leftNull = OperatorVectorSupport.isNull(leftStreams.getOrNull(Stream.NULLS), leftPosition);
        boolean rightNull = OperatorVectorSupport.isNull(rightStreams.getOrNull(Stream.NULLS), rightPosition);
        if (leftNull || rightNull) {
            if (leftNull == rightNull) {
                return 0;
            }
            return leftNull == nullsFirstByColumn[orderingIndex] ? -1 : 1;
        }
        int column = orderingColumns[orderingIndex];
        int comparison = comparisonKernels[column].compare(
                leftStreams.values(),
                leftStreams.getOrNull(Stream.NULLS),
                leftPosition,
                rightStreams.values(),
                rightStreams.getOrNull(Stream.NULLS),
                rightPosition);
        return descendingByColumn[orderingIndex] ? -comparison : comparison;
    }

    private int compareColumn(int column, long left, long right)
    {
        Streams leftStreams = pages.get(pageIndex(left)).columns()[column];
        Streams rightStreams = pages.get(pageIndex(right)).columns()[column];
        return comparisonKernels[column].compare(
                leftStreams.values(),
                leftStreams.getOrNull(Stream.NULLS),
                pagePosition(left),
                rightStreams.values(),
                rightStreams.getOrNull(Stream.NULLS),
                pagePosition(right));
    }

    private Streams materializeSourceColumnBatch(int outputIndex, int startPosition, int batchSize)
    {
        Streams.Builder builder = Streams.builder();
        Streams schema = sourceSchema[outputIndex];
        for (Stream stream : schema.streams()) {
            Vector result = null;
            int outputPosition = 0;
            while (outputPosition < batchSize) {
                long firstRow = rowReferences[startPosition + outputPosition];
                Streams sourceStreams = pages.get(pageIndex(firstRow)).columns()[outputIndex];
                if (!sourceStreams.has(stream)) {
                    outputPosition++;
                    continue;
                }

                int groupStart = outputPosition;
                int groupPageIndex = pageIndex(firstRow);
                while (outputPosition < batchSize && pageIndex(rowReferences[startPosition + outputPosition]) == groupPageIndex) {
                    outputPosition++;
                }

                int groupSize = outputPosition - groupStart;
                ensureBatchPositions(groupSize);
                for (int index = 0; index < groupSize; index++) {
                    batchPositions[index] = pagePosition(rowReferences[startPosition + groupStart + index]);
                }
                result = sourceStreams.get(stream).copyPositionsInto(
                        allocator,
                        allocationContext,
                        result,
                        batchPositions,
                        groupSize,
                        groupStart,
                        batchSize);
            }
            builder.put(stream, result == null ? schema.get(stream).emptyLike(allocator, allocationContext) : result);
        }
        return builder.build();
    }

    private Streams materializeSinglePageSourceColumnBatch(int outputIndex, int batchSize)
    {
        Streams sourceStreams = pages.getFirst().columns()[outputIndex];
        Streams.Builder builder = Streams.builder();
        for (Stream stream : sourceStreams.streams()) {
            builder.put(stream, sourceStreams.get(stream).copyPositionsInto(
                    allocator,
                    allocationContext,
                    null,
                    batchPositions,
                    batchSize,
                    0,
                    batchSize));
        }
        return builder.build();
    }

    private Streams materializeWindowBatch(int functionIndex, int startPosition, int batchSize)
    {
        Streams fullOutput = windowOutputs[functionIndex];
        Streams.Builder builder = Streams.builder();
        for (Stream stream : fullOutput.streams()) {
            Vector source = fullOutput.get(stream);
            ensureBatchPositions(batchSize);
            for (int index = 0; index < batchSize; index++) {
                batchPositions[index] = startPosition + index;
            }
            Vector result = source.copyPositionsInto(
                    allocator,
                    allocationContext,
                    null,
                    batchPositions,
                    batchSize,
                    0,
                    batchSize);
            builder.put(stream, result == null ? source.emptyLike(allocator, allocationContext) : result);
        }
        return builder.build();
    }

    private Streams outputBatchStreams(
            int functionIndex,
            int startPosition,
            int batchSize,
            Streams[] forwardBatchOutputs,
            boolean[] claimedForwardOutput)
    {
        if (!forwardBatchRangeFunctions[functionIndex]) {
            return materializeWindowBatch(functionIndex, startPosition, batchSize);
        }
        return claimForwardOutput(forwardBatchOutputs[functionIndex], functionIndex, claimedForwardOutput);
    }

    private Streams claimForwardOutput(Streams output, int functionIndex, boolean[] claimed)
    {
        if (!claimed[functionIndex]) {
            claimed[functionIndex] = true;
            return output;
        }
        Streams.Builder copy = Streams.builder();
        for (Stream stream : output.streams()) {
            copy.put(stream, output.get(stream).copy(allocator, allocationContext));
        }
        return copy.build();
    }

    private Vector materializeSourceStreamBatch(int outputIndex, Stream stream, int startPosition, int batchSize, int[] positions)
    {
        if (singlePage) {
            Streams sourceStreams = pages.getFirst().columns()[outputIndex];
            if (singlePageIdentityOrder && startPosition == 0 && batchSize == singlePageRowCount) {
                return sourceStreams.get(stream).copy(allocator, allocationContext);
            }
            return sourceStreams.get(stream).copyPositionsInto(
                    allocator,
                    allocationContext,
                    null,
                    positions,
                    batchSize,
                    0,
                    batchSize);
        }

        if (inputOrder.isFullyOrdered(orderingColumns.length)) {
            long firstRow = rowReferences[startPosition];
            TableOperator.Page page = pages.get(pageIndex(firstRow));
            int pagePosition = pagePosition(firstRow);
            if (page.mask().all() && pagePosition == 0 && batchSize == page.rows()) {
                return page.columns()[outputIndex].get(stream).copy(allocator, allocationContext);
            }
        }

        Vector result = null;
        int outputPosition = 0;
        while (outputPosition < batchSize) {
            long firstRow = rowReferences[startPosition + outputPosition];
            Streams sourceStreams = pages.get(pageIndex(firstRow)).columns()[outputIndex];
            if (!sourceStreams.has(stream)) {
                outputPosition++;
                continue;
            }

            int groupStart = outputPosition;
            int groupPageIndex = pageIndex(firstRow);
            while (outputPosition < batchSize && pageIndex(rowReferences[startPosition + outputPosition]) == groupPageIndex) {
                outputPosition++;
            }

            int groupSize = outputPosition - groupStart;
            ensureBatchPositions(groupSize);
            for (int index = 0; index < groupSize; index++) {
                batchPositions[index] = pagePosition(rowReferences[startPosition + groupStart + index]);
            }
            result = sourceStreams.get(stream).copyPositionsInto(
                    allocator,
                    allocationContext,
                    result,
                    batchPositions,
                    groupSize,
                    groupStart,
                    batchSize);
        }
        return result == null
                ? sourceSchema[outputIndex].get(stream).emptyLike(allocator, allocationContext)
                : result;
    }

    private Vector materializeWindowStreamBatch(int functionIndex, Stream stream, int batchSize, int[] positions)
    {
        return windowOutputs[functionIndex].get(stream).copyPositionsInto(
                allocator,
                allocationContext,
                null,
                positions,
                batchSize,
                0,
                batchSize);
    }

    private int[] borrowLazyBatchPositions(int batchSize)
    {
        if (recycledLazyBatchPositions != null && recycledLazyBatchPositions.length == batchSize) {
            int[] positions = recycledLazyBatchPositions;
            recycledLazyBatchPositions = null;
            return positions;
        }
        return arrayPool.borrowInts(batchSize);
    }

    private void releaseLazyBatchPositions(int[] positions)
    {
        if (positions == null) {
            return;
        }
        if (recycledLazyBatchPositions == null && positions.length == policy.maxBatchRows()) {
            recycledLazyBatchPositions = positions;
            return;
        }
        arrayPool.release(positions);
    }

    private final class LazyBatchState
            implements AutoCloseable
    {
        private static final int NO_POSITIONS = 0;
        private static final int SOURCE_POSITIONS = 1;
        private static final int WINDOW_POSITIONS = 2;

        private final int startPosition;
        private final int batchSize;
        private int[] positions;
        private int positionMode;

        private LazyBatchState(int startPosition, int batchSize)
        {
            this.startPosition = startPosition;
            this.batchSize = batchSize;
        }

        private Vector materializeSourceStream(int outputIndex, Stream stream)
        {
            int[] sourcePositions = null;
            if (singlePage) {
                sourcePositions = positions();
            }
            if (singlePage && positionMode != SOURCE_POSITIONS) {
                if (singlePageIdentityOrder) {
                    for (int index = 0; index < batchSize; index++) {
                        sourcePositions[index] = startPosition + index;
                    }
                }
                else {
                    System.arraycopy(singlePageOrder, startPosition, sourcePositions, 0, batchSize);
                }
                positionMode = SOURCE_POSITIONS;
            }
            return materializeSourceStreamBatch(outputIndex, stream, startPosition, batchSize, sourcePositions);
        }

        private Vector materializeWindowStream(int functionIndex, Stream stream)
        {
            int[] windowPositions = positions();
            if (positionMode != WINDOW_POSITIONS) {
                for (int index = 0; index < batchSize; index++) {
                    windowPositions[index] = startPosition + index;
                }
                positionMode = WINDOW_POSITIONS;
            }
            return materializeWindowStreamBatch(functionIndex, stream, batchSize, windowPositions);
        }

        private int[] positions()
        {
            if (positions == null) {
                positions = borrowLazyBatchPositions(batchSize);
                positionMode = NO_POSITIONS;
            }
            return positions;
        }

        @Override
        public void close()
        {
            releaseLazyBatchPositions(positions);
            positions = null;
            positionMode = NO_POSITIONS;
        }
    }

    private void ensureBatchPositions(int size)
    {
        if (batchPositions == null || batchPositions.length < size) {
            int[] previous = batchPositions;
            batchPositions = arrayPool.borrowInts(size);
            arrayPool.release(previous);
            accountRetainedArrays();
        }
    }

    private void accountRetainedArrays()
    {
        allocator.setRetainedBytes(
                allocationContext,
                this,
                (singlePageOrder == null ? 0 : (long) singlePageOrder.length * Integer.BYTES) +
                        (batchPositions == null ? 0 : (long) batchPositions.length * Integer.BYTES) +
                        (rowReferences == null ? 0 : (long) rowReferences.length * Long.BYTES) +
                        partitionPositionIndex.retainedBytes());
    }

    private long[] rowReferences(List<TableOperator.Page> pages)
    {
        int rowCount = 0;
        for (TableOperator.Page page : pages) {
            rowCount = Math.addExact(rowCount, page.mask().count());
        }
        long[] rows = arrayPool.borrowLongs(rowCount);
        int rowIndex = 0;
        for (int pageIndex = 0; pageIndex < pages.size(); pageIndex++) {
            TableOperator.Page page = pages.get(pageIndex);
            if (page.mask().all()) {
                for (int position = 0; position < page.rows(); position++) {
                    rows[rowIndex++] = rowReference(pageIndex, position);
                }
                continue;
            }
            for (int index = 0; index < page.mask().selectedCount(); index++) {
                rows[rowIndex++] = rowReference(pageIndex, page.mask().position(index));
            }
        }
        return rows;
    }

    private void stableSortRowReferences(long[] rows)
    {
        if (rows.length < 2) {
            return;
        }
        if (policy.radixSort() && tryStableRadixSortRowReferences(rows)) {
            return;
        }
        long[] scratch = arrayPool.borrowLongs(rows.length);
        try {
            long[] source = rows;
            long[] target = scratch;
            for (int width = 1; width < rows.length; width = width > rows.length / 2 ? rows.length : width * 2) {
                int step = width > rows.length - width ? rows.length : width * 2;
                for (int start = 0; start < rows.length; start += step) {
                    int middle = start + Math.min(width, rows.length - start);
                    int end = start + Math.min(step, rows.length - start);
                    int left = start;
                    int right = middle;
                    int output = start;
                    while (left < middle && right < end) {
                        target[output++] = compareRows(source[left], source[right]) <= 0
                                ? source[left++]
                                : source[right++];
                    }
                    while (left < middle) {
                        target[output++] = source[left++];
                    }
                    while (right < end) {
                        target[output++] = source[right++];
                    }
                }
                long[] swap = source;
                source = target;
                target = swap;
            }
            if (source != rows) {
                System.arraycopy(source, 0, rows, 0, rows.length);
            }
        }
        finally {
            arrayPool.release(scratch);
        }
    }

    /** Stable LSD radix ordering over packed page/position references for flat integer window keys. */
    private boolean tryStableRadixSortRowReferences(long[] rows)
    {
        if (!allowsLegacyOrderingShortcuts) {
            return false;
        }
        for (TableOperator.Page page : pages) {
            for (int column : partitionColumns) {
                if (!isFlatIntegerSortKey(page.columns()[column])) {
                    return false;
                }
            }
            for (int column : orderingColumns) {
                if (!isFlatIntegerSortKey(page.columns()[column])) {
                    return false;
                }
            }
        }

        long[] scratch = arrayPool.borrowLongs(rows.length);
        try {
            for (int index = orderingColumns.length - 1; index >= 0; index--) {
                stableRadixSortRowReferenceColumn(
                        rows,
                        scratch,
                        orderingColumns[index],
                        descendingByColumn[index],
                        nullsFirstByColumn[index]);
            }
            for (int index = partitionColumns.length - 1; index >= 0; index--) {
                stableRadixSortRowReferenceColumn(rows, scratch, partitionColumns[index], false, false);
            }
        }
        finally {
            arrayPool.release(scratch);
        }
        return true;
    }

    private void stableRadixSortRowReferenceColumn(long[] rows, long[] scratch, int column, boolean descending, boolean nullsFirst)
    {
        int bytes = pages.getFirst().columns()[column].values() instanceof I64Vector ? Long.BYTES : Integer.BYTES;
        long firstKey = 0;
        long varyingBytes = 0;
        boolean first = true;
        boolean hasNull = false;
        for (long row : rows) {
            Streams streams = pages.get(pageIndex(row)).columns()[column];
            int position = pagePosition(row);
            if (OperatorVectorSupport.isNull(streams.getOrNull(Stream.NULLS), position)) {
                hasNull = true;
                continue;
            }
            long key = integerSortKey(streams.values(), position, descending);
            if (first) {
                firstKey = key;
                first = false;
            }
            else {
                varyingBytes |= firstKey ^ key;
            }
        }
        if (first) {
            return;
        }

        long[] source = rows;
        long[] target = scratch;
        for (int byteIndex = 0; byteIndex < bytes; byteIndex++) {
            int shift = byteIndex * Byte.SIZE;
            if (((varyingBytes >>> shift) & 0xFF) == 0) {
                continue;
            }
            java.util.Arrays.fill(radixCounts, 0);
            for (long row : source) {
                Streams streams = pages.get(pageIndex(row)).columns()[column];
                int position = pagePosition(row);
                int bucket = OperatorVectorSupport.isNull(streams.getOrNull(Stream.NULLS), position)
                        ? 0
                        : integerSortByte(streams.values(), position, shift, descending);
                radixCounts[bucket]++;
            }
            int offset = 0;
            for (int bucket = 0; bucket < radixCounts.length; bucket++) {
                int count = radixCounts[bucket];
                radixCounts[bucket] = offset;
                offset += count;
            }
            for (long row : source) {
                Streams streams = pages.get(pageIndex(row)).columns()[column];
                int position = pagePosition(row);
                int bucket = OperatorVectorSupport.isNull(streams.getOrNull(Stream.NULLS), position)
                        ? 0
                        : integerSortByte(streams.values(), position, shift, descending);
                target[radixCounts[bucket]++] = row;
            }
            long[] swap = source;
            source = target;
            target = swap;
        }
        if (hasNull) {
            int nullCount = 0;
            for (long row : source) {
                Streams streams = pages.get(pageIndex(row)).columns()[column];
                if (OperatorVectorSupport.isNull(streams.getOrNull(Stream.NULLS), pagePosition(row))) {
                    nullCount++;
                }
            }
            int nullOffset = nullsFirst ? 0 : source.length - nullCount;
            int valueOffset = nullsFirst ? nullCount : 0;
            for (long row : source) {
                Streams streams = pages.get(pageIndex(row)).columns()[column];
                if (OperatorVectorSupport.isNull(streams.getOrNull(Stream.NULLS), pagePosition(row))) {
                    target[nullOffset++] = row;
                }
                else {
                    target[valueOffset++] = row;
                }
            }
            source = target;
        }
        if (source != rows) {
            System.arraycopy(source, 0, rows, 0, rows.length);
        }
    }

    private static long rowReference(int pageIndex, int position)
    {
        return ((long) pageIndex << Integer.SIZE) | Integer.toUnsignedLong(position);
    }

    private static int pageIndex(long rowReference)
    {
        return (int) (rowReference >>> Integer.SIZE);
    }

    private static int pagePosition(long rowReference)
    {
        return (int) rowReference;
    }

    private final class PartitionPositionIndex
            implements WindowPositionIndex
    {
        private int start;
        private int end;
        private int[] peerBounds;
        private boolean initialized;

        private void reset(int start, int end)
        {
            if (start < 0 || start > end || end > rowCount()) {
                throw new IndexOutOfBoundsException("Invalid partition range [%s, %s) for %s rows".formatted(start, end, rowCount()));
            }
            if (initialized && this.start == start && this.end == end) {
                return;
            }
            arrayPool.release(peerBounds);
            peerBounds = null;
            this.start = start;
            this.end = end;
            initialized = true;
            accountRetainedArrays();
        }

        @Override
        public int size()
        {
            return end - start;
        }

        @Override
        public Streams column(int column, int position)
        {
            int absolutePosition = absolutePosition(position);
            if (singlePage) {
                return pages.getFirst().columns()[column];
            }
            return pages.get(pageIndex(rowReferences[absolutePosition])).columns()[column];
        }

        @Override
        public int sourcePosition(int position)
        {
            int absolutePosition = absolutePosition(position);
            if (singlePage) {
                return singlePageIdentityOrder ? absolutePosition : singlePageOrder[absolutePosition];
            }
            return pagePosition(rowReferences[absolutePosition]);
        }

        @Override
        public boolean sharesSource(int leftPosition, int rightPosition)
        {
            int leftAbsolutePosition = absolutePosition(leftPosition);
            int rightAbsolutePosition = absolutePosition(rightPosition);
            return singlePage || pageIndex(rowReferences[leftAbsolutePosition]) == pageIndex(rowReferences[rightAbsolutePosition]);
        }

        @Override
        public int peerStart(int position)
        {
            absolutePosition(position);
            ensurePeerBounds();
            return peerBounds[position * 2];
        }

        @Override
        public int peerEnd(int position)
        {
            absolutePosition(position);
            ensurePeerBounds();
            return peerBounds[position * 2 + 1];
        }

        @Override
        public int compareNonNull(int leftColumn, int leftPosition, int rightColumn, int rightPosition)
        {
            absolutePosition(leftPosition);
            absolutePosition(rightPosition);
            var leftType = source.outputSchema().field(leftColumn).type();
            var rightType = source.outputSchema().field(rightColumn).type();
            if (!leftType.identity().equals(rightType.identity())) {
                throw new IllegalArgumentException("Window comparison columns have different logical types");
            }
            Streams left = column(leftColumn, leftPosition);
            Streams right = column(rightColumn, rightPosition);
            int leftSourcePosition = sourcePosition(leftPosition);
            int rightSourcePosition = sourcePosition(rightPosition);
            if (OperatorVectorSupport.isNull(left.getOrNull(Stream.NULLS), leftSourcePosition) ||
                    OperatorVectorSupport.isNull(right.getOrNull(Stream.NULLS), rightSourcePosition)) {
                throw new IllegalArgumentException("Window comparison values must be non-null");
            }
            StructuralComparisonKernel comparison = comparisonKernels[leftColumn];
            if (comparison == null) {
                comparison = structuralTypes.comparison(leftType);
                comparisonKernels[leftColumn] = comparison;
            }
            return comparison.compare(
                    left.values(),
                    left.getOrNull(Stream.NULLS),
                    leftSourcePosition,
                    right.values(),
                    right.getOrNull(Stream.NULLS),
                    rightSourcePosition);
        }

        private void ensurePeerBounds()
        {
            if (peerBounds != null) {
                return;
            }
            peerBounds = arrayPool.borrowInts(Math.multiplyExact(size(), 2));
            int peerStart = 0;
            while (peerStart < size()) {
                int peerEnd = peerStart + 1;
                while (peerEnd < size() && samePeer(peerEnd - 1, peerEnd)) {
                    peerEnd++;
                }
                for (int position = peerStart; position < peerEnd; position++) {
                    peerBounds[position * 2] = peerStart;
                    peerBounds[position * 2 + 1] = peerEnd;
                }
                peerStart = peerEnd;
            }
            accountRetainedArrays();
        }

        private boolean samePeer(int leftPosition, int rightPosition)
        {
            for (int orderingIndex = 0; orderingIndex < orderingColumns.length; orderingIndex++) {
                int column = orderingColumns[orderingIndex];
                Streams left = column(column, leftPosition);
                Streams right = column(column, rightPosition);
                if (compareOrderingValues(
                        orderingIndex,
                        left,
                        sourcePosition(leftPosition),
                        right,
                        sourcePosition(rightPosition)) != 0) {
                    return false;
                }
            }
            return true;
        }

        private int absolutePosition(int position)
        {
            if (position < 0 || position >= size()) {
                throw new IndexOutOfBoundsException(position);
            }
            return start + position;
        }

        private long retainedBytes()
        {
            return peerBounds == null ? 0 : (long) peerBounds.length * Integer.BYTES;
        }

        private void close()
        {
            arrayPool.release(peerBounds);
            peerBounds = null;
        }
    }

    private Streams emptyStreamsLike(Output output)
    {
        Streams.Builder builder = Streams.builder();
        for (Stream stream : output.streams()) {
            builder.put(stream, output.borrow(stream).emptyLike(allocator, allocationContext));
        }
        return builder.build();
    }

    private static Streams borrowedStreams(Output output)
    {
        Streams.Builder builder = Streams.builder();
        for (Stream stream : output.streams()) {
            builder.put(stream, output.borrow(stream));
        }
        return builder.build();
    }

    private Streams takeStreams(Output output)
    {
        Streams.Builder builder = Streams.builder();
        for (Stream stream : output.streams()) {
            builder.put(stream, allocator.transfer(allocationContext, output.take(stream)));
        }
        return builder.build();
    }

    private void closeRetainedInputBatches()
    {
        RuntimeException failure = null;
        for (Batch batch : retainedInputBatches) {
            try {
                batch.close();
            }
            catch (RuntimeException closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                }
                else {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
        retainedInputBatches.clear();
        if (failure != null) {
            throw failure;
        }
    }

    private record FlatIntegerOrderKey(long[] longs, int[] integers, boolean[] nulls, boolean descending, boolean nullsFirst) {}
}
