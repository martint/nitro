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
import org.weakref.nitro.core.function.aggregation.AggregationExecution;
import org.weakref.nitro.core.function.aggregation.AggregationImplementation;
import org.weakref.nitro.core.function.aggregation.AggregationInput;
import org.weakref.nitro.core.function.aggregation.AggregationPositionAccumulator;
import org.weakref.nitro.core.function.aggregation.AggregationWindowFrameBounds;
import org.weakref.nitro.core.function.aggregation.AggregationWindowPartition;
import org.weakref.nitro.core.function.aggregation.PrimitiveRangeConsumer;
import org.weakref.nitro.core.function.aggregation.PrimitiveRangeContribution;
import org.weakref.nitro.core.function.aggregation.ReversibleAggregationPositionAccumulator;
import org.weakref.nitro.core.function.aggregation.ReversibleAggregationWindowKernel;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * Adapts one dynamically resolved aggregate to an exact running or whole-partition window.
 *
 * <p>The aggregate implementation owns all function and type semantics. This adapter owns only
 * the one-group window lifecycle, physical input mapping, and generic result-stream placement.
 */
public final class RegisteredAggregationWindowFunction
        implements RunningWindowFunction
{
    public static final String KERNEL_POSITIONS = "nitro.window-kernel-positions";
    public static final String KERNEL_ADDITIONS = "nitro.window-kernel-additions";
    public static final String KERNEL_REMOVALS = "nitro.window-kernel-removals";
    public static final String KERNEL_RESULTS = "nitro.window-kernel-results";
    private final AggregationImplementation implementation;
    private final Schema inputSchema;
    private final int[] inputColumns;
    private final Frame frame;
    private final WindowFrame positionFrame;
    private final int[] orderingColumns;
    private final StructuralComparisonKernel[] orderingKernels;
    private final int[] activePosition = new int[1];
    private final WindowFrame.Bounds positionBounds = new WindowFrame.Bounds();
    private final WindowFrame.Bounds previousPositionBounds = new WindowFrame.Bounds();
    private final Streams[] framedSourceColumns;

    private Object state;
    private Streams result;
    private Streams[] boundSourceColumns;
    private AggregationInput boundInput;
    private AggregationPositionAccumulator boundPositionAccumulator;
    private ReversibleAggregationPositionAccumulator boundReversiblePositionAccumulator;
    private ReversibleAggregationWindowKernel reversibleWindowKernel;
    private Streams[] previousColumns;
    private int previousPosition;
    private int peerStartOutputPosition;
    private WindowPositionIndex boundPartition;
    private int boundPartitionPosition = -1;
    private long kernelPositions;
    private long kernelAdditions;
    private long kernelRemovals;
    private long kernelResults;

    public RegisteredAggregationWindowFunction(
            AggregationImplementation implementation,
            Schema inputSchema,
            int[] inputColumns,
            Frame frame)
    {
        this(implementation, inputSchema, inputColumns, frame, new int[0]);
    }

    public RegisteredAggregationWindowFunction(
            AggregationImplementation implementation,
            Schema inputSchema,
            int[] inputColumns,
            Frame frame,
            int[] orderingColumns)
    {
        this(implementation, inputSchema, inputColumns, requireNonNull(frame, "frame is null"), null, orderingColumns);
    }

    public RegisteredAggregationWindowFunction(
            AggregationImplementation implementation,
            Schema inputSchema,
            int[] inputColumns,
            WindowFrame frame)
    {
        this(implementation, inputSchema, inputColumns, null, requireNonNull(frame, "frame is null"), new int[0]);
    }

    private RegisteredAggregationWindowFunction(
            AggregationImplementation implementation,
            Schema inputSchema,
            int[] inputColumns,
            Frame frame,
            WindowFrame positionFrame,
            int[] orderingColumns)
    {
        this.implementation = requireNonNull(implementation, "implementation is null");
        this.inputSchema = requireNonNull(inputSchema, "inputSchema is null");
        this.inputColumns = requireNonNull(inputColumns, "inputColumns is null").clone();
        this.frame = frame;
        this.positionFrame = positionFrame;
        this.framedSourceColumns = new Streams[inputSchema.size()];
        this.orderingColumns = requireNonNull(orderingColumns, "orderingColumns is null").clone();
        if (Arrays.stream(this.inputColumns).anyMatch(column -> column < 0 || column >= inputSchema.size())) {
            throw new IllegalArgumentException("aggregate input column is outside the input schema");
        }
        if (Arrays.stream(this.orderingColumns).anyMatch(column -> column < 0 || column >= inputSchema.size())) {
            throw new IllegalArgumentException("aggregate ordering column is outside the input schema");
        }
        if ((frame == Frame.RUNNING_PEERS) != (this.orderingColumns.length > 0)) {
            throw new IllegalArgumentException("peer-running aggregate requires one or more ordering columns only");
        }
        StructuralTypeKernelFactory structuralTypes = new StructuralTypeKernelFactory();
        this.orderingKernels = Arrays.stream(this.orderingColumns)
                .mapToObj(column -> structuralTypes.comparison(inputSchema.field(column).type()))
                .toArray(StructuralComparisonKernel[]::new);
    }

    @Override
    public Streams emptyOutput(Allocator allocator, Allocator.Context allocationContext, int size)
    {
        if (state != null) {
            throw new IllegalStateException("aggregate window output is already initialized");
        }
        state = implementation.allocate(
                new AggregationExecution(allocator, allocationContext, inputSchema),
                1);
        implementation.initialize(state, 0, 1);
        result = implementation.result(0, state, Streams.empty(), allocator, allocationContext);

        Streams.Builder output = Streams.builder();
        for (Stream stream : result.streams()) {
            output.put(stream, result.get(stream).emptyLike(allocator, allocationContext));
        }
        return output.build();
    }

    @Override
    public void reset()
    {
        requireInitialized();
        implementation.initialize(state, 0, 1);
        previousColumns = null;
        previousPosition = -1;
        peerStartOutputPosition = -1;
        boundPartition = null;
        boundPartitionPosition = -1;
        positionBounds.clear();
        previousPositionBounds.clear();
    }

    @Override
    public Streams append(
            Allocator allocator,
            Allocator.Context allocationContext,
            Streams output,
            Streams[] sourceColumns,
            int inputPosition,
            int outputPosition,
            int outputSize)
    {
        if (positionFrame != null) {
            return output;
        }
        if (frame == Frame.RUNNING_PEERS) {
            if (previousColumns == null) {
                peerStartOutputPosition = outputPosition;
            }
            else if (orderingChanged(previousColumns, previousPosition, sourceColumns, inputPosition)) {
                output = copyCurrentResultRange(
                        allocator,
                        allocationContext,
                        output,
                        peerStartOutputPosition,
                        outputPosition,
                        outputSize);
                peerStartOutputPosition = outputPosition;
            }
        }
        addPosition(allocator, allocationContext, sourceColumns, inputPosition);
        if (frame == Frame.RUNNING_ROWS) {
            Streams direct = implementation.copyResultPosition(
                    0,
                    0,
                    state,
                    output,
                    outputPosition,
                    outputSize,
                    allocator,
                    allocationContext);
            if (direct != null) {
                return direct;
            }
            result = implementation.result(0, state, result, allocator, allocationContext);
            output = copyResultPosition(allocator, allocationContext, result, output, outputPosition, outputSize);
        }
        previousColumns = sourceColumns;
        previousPosition = inputPosition;
        return output;
    }

    private void addPosition(
            Allocator allocator,
            Allocator.Context allocationContext,
            Streams[] sourceColumns,
            int inputPosition)
    {
        bindInput(sourceColumns);
        if (boundReversiblePositionAccumulator != null) {
            boundReversiblePositionAccumulator.add(inputPosition);
        }
        else if (boundPositionAccumulator != null) {
            boundPositionAccumulator.add(inputPosition);
        }
        else {
            activePosition[0] = inputPosition;
            Mask activeMask = allocator.allocateSparseMask(
                    allocationContext,
                    activePosition,
                    1,
                    inputSize(sourceColumns, inputPosition));
            try {
                implementation.addRawInput(state, 0, activeMask, boundInput);
            }
            finally {
                allocator.release(allocationContext, activeMask);
            }
        }
    }

    private void bindInput(Streams[] sourceColumns)
    {
        if (sourceColumns == boundSourceColumns) {
            return;
        }
        rebindInput(sourceColumns);
    }

    private void rebindInput(Streams[] sourceColumns)
    {
        boundSourceColumns = sourceColumns;
        boundInput = (argument, stream) -> {
            if (argument < 0 || argument >= inputColumns.length) {
                throw new IndexOutOfBoundsException("aggregate input: " + argument);
            }
            return sourceColumns[inputColumns[argument]].getOrNull(stream);
        };
        boundPositionAccumulator = implementation.bindRawInputPosition(state, 0, boundInput);
        boundReversiblePositionAccumulator = implementation.bindReversibleRawInputPosition(state, 0, boundInput);
    }

    @Override
    public Streams finishPartition(
            Allocator allocator,
            Allocator.Context allocationContext,
            Streams output,
            int partitionStart,
            int partitionEnd,
            int outputSize)
    {
        if (frame == Frame.RUNNING_ROWS || partitionStart == partitionEnd) {
            return output;
        }
        if (frame == Frame.RUNNING_PEERS) {
            return copyCurrentResultRange(
                    allocator,
                    allocationContext,
                    output,
                    peerStartOutputPosition,
                    partitionEnd,
                    outputSize);
        }
        result = implementation.result(0, state, result, allocator, allocationContext);
        return copyResultRange(allocator, allocationContext, result, output, partitionStart, partitionEnd, outputSize);
    }

    @Override
    public Streams finishPartition(
            Allocator allocator,
            Allocator.Context allocationContext,
            Streams output,
            WindowPositionIndex partition,
            int partitionStart,
            int outputSize)
    {
        if (positionFrame == null) {
            return RunningWindowFunction.super.finishPartition(
                    allocator,
                    allocationContext,
                    output,
                    partition,
                    partitionStart,
                    outputSize);
        }

        WindowFrame.Bounds previousBounds = new WindowFrame.Bounds();
        for (int outputPosition = 0; outputPosition < partition.size(); outputPosition++) {
            previousBounds.setFrom(positionBounds);
            positionBounds.clear();
            positionFrame.resolve(partition, outputPosition, positionBounds);
            if (!updateFrame(allocator, allocationContext, partition, previousBounds)) {
                implementation.initialize(state, 0, 1);
                invalidateInputBinding();
                addRange(allocator, allocationContext, partition, positionBounds);
            }
            Streams direct = implementation.copyResultPosition(
                    0,
                    0,
                    state,
                    output,
                    partitionStart + outputPosition,
                    outputSize,
                    allocator,
                    allocationContext);
            if (direct != null) {
                output = direct;
            }
            else {
                result = implementation.result(0, state, result, allocator, allocationContext);
                output = copyResultPosition(
                        allocator,
                        allocationContext,
                        result,
                        output,
                        partitionStart + outputPosition,
                        outputSize);
            }
        }
        return output;
    }

    @Override
    public boolean supportsForwardBatchRangeMaterialization()
    {
        return positionFrame != null;
    }

    @Override
    public PrimitiveRangeContribution primitiveRangeContribution()
    {
        return positionFrame == null ? null : implementation.primitiveWindowResultContribution(inputColumns.length);
    }

    @Override
    public void emitForwardPrimitiveRange(
            Allocator allocator,
            Allocator.Context allocationContext,
            WindowPositionIndex partition,
            int rangeStart,
            int rangeEnd,
            AggregationWindowFrameBounds bounds,
            PrimitiveRangeConsumer consumer)
    {
        requireNonNull(consumer, "consumer is null");
        if (state == null) {
            state = implementation.allocate(new AggregationExecution(allocator, allocationContext, inputSchema), 1);
            implementation.initialize(state, 0, 1);
            reversibleWindowKernel = implementation.bindReversibleWindowKernel(state, 0, inputColumns.length);
        }
        if (rangeStart == 0) {
            reset();
            reversibleWindowKernel.reset();
        }
        ReversibleAggregationWindowKernel.Work work = reversibleWindowKernel.process(
                aggregationWindowPartition(partition),
                bounds,
                rangeStart,
                rangeEnd - rangeStart,
                consumer);
        kernelPositions += work.positions();
        kernelAdditions += work.additions();
        kernelRemovals += work.removals();
        kernelResults += work.results();
    }

    @Override
    public Streams materializeForwardBatchRange(
            Allocator allocator,
            Allocator.Context allocationContext,
            Streams output,
            WindowPositionIndex partition,
            int rangeStart,
            int rangeEnd,
            int destinationStart,
            int destinationSize)
    {
        AggregationWindowFrameBounds bounds = bindForwardBatchBounds(partition).bind(rangeStart, rangeEnd - rangeStart);
        return materializeForwardBatchRange(
                allocator,
                allocationContext,
                output,
                partition,
                rangeStart,
                rangeEnd,
                destinationStart,
                destinationSize,
                bounds);
    }

    @Override
    public Object frameTraversalIdentity()
    {
        return positionFrame == null ? this : positionFrame.traversalIdentity();
    }

    @Override
    public WindowFrame.Cursor bindForwardBatchBounds(WindowPositionIndex partition)
    {
        return positionFrame.bind(partition);
    }

    @Override
    public Streams materializeForwardBatchRange(
            Allocator allocator,
            Allocator.Context allocationContext,
            Streams output,
            WindowPositionIndex partition,
            int rangeStart,
            int rangeEnd,
            int destinationStart,
            int destinationSize,
            AggregationWindowFrameBounds bounds)
    {
        if (positionFrame == null) {
            throw new UnsupportedOperationException("forward batch-range materialization requires a positional frame");
        }
        if (rangeStart < 0 || rangeStart > rangeEnd || rangeEnd > partition.size()) {
            throw new IndexOutOfBoundsException("Invalid partition range [%s, %s) for %s positions"
                    .formatted(rangeStart, rangeEnd, partition.size()));
        }
        if (destinationStart < 0 || destinationStart + rangeEnd - rangeStart > destinationSize) {
            throw new IndexOutOfBoundsException("Invalid destination range [%s, %s) for %s positions"
                    .formatted(destinationStart, destinationStart + rangeEnd - rangeStart, destinationSize));
        }

        if (state == null) {
            output = emptyOutput(allocator, allocationContext, destinationSize);
            reversibleWindowKernel = implementation.bindReversibleWindowKernel(state, 0, inputColumns.length);
        }
        else if (output == null) {
            output = emptyBatchOutput(allocator, allocationContext);
        }
        if (rangeStart == 0) {
            reset();
            if (reversibleWindowKernel != null) {
                reversibleWindowKernel.reset();
            }
        }

        if (reversibleWindowKernel != null) {
            output = reversibleWindowKernel.prepareOutput(output, destinationSize, allocator, allocationContext);
            ReversibleAggregationWindowKernel.Work work = reversibleWindowKernel.process(
                    aggregationWindowPartition(partition),
                    bounds,
                    rangeStart,
                    rangeEnd - rangeStart,
                    output,
                    destinationStart);
            kernelPositions += work.positions();
            kernelAdditions += work.additions();
            kernelRemovals += work.removals();
            kernelResults += work.results();
            return output;
        }

        for (int partitionPosition = rangeStart; partitionPosition < rangeEnd; partitionPosition++) {
            previousPositionBounds.setFrom(positionBounds);
            if (bounds.start(partitionPosition) < 0) {
                positionBounds.clear();
            }
            else {
                positionBounds.set(bounds.start(partitionPosition), bounds.end(partitionPosition));
            }
            if (!updateFrame(allocator, allocationContext, partition, previousPositionBounds)) {
                implementation.initialize(state, 0, 1);
                invalidateInputBinding();
                addRange(allocator, allocationContext, partition, positionBounds);
            }
            int outputPosition = destinationStart + partitionPosition - rangeStart;
            Streams direct = implementation.copyResultPosition(
                    0,
                    0,
                    state,
                    output,
                    outputPosition,
                    destinationSize,
                    allocator,
                    allocationContext);
            if (direct != null) {
                output = direct;
            }
            else {
                result = implementation.result(0, state, result, allocator, allocationContext);
                output = copyResultPosition(
                        allocator,
                        allocationContext,
                        result,
                        output,
                        outputPosition,
                        destinationSize);
            }
        }
        return output;
    }

    @Override
    public void reportDiagnostics(ExecutionDiagnostics diagnostics)
    {
        diagnostics.record(KERNEL_POSITIONS, kernelPositions);
        diagnostics.record(KERNEL_ADDITIONS, kernelAdditions);
        diagnostics.record(KERNEL_REMOVALS, kernelRemovals);
        diagnostics.record(KERNEL_RESULTS, kernelResults);
        kernelPositions = 0;
        kernelAdditions = 0;
        kernelRemovals = 0;
        kernelResults = 0;
    }

    private AggregationWindowPartition aggregationWindowPartition(WindowPositionIndex partition)
    {
        return new AggregationWindowPartition()
        {
            @Override
            public int size()
            {
                return partition.size();
            }

            @Override
            public org.weakref.nitro.data.Vector stream(int argument, Stream stream, int partitionPosition)
            {
                return partition.column(inputColumns[argument], partitionPosition).getOrNull(stream);
            }

            @Override
            public int sourcePosition(int partitionPosition)
            {
                return partition.sourcePosition(partitionPosition);
            }

            @Override
            public boolean sharesSource(int leftPosition, int rightPosition)
            {
                return partition.sharesSource(leftPosition, rightPosition);
            }
        };
    }

    private Streams emptyBatchOutput(Allocator allocator, Allocator.Context allocationContext)
    {
        Streams.Builder output = Streams.builder();
        for (Stream stream : result.streams()) {
            output.put(stream, result.get(stream).emptyLike(allocator, allocationContext));
        }
        return output.build();
    }

    private boolean updateFrame(
            Allocator allocator,
            Allocator.Context allocationContext,
            WindowPositionIndex partition,
            WindowFrame.Bounds previousBounds)
    {
        if (!previousBounds.present() || !positionBounds.present() ||
                !implementation.supportsReversibleRawInputPosition() ||
                positionBounds.start() < previousBounds.start() || positionBounds.end() < previousBounds.end()) {
            return false;
        }
        for (int position = previousBounds.start(); position < Math.min(previousBounds.end(), positionBounds.start()); position++) {
            removePosition(partition, position);
        }
        addRange(
                allocator,
                allocationContext,
                partition,
                Math.max(previousBounds.end(), positionBounds.start()),
                positionBounds.end());
        return true;
    }

    private void addRange(
            Allocator allocator,
            Allocator.Context allocationContext,
            WindowPositionIndex partition,
            WindowFrame.Bounds bounds)
    {
        if (bounds.present()) {
            addRange(allocator, allocationContext, partition, bounds.start(), bounds.end());
        }
    }

    private void addRange(
            Allocator allocator,
            Allocator.Context allocationContext,
            WindowPositionIndex partition,
            int start,
            int end)
    {
        for (int position = start; position < end; position++) {
            bindPartitionPosition(partition, position);
            addPosition(allocator, allocationContext, framedSourceColumns, partition.sourcePosition(position));
        }
    }

    private void removePosition(WindowPositionIndex partition, int position)
    {
        bindPartitionPosition(partition, position);
        if (boundReversiblePositionAccumulator == null) {
            throw new IllegalStateException("reversible aggregation binding is unavailable");
        }
        boundReversiblePositionAccumulator.remove(partition.sourcePosition(position));
    }

    private void bindPartitionPosition(WindowPositionIndex partition, int position)
    {
        if (boundPartition != partition || boundPartitionPosition < 0 || !partition.sharesSource(boundPartitionPosition, position)) {
            for (int column : inputColumns) {
                framedSourceColumns[column] = partition.column(column, position);
            }
            rebindInput(framedSourceColumns);
        }
        boundPartition = partition;
        boundPartitionPosition = position;
    }

    private void invalidateInputBinding()
    {
        boundSourceColumns = null;
        boundInput = null;
        boundPositionAccumulator = null;
        boundReversiblePositionAccumulator = null;
        boundPartition = null;
        boundPartitionPosition = -1;
    }

    private Streams copyCurrentResultRange(
            Allocator allocator,
            Allocator.Context allocationContext,
            Streams output,
            int outputStart,
            int outputEnd,
            int outputSize)
    {
        result = implementation.result(0, state, result, allocator, allocationContext);
        return copyResultRange(allocator, allocationContext, result, output, outputStart, outputEnd, outputSize);
    }

    private boolean orderingChanged(
            Streams[] leftColumns,
            int leftPosition,
            Streams[] rightColumns,
            int rightPosition)
    {
        for (int orderingIndex = 0; orderingIndex < orderingColumns.length; orderingIndex++) {
            Streams left = leftColumns[orderingColumns[orderingIndex]];
            Streams right = rightColumns[orderingColumns[orderingIndex]];
            boolean leftNull = OperatorVectorSupport.isNull(left.getOrNull(Stream.NULLS), leftPosition);
            boolean rightNull = OperatorVectorSupport.isNull(right.getOrNull(Stream.NULLS), rightPosition);
            if (leftNull || rightNull) {
                if (leftNull != rightNull) {
                    return true;
                }
                continue;
            }
            if (orderingKernels[orderingIndex].compare(
                    left.values(),
                    left.getOrNull(Stream.NULLS),
                    leftPosition,
                    right.values(),
                    right.getOrNull(Stream.NULLS),
                    rightPosition) != 0) {
                return true;
            }
        }
        return false;
    }

    private void requireInitialized()
    {
        if (state == null) {
            throw new IllegalStateException("aggregate window output is not initialized");
        }
    }

    private static int inputSize(Streams[] sourceColumns, int inputPosition)
    {
        for (Streams column : sourceColumns) {
            if (column != null && column.vectorCount() > 0) {
                return column.vectorAt(0).length();
            }
        }
        return inputPosition + 1;
    }

    private static Streams copyResultPosition(
            Allocator allocator,
            Allocator.Context allocationContext,
            Streams source,
            Streams target,
            int outputPosition,
            int outputSize)
    {
        return allocator.copySinglePositionInto(
                allocationContext,
                source,
                target,
                0,
                outputPosition,
                outputSize);
    }

    private static Streams copyResultRange(
            Allocator allocator,
            Allocator.Context allocationContext,
            Streams source,
            Streams target,
            int outputStart,
            int outputEnd,
            int outputSize)
    {
        return allocator.copySinglePositionRangeInto(
                allocationContext,
                source,
                target,
                0,
                outputStart,
                outputEnd,
                outputSize);
    }

    public enum Frame
    {
        RUNNING_ROWS,
        RUNNING_PEERS,
        FULL_PARTITION
    }
}
