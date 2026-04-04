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
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class WindowOperator
        implements Operator
{
    private static final int BATCH_SIZE = Integer.getInteger("nitro.window.maxBatchRows", 10_000);

    private final Allocator allocator;
    private final Allocator.Context allocationContext = new Allocator.Context("WindowOperator");
    private final Operator source;
    private final int[] partitionColumns;
    private final int[] orderingColumns;
    private final boolean[] descendingByColumn;
    private final List<RunningWindowFunction> windowFunctions;

    private Streams[] sourceSchema;
    private List<TableOperator.Page> pages;
    private List<RowReference> rows;
    private Streams[] windowOutputs;
    private int currentOutputPosition;
    private boolean loaded;

    public WindowOperator(Allocator allocator, Operator source, int[] partitionColumns, int[] orderingColumns, boolean[] descendingByColumn, List<RunningWindowFunction> windowFunctions)
    {
        if (orderingColumns.length != descendingByColumn.length) {
            throw new IllegalArgumentException("Ordering columns and directions must have the same length");
        }
        if (windowFunctions.isEmpty()) {
            throw new IllegalArgumentException("windowFunctions is empty");
        }
        this.allocator = allocator;
        this.source = source;
        this.partitionColumns = partitionColumns.clone();
        this.orderingColumns = orderingColumns.clone();
        this.descendingByColumn = descendingByColumn.clone();
        this.windowFunctions = List.copyOf(windowFunctions);
    }

    @Override
    public int outputCount()
    {
        return source.outputCount() + windowFunctions.size();
    }

    @Override
    public boolean hasNext()
    {
        if (!loaded) {
            load();
        }
        return currentOutputPosition < rows.size();
    }

    @Override
    public Batch next()
    {
        if (!loaded) {
            load();
        }
        int batchSize = Math.min(BATCH_SIZE, rows.size() - currentOutputPosition);
        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < source.outputCount(); outputIndex++) {
            Streams batchStreams = materializeSourceColumnBatch(outputIndex, currentOutputPosition, batchSize);
            outputs[outputIndex] = new Output(
                    batchStreams.streams(),
                    batchStreams::get,
                    (stream, vector) -> allocator.transfer(allocationContext, vector),
                    (stream, vector) -> allocator.release(allocationContext, vector));
        }
        for (int functionIndex = 0; functionIndex < windowFunctions.size(); functionIndex++) {
            Streams batchStreams = materializeWindowBatch(functionIndex, currentOutputPosition, batchSize);
            outputs[source.outputCount() + functionIndex] = new Output(
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
    public void constrain(Mask mask)
    {
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        return true;
    }

    @Override
    public void close()
    {
        source.close();
        allocator.release(allocationContext);
    }

    private void load()
    {
        loaded = true;

        pages = new ArrayList<>();
        sourceSchema = new Streams[source.outputCount()];
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
                if (source.supportsRetainedBatches()) {
                    Streams[] retainedColumns = new Streams[source.outputCount()];
                    for (int outputIndex = 0; outputIndex < retainedColumns.length; outputIndex++) {
                        retainedColumns[outputIndex] = takeStreams(batch.output(outputIndex));
                    }
                    pages.add(new TableOperator.Page(mask.count(), retainedColumns, allocator.transfer(allocationContext, batch.takeMask())));
                    continue;
                }
                Streams[] columns = new Streams[source.outputCount()];
                for (int outputIndex = 0; outputIndex < columns.length; outputIndex++) {
                    columns[outputIndex] = allocator.copyStreams(allocationContext, borrowedStreams(batch.output(outputIndex)), mask);
                }
                pages.add(new TableOperator.Page(mask.count(), columns, Mask.all(mask.count())));
            }
        }

        rows = rows(pages);
        rows.sort(this::compareRows);
        windowOutputs = new Streams[windowFunctions.size()];
        for (int functionIndex = 0; functionIndex < windowFunctions.size(); functionIndex++) {
            windowOutputs[functionIndex] = materializeWindow(windowFunctions.get(functionIndex), rows);
        }
    }

    private Streams materializeWindow(RunningWindowFunction function, List<RowReference> rows)
    {
        Streams output = function.emptyOutput(allocator, allocationContext, rows.size());
        RowReference previous = null;
        function.reset();
        int partitionStart = 0;
        for (int outputPosition = 0; outputPosition < rows.size(); outputPosition++) {
            RowReference row = rows.get(outputPosition);
            if (previous != null && !samePartition(previous, row)) {
                output = function.finishPartition(allocator, allocationContext, output, partitionStart, outputPosition);
                function.reset();
                partitionStart = outputPosition;
            }
            output = function.append(allocator, allocationContext, output, row.page().columns(), row.position(), outputPosition, rows.size());
            previous = row;
        }
        if (!rows.isEmpty()) {
            output = function.finishPartition(allocator, allocationContext, output, partitionStart, rows.size());
        }
        return output;
    }

    private int compareRows(RowReference left, RowReference right)
    {
        for (int partitionColumn : partitionColumns) {
            int comparison = compareColumn(partitionColumn, left, right);
            if (comparison != 0) {
                return comparison;
            }
        }
        for (int orderingIndex = 0; orderingIndex < orderingColumns.length; orderingIndex++) {
            int comparison = compareColumn(orderingColumns[orderingIndex], left, right);
            if (descendingByColumn[orderingIndex]) {
                comparison = -comparison;
            }
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    private boolean samePartition(RowReference left, RowReference right)
    {
        for (int partitionColumn : partitionColumns) {
            Streams leftStreams = left.page().columns()[partitionColumn];
            Streams rightStreams = right.page().columns()[partitionColumn];
            if (OperatorVectorSupport.isNull(leftStreams.getOrNull(Stream.NULLS), left.position()) ||
                    OperatorVectorSupport.isNull(rightStreams.getOrNull(Stream.NULLS), right.position())) {
                return false;
            }
            if (!OperatorEqualitySemantics.equal(
                    leftStreams.values(),
                    (BooleanVector) leftStreams.getOrNull(Stream.NULLS),
                    left.position(),
                    rightStreams.values(),
                    (BooleanVector) rightStreams.getOrNull(Stream.NULLS),
                    right.position())) {
                return false;
            }
        }
        return true;
    }

    private int compareColumn(int column, RowReference left, RowReference right)
    {
        Streams leftStreams = left.page().columns()[column];
        Streams rightStreams = right.page().columns()[column];
        return OperatorOrderingSemantics.compare(
                leftStreams.values(),
                (BooleanVector) leftStreams.getOrNull(Stream.NULLS),
                left.position(),
                rightStreams.values(),
                (BooleanVector) rightStreams.getOrNull(Stream.NULLS),
                right.position());
    }

    private boolean equalColumn(int column, RowReference left, RowReference right)
    {
        Streams leftStreams = left.page().columns()[column];
        Streams rightStreams = right.page().columns()[column];
        return OperatorEqualitySemantics.equal(
                leftStreams.values(),
                (BooleanVector) leftStreams.getOrNull(Stream.NULLS),
                left.position(),
                rightStreams.values(),
                (BooleanVector) rightStreams.getOrNull(Stream.NULLS),
                right.position());
    }

    private Streams materializeSourceColumnBatch(int outputIndex, int startPosition, int batchSize)
    {
        Streams.Builder builder = Streams.builder();
        Streams schema = sourceSchema[outputIndex];
        for (Stream stream : schema.streams()) {
            Vector result = null;
            int outputPosition = 0;
            while (outputPosition < batchSize) {
                RowReference firstRow = rows.get(startPosition + outputPosition);
                Streams sourceStreams = pages.get(firstRow.pageIndex()).columns()[outputIndex];
                if (!sourceStreams.has(stream)) {
                    outputPosition++;
                    continue;
                }

                int groupStart = outputPosition;
                int groupPageIndex = firstRow.pageIndex();
                while (outputPosition < batchSize && rows.get(startPosition + outputPosition).pageIndex() == groupPageIndex) {
                    outputPosition++;
                }

                int groupSize = outputPosition - groupStart;
                int[] positions = new int[groupSize];
                for (int index = 0; index < groupSize; index++) {
                    positions[index] = rows.get(startPosition + groupStart + index).position();
                }
                result = sourceStreams.get(stream).copyPositionsInto(
                        allocator,
                        allocationContext,
                        result,
                        positions,
                        groupSize,
                        groupStart,
                        batchSize);
            }
            builder.put(stream, result == null ? schema.get(stream).emptyLike(allocator, allocationContext) : result);
        }
        return builder.build();
    }

    private Streams materializeWindowBatch(int functionIndex, int startPosition, int batchSize)
    {
        Streams fullOutput = windowOutputs[functionIndex];
        Streams.Builder builder = Streams.builder();
        for (Stream stream : fullOutput.streams()) {
            Vector result = null;
            Vector source = fullOutput.get(stream);
            for (int outputPosition = 0; outputPosition < batchSize; outputPosition++) {
                result = source.copySinglePositionInto(
                        allocator,
                        allocationContext,
                        result,
                        startPosition + outputPosition,
                        outputPosition,
                        batchSize);
            }
            builder.put(stream, result == null ? source.emptyLike(allocator, allocationContext) : result);
        }
        return builder.build();
    }

    private List<RowReference> rows(List<TableOperator.Page> pages)
    {
        List<RowReference> rows = new ArrayList<>();
        for (int pageIndex = 0; pageIndex < pages.size(); pageIndex++) {
            TableOperator.Page page = pages.get(pageIndex);
            if (page.mask().all()) {
                for (int position = 0; position < page.rows(); position++) {
                    rows.add(new RowReference(pageIndex, page, position));
                }
                continue;
            }
            for (int index = 0; index < page.mask().selectedCount(); index++) {
                rows.add(new RowReference(pageIndex, page, page.mask().position(index)));
            }
        }
        return rows;
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

    public interface RunningWindowFunction
    {
        Streams emptyOutput(Allocator allocator, Allocator.Context allocationContext, int size);

        void reset();

        Streams append(Allocator allocator, Allocator.Context allocationContext, Streams output, Streams[] sourceColumns, int inputPosition, int outputPosition, int outputSize);

        default Streams finishPartition(Allocator allocator, Allocator.Context allocationContext, Streams output, int partitionStart, int partitionEnd)
        {
            return output;
        }
    }

    public static final class RunningSumI64WindowFunction
            implements RunningWindowFunction
    {
        private final int inputColumn;
        private long runningSum;
        private boolean hasValue;

        public RunningSumI64WindowFunction(int inputColumn)
        {
            this.inputColumn = inputColumn;
        }

        @Override
        public Streams emptyOutput(Allocator allocator, Allocator.Context allocationContext, int size)
        {
            return Streams.ofValuesAndNulls(
                    allocator.allocate(allocationContext, I64Vector.class, size, I64Vector::new),
                    allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new));
        }

        @Override
        public void reset()
        {
            runningSum = 0;
            hasValue = false;
        }

        @Override
        public Streams append(Allocator allocator, Allocator.Context allocationContext, Streams output, Streams[] sourceColumns, int inputPosition, int outputPosition, int outputSize)
        {
            Streams input = sourceColumns[inputColumn];
            Vector values = input.values();
            BooleanVector nulls = (BooleanVector) input.getOrNull(Stream.NULLS);
            if (!OperatorVectorSupport.isNull(nulls, inputPosition)) {
                runningSum += OperatorVectorSupport.longValue(values, inputPosition);
                hasValue = true;
            }

            I64Vector outputValues = (I64Vector) output.values();
            BooleanVector outputNulls = (BooleanVector) output.get(Stream.NULLS);
            outputNulls.values()[outputPosition] = !hasValue;
            if (hasValue) {
                outputValues.values()[outputPosition] = runningSum;
            }
            return output;
        }
    }

    public static final class RunningMaxI64WindowFunction
            implements RunningWindowFunction
    {
        private final int inputColumn;
        private long runningMax;
        private boolean hasValue;

        public RunningMaxI64WindowFunction(int inputColumn)
        {
            this.inputColumn = inputColumn;
        }

        @Override
        public Streams emptyOutput(Allocator allocator, Allocator.Context allocationContext, int size)
        {
            return Streams.ofValuesAndNulls(
                    allocator.allocate(allocationContext, I64Vector.class, size, I64Vector::new),
                    allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new));
        }

        @Override
        public void reset()
        {
            runningMax = 0;
            hasValue = false;
        }

        @Override
        public Streams append(Allocator allocator, Allocator.Context allocationContext, Streams output, Streams[] sourceColumns, int inputPosition, int outputPosition, int outputSize)
        {
            Streams input = sourceColumns[inputColumn];
            Vector values = input.values();
            BooleanVector nulls = (BooleanVector) input.getOrNull(Stream.NULLS);
            if (!OperatorVectorSupport.isNull(nulls, inputPosition)) {
                long value = OperatorVectorSupport.longValue(values, inputPosition);
                if (!hasValue || value > runningMax) {
                    runningMax = value;
                }
                hasValue = true;
            }

            I64Vector outputValues = (I64Vector) output.values();
            BooleanVector outputNulls = (BooleanVector) output.get(Stream.NULLS);
            outputNulls.values()[outputPosition] = !hasValue;
            if (hasValue) {
                outputValues.values()[outputPosition] = runningMax;
            }
            return output;
        }
    }

    public static final class PartitionAverageI64WindowFunction
            implements RunningWindowFunction
    {
        private final int inputColumn;
        private long runningSum;
        private long runningCount;

        public PartitionAverageI64WindowFunction(int inputColumn)
        {
            this.inputColumn = inputColumn;
        }

        @Override
        public Streams emptyOutput(Allocator allocator, Allocator.Context allocationContext, int size)
        {
            return Streams.ofValuesAndNulls(
                    allocator.allocate(allocationContext, I64Vector.class, size, I64Vector::new),
                    allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new));
        }

        @Override
        public void reset()
        {
            runningSum = 0;
            runningCount = 0;
        }

        @Override
        public Streams append(Allocator allocator, Allocator.Context allocationContext, Streams output, Streams[] sourceColumns, int inputPosition, int outputPosition, int outputSize)
        {
            Streams input = sourceColumns[inputColumn];
            Vector values = input.values();
            BooleanVector nulls = (BooleanVector) input.getOrNull(Stream.NULLS);
            if (!OperatorVectorSupport.isNull(nulls, inputPosition)) {
                runningSum += OperatorVectorSupport.longValue(values, inputPosition);
                runningCount++;
            }
            return output;
        }

        @Override
        public Streams finishPartition(Allocator allocator, Allocator.Context allocationContext, Streams output, int partitionStart, int partitionEnd)
        {
            I64Vector outputValues = (I64Vector) output.values();
            BooleanVector outputNulls = (BooleanVector) output.get(Stream.NULLS);
            boolean hasValue = runningCount > 0;
            long average = hasValue ? roundDivide(runningSum, runningCount) : 0;
            for (int outputPosition = partitionStart; outputPosition < partitionEnd; outputPosition++) {
                outputNulls.values()[outputPosition] = !hasValue;
                if (hasValue) {
                    outputValues.values()[outputPosition] = average;
                }
            }
            return output;
        }

        private static long roundDivide(long numerator, long denominator)
        {
            long positiveNumerator = numerator >= 0 ? numerator : -numerator;
            long positiveDenominator = denominator >= 0 ? denominator : -denominator;
            long rounded = (positiveNumerator + (positiveDenominator / 2)) / positiveDenominator;
            return (numerator < 0) ^ (denominator < 0) ? -rounded : rounded;
        }
    }

    public static final class PartitionSumI64WindowFunction
            implements RunningWindowFunction
    {
        private final int inputColumn;
        private long runningSum;
        private boolean hasValue;

        public PartitionSumI64WindowFunction(int inputColumn)
        {
            this.inputColumn = inputColumn;
        }

        @Override
        public Streams emptyOutput(Allocator allocator, Allocator.Context allocationContext, int size)
        {
            return Streams.ofValuesAndNulls(
                    allocator.allocate(allocationContext, I64Vector.class, size, I64Vector::new),
                    allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new));
        }

        @Override
        public void reset()
        {
            runningSum = 0;
            hasValue = false;
        }

        @Override
        public Streams append(Allocator allocator, Allocator.Context allocationContext, Streams output, Streams[] sourceColumns, int inputPosition, int outputPosition, int outputSize)
        {
            Streams input = sourceColumns[inputColumn];
            Vector values = input.values();
            BooleanVector nulls = (BooleanVector) input.getOrNull(Stream.NULLS);
            if (!OperatorVectorSupport.isNull(nulls, inputPosition)) {
                runningSum += OperatorVectorSupport.longValue(values, inputPosition);
                hasValue = true;
            }
            return output;
        }

        @Override
        public Streams finishPartition(Allocator allocator, Allocator.Context allocationContext, Streams output, int partitionStart, int partitionEnd)
        {
            I64Vector outputValues = (I64Vector) output.values();
            BooleanVector outputNulls = (BooleanVector) output.get(Stream.NULLS);
            for (int outputPosition = partitionStart; outputPosition < partitionEnd; outputPosition++) {
                outputNulls.values()[outputPosition] = !hasValue;
                if (hasValue) {
                    outputValues.values()[outputPosition] = runningSum;
                }
            }
            return output;
        }
    }

    public static final class PartitionOffsetI64WindowFunction
            implements RunningWindowFunction
    {
        private final int inputColumn;
        private final int offset;

        private long[] partitionValues = new long[0];
        private boolean[] partitionNulls = new boolean[0];
        private int partitionCount;

        public PartitionOffsetI64WindowFunction(int inputColumn, int offset)
        {
            this.inputColumn = inputColumn;
            this.offset = offset;
        }

        @Override
        public Streams emptyOutput(Allocator allocator, Allocator.Context allocationContext, int size)
        {
            return Streams.ofValuesAndNulls(
                    allocator.allocate(allocationContext, I64Vector.class, size, I64Vector::new),
                    allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new));
        }

        @Override
        public void reset()
        {
            partitionCount = 0;
        }

        @Override
        public Streams append(Allocator allocator, Allocator.Context allocationContext, Streams output, Streams[] sourceColumns, int inputPosition, int outputPosition, int outputSize)
        {
            ensureCapacity(partitionCount + 1);
            Streams input = sourceColumns[inputColumn];
            Vector values = input.values();
            BooleanVector nulls = (BooleanVector) input.getOrNull(Stream.NULLS);
            boolean isNull = OperatorVectorSupport.isNull(nulls, inputPosition);
            partitionNulls[partitionCount] = isNull;
            if (!isNull) {
                partitionValues[partitionCount] = OperatorVectorSupport.longValue(values, inputPosition);
            }
            partitionCount++;
            return output;
        }

        @Override
        public Streams finishPartition(Allocator allocator, Allocator.Context allocationContext, Streams output, int partitionStart, int partitionEnd)
        {
            I64Vector outputValues = (I64Vector) output.values();
            BooleanVector outputNulls = (BooleanVector) output.get(Stream.NULLS);
            for (int partitionPosition = 0; partitionPosition < partitionCount; partitionPosition++) {
                int sourcePosition = partitionPosition + offset;
                int outputPosition = partitionStart + partitionPosition;
                boolean isNull = sourcePosition < 0 || sourcePosition >= partitionCount || partitionNulls[sourcePosition];
                outputNulls.values()[outputPosition] = isNull;
                if (!isNull) {
                    outputValues.values()[outputPosition] = partitionValues[sourcePosition];
                }
            }
            return output;
        }

        private void ensureCapacity(int requiredSize)
        {
            if (partitionValues.length >= requiredSize) {
                return;
            }
            int newSize = Math.max(requiredSize, Math.max(8, partitionValues.length * 2));
            partitionValues = Arrays.copyOf(partitionValues, newSize);
            partitionNulls = Arrays.copyOf(partitionNulls, newSize);
        }
    }

    public static final class RankWindowFunction
            implements RunningWindowFunction
    {
        private final int[] orderingColumns;
        private final boolean[] descendingByColumn;

        private Streams[] previousColumns;
        private int previousPosition;
        private long rowNumber;
        private long rank;

        public RankWindowFunction(int[] orderingColumns, boolean[] descendingByColumn)
        {
            if (orderingColumns.length != descendingByColumn.length) {
                throw new IllegalArgumentException("Ordering columns and directions must have the same length");
            }
            this.orderingColumns = orderingColumns.clone();
            this.descendingByColumn = descendingByColumn.clone();
        }

        @Override
        public Streams emptyOutput(Allocator allocator, Allocator.Context allocationContext, int size)
        {
            return Streams.ofValues(
                    allocator.allocate(allocationContext, I64Vector.class, size, I64Vector::new));
        }

        @Override
        public void reset()
        {
            previousColumns = null;
            previousPosition = -1;
            rowNumber = 0;
            rank = 0;
        }

        @Override
        public Streams append(Allocator allocator, Allocator.Context allocationContext, Streams output, Streams[] sourceColumns, int inputPosition, int outputPosition, int outputSize)
        {
            rowNumber++;
            if (previousColumns == null || orderingChanged(previousColumns, previousPosition, sourceColumns, inputPosition)) {
                rank = rowNumber;
            }
            ((I64Vector) output.values()).values()[outputPosition] = rank;
            previousColumns = sourceColumns;
            previousPosition = inputPosition;
            return output;
        }

        private boolean orderingChanged(Streams[] leftColumns, int leftPosition, Streams[] rightColumns, int rightPosition)
        {
            for (int orderingIndex = 0; orderingIndex < orderingColumns.length; orderingIndex++) {
                Streams left = leftColumns[orderingColumns[orderingIndex]];
                Streams right = rightColumns[orderingColumns[orderingIndex]];
                int comparison = OperatorOrderingSemantics.compare(
                        left.values(),
                        (BooleanVector) left.getOrNull(Stream.NULLS),
                        leftPosition,
                        right.values(),
                        (BooleanVector) right.getOrNull(Stream.NULLS),
                        rightPosition);
                if (descendingByColumn[orderingIndex]) {
                    comparison = -comparison;
                }
                if (comparison != 0) {
                    return true;
                }
            }
            return false;
        }
    }

    private record RowReference(int pageIndex, TableOperator.Page page, int position) {}
}
