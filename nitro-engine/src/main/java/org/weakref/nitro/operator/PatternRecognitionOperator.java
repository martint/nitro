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

import org.weakref.nitro.core.execution.ExecutionContext;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.pattern.PatternCompiler;
import org.weakref.nitro.operator.pattern.PatternDefinition;
import org.weakref.nitro.operator.pattern.PatternDefinitionEvaluator;
import org.weakref.nitro.operator.pattern.PatternExpression;
import org.weakref.nitro.operator.pattern.PatternMatcher;
import org.weakref.nitro.operator.pattern.PatternOutputMode;
import org.weakref.nitro.operator.pattern.PatternPartitionCursor;
import org.weakref.nitro.operator.pattern.PatternPartitionOutput;
import org.weakref.nitro.operator.pattern.PatternSearch;
import org.weakref.nitro.operator.pattern.PatternSkipPolicy;
import org.weakref.nitro.operator.pattern.PatternValueProgram;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/// Blocking pull operator for an already ordered row-pattern input.
///
/// Partition/order preparation is an explicit upstream physical responsibility. This keeps adjacent execution on
/// Nitro's operator interface while avoiding an implicit host PagesIndex boundary. Matcher, definition, and measure
/// state remain restartable when the injected execution context suspends the driver thread.
public final class PatternRecognitionOperator
        implements Operator
{
    private final Allocator.Context allocationContext = new Allocator.Context("PatternRecognitionOperator", PatternRecognitionOperator.class);
    private final Allocator.Context rowBufferContext = new Allocator.Context("PatternRecognitionOperatorRows", PatternRecognitionOperator.class);

    private final Allocator allocator;
    private final ExecutionContext executionContext;
    private final Operator source;
    private final int[] partitionColumns;
    private final int[] outputChannels;
    private final PatternExpression pattern;
    private final List<PatternDefinition> definitions;
    private final PatternSkipPolicy skip;
    private final PatternOutputMode outputMode;
    private final PatternValueProgram measures;
    private final boolean initial;
    private final int outputBatchRows;
    private final Schema outputSchema;
    private final StructuralComparisonKernel[] partitionEquality;
    private final EncodedRowBuffer rows;

    private PatternSearch search;
    private PatternMatcher matcher;
    private PatternPartitionOutput partitionOutput;
    private int nextPartitionStart;
    private int partitionEnd;
    private Streams[] buildingColumns;
    private int buildingRows;
    private Batch staged;
    private boolean loaded;
    private boolean exhausted;
    private boolean closed;

    public PatternRecognitionOperator(
            Allocator allocator,
            ExecutionContext executionContext,
            Operator source,
            int[] partitionColumns,
            WindowInputOrder inputOrder,
            int orderingColumnCount,
            int[] outputChannels,
            PatternExpression pattern,
            List<PatternDefinition> definitions,
            PatternSkipPolicy skip,
            PatternOutputMode outputMode,
            PatternValueProgram measures,
            boolean initial,
            int outputBatchRows,
            Schema outputSchema,
            OperatorResources resources)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.executionContext = requireNonNull(executionContext, "executionContext is null");
        this.source = requireNonNull(source, "source is null");
        this.partitionColumns = requireNonNull(partitionColumns, "partitionColumns is null").clone();
        if (!requireNonNull(inputOrder, "inputOrder is null").isFullyOrdered(orderingColumnCount)) {
            throw new IllegalArgumentException("pattern input is not fully partitioned and ordered");
        }
        this.outputChannels = requireNonNull(outputChannels, "outputChannels is null").clone();
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.definitions = List.copyOf(requireNonNull(definitions, "definitions is null"));
        this.skip = requireNonNull(skip, "skip is null");
        this.outputMode = requireNonNull(outputMode, "outputMode is null");
        this.measures = requireNonNull(measures, "measures is null");
        this.initial = initial;
        if (outputBatchRows <= 0) {
            throw new IllegalArgumentException("outputBatchRows is not positive");
        }
        this.outputBatchRows = outputBatchRows;
        this.outputSchema = requireNonNull(outputSchema, "outputSchema is null");
        if (outputSchema.size() != outputChannels.length + measures.size()) {
            throw new IllegalArgumentException("output schema does not match pass-through and measure columns");
        }
        if (outputMode == PatternOutputMode.WINDOW) {
            throw new IllegalArgumentException("WINDOW output requires frame-aware output support");
        }
        for (int column : this.partitionColumns) {
            if (column < 0 || column >= source.outputCount()) {
                throw new IllegalArgumentException("partition column is outside source schema");
            }
        }
        for (int column : this.outputChannels) {
            if (column < 0 || column >= source.outputCount()) {
                throw new IllegalArgumentException("output channel is outside source schema");
            }
        }

        StructuralTypeKernelFactory structuralTypes = requireNonNull(resources, "resources is null").codeGeneration().structuralTypes();
        partitionEquality = new StructuralComparisonKernel[source.outputCount()];
        for (int column : this.partitionColumns) {
            partitionEquality[column] = structuralTypes.comparison(source.outputSchema().field(column).type());
        }
        rows = new EncodedRowBuffer(allocator, rowBufferContext, source.outputCount());
    }

    @Override
    public int outputCount()
    {
        return outputSchema.size();
    }

    @Override
    public Schema outputSchema()
    {
        return outputSchema;
    }

    @Override
    public boolean hasNext()
    {
        checkOpen();
        prepareBatch();
        return staged != null;
    }

    @Override
    public Batch next()
    {
        checkOpen();
        prepareBatch();
        if (staged == null) {
            throw new IllegalStateException("pattern recognition output is exhausted");
        }
        Batch result = staged;
        staged = null;
        return result;
    }

    private void prepareBatch()
    {
        if (staged != null || exhausted) {
            return;
        }
        load();
        if (buildingColumns == null) {
            buildingColumns = new Streams[outputCount()];
            initializeNullableMeasureColumns();
        }

        while (buildingRows < outputBatchRows) {
            if (partitionOutput == null && !startPartition()) {
                exhausted = true;
                break;
            }
            if (!partitionOutput.advance(executionContext)) {
                partitionOutput.close();
                partitionOutput = null;
                nextPartitionStart = partitionEnd;
                continue;
            }

            appendSourceColumns(partitionOutput.sourcePosition());
            if (partitionOutput.measureContext() != null) {
                measures.append(
                        partitionOutput.measureContext(),
                        allocator,
                        allocationContext,
                        buildingColumns,
                        outputChannels.length,
                        buildingRows,
                        outputBatchRows);
            }
            buildingRows++;
        }

        if (buildingRows == 0) {
            buildingColumns = null;
            return;
        }
        staged = outputBatch(buildingColumns, buildingRows);
        buildingColumns = null;
        buildingRows = 0;
    }

    private void load()
    {
        if (loaded) {
            return;
        }
        rows.load(source);
        PatternDefinitionEvaluator evaluator = new PatternDefinitionEvaluator(rows, definitions);
        matcher = new PatternMatcher(PatternCompiler.compile(pattern), allocator.primitiveArrays());
        search = new PatternSearch(matcher, evaluator);
        loaded = true;
    }

    private boolean startPartition()
    {
        if (nextPartitionStart >= rows.size()) {
            return false;
        }
        partitionEnd = nextPartitionStart + 1;
        while (partitionEnd < rows.size() && samePartition(partitionEnd - 1, partitionEnd)) {
            partitionEnd++;
        }
        partitionOutput = new PatternPartitionOutput(
                new PatternPartitionCursor(search, skip, nextPartitionStart, partitionEnd, initial),
                outputMode);
        return true;
    }

    private boolean samePartition(int leftPosition, int rightPosition)
    {
        for (int column : partitionColumns) {
            Streams left = rows.column(column, leftPosition);
            Streams right = rows.column(column, rightPosition);
            if (!partitionEquality[column].identical(
                    left.values(), left.getOrNull(Stream.NULLS), rows.sourcePosition(leftPosition),
                    right.values(), right.getOrNull(Stream.NULLS), rows.sourcePosition(rightPosition))) {
                return false;
            }
        }
        return true;
    }

    private void appendSourceColumns(int rowPosition)
    {
        for (int output = 0; output < outputChannels.length; output++) {
            Streams input = rows.column(outputChannels[output], rowPosition);
            buildingColumns[output] = allocator.copySinglePositionInto(
                    allocationContext,
                    input,
                    buildingColumns[output] == null ? Streams.empty() : buildingColumns[output],
                    rows.sourcePosition(rowPosition),
                    buildingRows,
                    outputBatchRows);
        }
    }

    private void initializeNullableMeasureColumns()
    {
        if (!outputMode.outputsUnmatchedRows()) {
            return;
        }
        for (int measure = 0; measure < measures.size(); measure++) {
            int outputColumn = outputChannels.length + measure;
            var vectorFactory = outputSchema.field(outputColumn).type().vectorFactory()
                    .orElseThrow(() -> new IllegalArgumentException("pattern measure type does not provide a vector factory"));
            buildingColumns[outputColumn] = WindowValueCopySupport.mutableNullOutput(
                    vectorFactory,
                    allocator,
                    allocationContext,
                    outputBatchRows);
        }
    }

    private Batch outputBatch(Streams[] columns, int size)
    {
        Output[] outputs = new Output[columns.length];
        for (int index = 0; index < columns.length; index++) {
            Streams streams = requireNonNull(columns[index], "pattern output column is missing");
            outputs[index] = new Output(
                    streams.streams(),
                    streams::get,
                    (stream, vector) -> allocator.transfer(allocationContext, vector),
                    (stream, vector) -> allocator.release(allocationContext, vector));
        }
        Mask mask = allocator.allocateRangeMask(allocationContext, 0, size);
        return new Batch(
                mask,
                _ -> {},
                takenMask -> allocator.transfer(allocationContext, takenMask),
                batchMask -> allocator.release(allocationContext, batchMask),
                () -> {},
                outputs);
    }

    @Override
    public void constrain(Mask mask) {}

    @Override
    public boolean supportsRetainedBatches()
    {
        return true;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        RuntimeException failure = null;
        if (staged != null) {
            try {
                staged.close();
            }
            catch (RuntimeException closeFailure) {
                failure = appendFailure(failure, closeFailure);
            }
            staged = null;
        }
        if (partitionOutput != null) {
            try {
                partitionOutput.close();
            }
            catch (RuntimeException closeFailure) {
                failure = appendFailure(failure, closeFailure);
            }
            partitionOutput = null;
        }
        try {
            releaseBuildingColumns();
        }
        catch (RuntimeException closeFailure) {
            failure = appendFailure(failure, closeFailure);
        }
        try {
            measures.close();
        }
        catch (RuntimeException closeFailure) {
            failure = appendFailure(failure, closeFailure);
        }
        for (PatternDefinition definition : definitions) {
            try {
                definition.close();
            }
            catch (RuntimeException closeFailure) {
                failure = appendFailure(failure, closeFailure);
            }
        }
        if (matcher != null) {
            try {
                matcher.close();
            }
            catch (RuntimeException closeFailure) {
                failure = appendFailure(failure, closeFailure);
            }
            matcher = null;
        }
        try {
            rows.close();
        }
        catch (RuntimeException closeFailure) {
            failure = appendFailure(failure, closeFailure);
        }
        try {
            source.close();
        }
        catch (RuntimeException closeFailure) {
            failure = appendFailure(failure, closeFailure);
        }
        try {
            allocator.release(allocationContext);
        }
        catch (RuntimeException closeFailure) {
            failure = appendFailure(failure, closeFailure);
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static RuntimeException appendFailure(RuntimeException failure, RuntimeException closeFailure)
    {
        if (failure == null) {
            return closeFailure;
        }
        failure.addSuppressed(closeFailure);
        return failure;
    }

    private void releaseBuildingColumns()
    {
        if (buildingColumns == null) {
            return;
        }
        Set<Vector> released = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Streams column : buildingColumns) {
            if (column == null) {
                continue;
            }
            for (Vector vector : column.asMap().values()) {
                if (released.add(vector)) {
                    allocator.release(allocationContext, vector);
                }
            }
        }
        buildingColumns = null;
        buildingRows = 0;
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("pattern recognition operator is closed");
        }
    }
}
