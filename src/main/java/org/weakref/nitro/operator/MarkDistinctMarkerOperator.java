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
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Vector;

import java.util.Arrays;
import java.util.Set;

/**
 * Appends a non-null Boolean column that marks the first occurrence of each key while preserving
 * every source row. The marker exposes a direct mask resolver, allowing filtered aggregation to
 * consume the selected positions without materializing the Boolean vector. This is the physical
 * mark-distinct boundary used by optimized SQL plans that mix ordinary and DISTINCT aggregates.
 */
public final class MarkDistinctMarkerOperator
        implements Operator
{
    private static final int[] EMPTY_POSITIONS = new int[0];

    private final Allocator allocator;
    private final Allocator.Context allocationContext;
    private final PrimitiveArrayPool arrayPool;
    private final int[] distinctColumns;
    private final boolean retainNulls;
    private final Operator source;
    private final OperatorCodeGenerationResources codeGeneration;
    private final Vector[] values;
    private final Vector[] nulls;

    private DistinctKeySet distinctKeySet;
    private int[] distinctPositions = EMPTY_POSITIONS;
    private BooleanVector reusableMarker;
    private BatchState currentBatchState;

    public MarkDistinctMarkerOperator(Allocator allocator, int[] distinctColumns, Operator source, boolean retainNulls, OperatorResources operatorResources)
    {
        if (distinctColumns.length == 0) {
            throw new IllegalArgumentException("distinctColumns is empty");
        }
        this.allocator = allocator;
        this.allocationContext = new Allocator.Context(
                "MarkDistinctMarkerOperator",
                operatorResources.grouping().markDistinctMarkerBufferPool());
        this.arrayPool = allocator.primitiveArrays();
        this.codeGeneration = operatorResources.codeGeneration();
        this.distinctColumns = distinctColumns.clone();
        this.retainNulls = retainNulls;
        this.source = source;
        this.values = new Vector[distinctColumns.length];
        this.nulls = new Vector[distinctColumns.length];
    }

    @Override
    public int outputCount()
    {
        return source.outputCount() + 1;
    }

    @Override
    public boolean hasNext()
    {
        return source.hasNext();
    }

    @Override
    public Batch next()
    {
        Batch sourceBatch = source.next();
        BatchState batchState = new BatchState(sourceBatch, sourceBatch.borrowMask());
        currentBatchState = batchState;

        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < source.outputCount(); outputIndex++) {
            Output sourceOutput = sourceBatch.output(outputIndex);
            outputs[outputIndex] = new Output(
                    sourceOutput.streams(),
                    sourceOutput::borrow,
                    (stream, vector) -> sourceOutput.take(stream),
                    (_, _) -> {},
                    (_, existing, sourcePositions, sourceStart, sourceCount, outputStart, size, assumeClearOutputRange) ->
                            sourceOutput.copyPositions(existing, sourcePositions, sourceStart, sourceCount, outputStart, size, assumeClearOutputRange),
                    sourceOutput::copySinglePosition);
        }
        outputs[outputs.length - 1] = new Output(
                Set.of(Stream.VALUES),
                _ -> markerVector(batchState),
                null,
                (_, requestedMask, selectTrue, resultAllocator, resultContext) -> markerMask(batchState, requestedMask, selectTrue, resultAllocator, resultContext),
                (_, vector) -> {
                    if (vector == reusableMarker) {
                        reusableMarker = null;
                    }
                    batchState.marker = null;
                    return allocator.transfer(allocationContext, vector);
                },
                (_, _) -> {},
                null,
                null);

        return new Batch(
                batchState.mask,
                batchState::constrain,
                ignored -> sourceBatch.takeMask(),
                _ -> {},
                () -> {
                    if (currentBatchState == batchState) {
                        currentBatchState = null;
                    }
                    sourceBatch.close();
                },
                outputs);
    }

    @Override
    public void constrain(Mask mask)
    {
        source.constrain(mask);
        if (currentBatchState != null) {
            currentBatchState.constrain(mask);
        }
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        return source.supportsRetainedBatches();
    }

    @Override
    public void close()
    {
        if (currentBatchState != null) {
            currentBatchState.sourceBatch.close();
            currentBatchState = null;
        }
        source.close();
        if (distinctKeySet != null) {
            distinctKeySet.releaseBuffers();
            distinctKeySet = null;
        }
        arrayPool.release(distinctPositions);
        distinctPositions = EMPTY_POSITIONS;
        Arrays.fill(values, null);
        Arrays.fill(nulls, null);
        reusableMarker = null;
        allocator.release(allocationContext);
    }

    private Mask markerMask(BatchState batchState, Mask requestedMask, boolean selectTrue, Allocator resultAllocator, Allocator.Context resultContext)
    {
        computeDistinctPositions(batchState);
        if (selectTrue && requestedMask == batchState.originalMask) {
            return resultAllocator.allocateSparseMask(resultContext, distinctPositions, batchState.distinctCount, requestedMask.size());
        }
        Mask result = resultAllocator.copyMask(resultContext, requestedMask);
        result.retainBooleans(markerVector(batchState).values(), selectTrue);
        return result;
    }

    private BooleanVector markerVector(BatchState batchState)
    {
        computeDistinctPositions(batchState);
        if (batchState.marker == null) {
            int size = batchState.originalMask.size();
            reusableMarker = allocator.reallocateIfNecessary(allocationContext, reusableMarker, BooleanVector.class, size, BooleanVector::new);
            boolean[] marker = reusableMarker.values();
            Arrays.fill(marker, 0, size, false);
            for (int index = 0; index < batchState.distinctCount; index++) {
                marker[distinctPositions[index]] = true;
            }
            batchState.marker = reusableMarker;
        }
        return batchState.marker;
    }

    private void computeDistinctPositions(BatchState batchState)
    {
        if (batchState.distinctComputed) {
            return;
        }
        batchState.distinctComputed = true;
        Mask mask = batchState.originalMask;
        if (mask.none()) {
            batchState.distinctCount = 0;
            return;
        }
        if (distinctPositions.length < mask.selectedCount()) {
            int[] previous = distinctPositions;
            distinctPositions = arrayPool.borrowInts(mask.selectedCount());
            arrayPool.release(previous);
        }
        try {
            for (int index = 0; index < distinctColumns.length; index++) {
                Output output = batchState.sourceBatch.output(distinctColumns[index]);
                values[index] = output.borrow(Stream.VALUES);
                nulls[index] = output.borrowOrNull(Stream.NULLS);
            }
            if (distinctKeySet == null) {
                distinctKeySet = DistinctKeySet.create(
                        values,
                        retainNulls,
                        arrayPool,
                        codeGeneration);
            }
            distinctKeySet.reserveAdditional(mask.selectedCount());
            batchState.distinctCount = distinctKeySet.addBatch(values, nulls, mask, distinctPositions);
        }
        finally {
            Arrays.fill(values, null);
            Arrays.fill(nulls, null);
        }
    }

    private final class BatchState
    {
        private final Batch sourceBatch;
        private final Mask originalMask;
        private Mask mask;
        private boolean distinctComputed;
        private int distinctCount;
        private BooleanVector marker;

        private BatchState(Batch sourceBatch, Mask mask)
        {
            this.sourceBatch = sourceBatch;
            this.originalMask = mask;
            this.mask = mask;
        }

        private void constrain(Mask mask)
        {
            this.mask = mask;
            sourceBatch.constrain(mask);
        }
    }
}
