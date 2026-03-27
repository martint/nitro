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

import java.util.Arrays;
import java.util.Set;
import java.util.function.Function;

public class SemiJoinOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("SemiJoinOperator");

    private final Operator outer;
    private final Operator inner;
    private final int outerJoinColumn;
    private final int innerJoinColumn;
    private final Allocator allocator;
    private final boolean includeMatches;
    private final boolean outputMatches;
    private final GroupingState membership = new GroupingState();

    private boolean loaded;
    private BatchState currentBatchState;
    private I64Vector membershipScratch;

    public SemiJoinOperator(Allocator allocator, Operator outer, int outerJoinColumn, Operator inner, int innerJoinColumn)
    {
        this(allocator, outer, outerJoinColumn, inner, innerJoinColumn, true);
    }

    public SemiJoinOperator(Allocator allocator, Operator outer, int outerJoinColumn, Operator inner, int innerJoinColumn, boolean includeMatches)
    {
        this(allocator, outer, outerJoinColumn, inner, innerJoinColumn, includeMatches, false);
    }

    public SemiJoinOperator(Allocator allocator, Operator outer, int outerJoinColumn, Operator inner, int innerJoinColumn, boolean includeMatches, boolean outputMatches)
    {
        this.outer = outer;
        this.inner = inner;
        this.outerJoinColumn = outerJoinColumn;
        this.innerJoinColumn = innerJoinColumn;
        this.allocator = allocator;
        this.includeMatches = includeMatches;
        this.outputMatches = outputMatches;
    }

    @Override
    public int outputCount()
    {
        return outer.outputCount() + (outputMatches ? 1 : 0);
    }

    @Override
    public boolean hasNext()
    {
        loadInnerIfNecessary();
        return outer.hasNext();
    }

    @Override
    public Batch next()
    {
        loadInnerIfNecessary();

        Batch sourceBatch = outer.next();
        BatchState batchState = new BatchState(sourceBatch, sourceBatch.borrowMask());
        currentBatchState = batchState;

        Mask batchMask = sourceBatch.borrowMask();
        if (!outputMatches) {
            batchMask = selectRows(sourceBatch);
            outer.constrain(batchMask);
            sourceBatch.constrain(batchMask);
            batchState.constrain(batchMask);
        }

        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outer.outputCount(); outputIndex++) {
            Output sourceOutput = sourceBatch.output(outputIndex);
            outputs[outputIndex] = new Output(
                    sourceOutput.streams(),
                    sourceOutput::borrow,
                    (stream, vector) -> sourceOutput.take(stream),
                    (_, _) -> {},
                    sourceOutput::copySinglePosition);
        }
        if (outputMatches) {
            outputs[outer.outputCount()] = new Output(
                    Set.of(Stream.VALUES),
                    stream -> {
                        if (stream != Stream.VALUES) {
                            throw new IllegalArgumentException("Unsupported stream: " + stream);
                        }
                        return batchState.borrowMatchValues(this);
                    },
                    (stream, vector) -> vector,
                    (stream, vector) -> allocator.release(ALLOCATION_CONTEXT, vector),
                    (existing, sourcePosition, outputPosition, size) -> {
                        BooleanVector matchValues = batchState.borrowMatchValues(this);
                        BooleanVector outputValues = allocator.allocateOrGrow(ALLOCATION_CONTEXT, existing == null ? null : (BooleanVector) existing.getOrNull(Stream.VALUES), BooleanVector.class, size, BooleanVector::new);
                        outputValues.values()[outputPosition] = matchValues.values()[sourcePosition];
                        return Streams.ofValues(outputValues);
                    });
        }
        return new Batch(
                batchMask,
                batchState::constrain,
                Function.identity(),
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
        outer.constrain(mask);
        if (currentBatchState != null) {
            currentBatchState.constrain(mask);
        }
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        return outer.supportsRetainedBatches();
    }

    @Override
    public void close()
    {
        if (currentBatchState != null) {
            currentBatchState.sourceBatch().close();
            currentBatchState = null;
        }
        outer.close();
        if (!loaded) {
            inner.close();
        }
        allocator.release(ALLOCATION_CONTEXT);
    }

    private void loadInnerIfNecessary()
    {
        if (loaded) {
            return;
        }
        loaded = true;
        while (inner.hasNext()) {
            Batch batch = inner.next();
            try {
                Mask mask = batch.borrowMask();
                if (mask.none()) {
                    continue;
                }
                Output output = batch.output(innerJoinColumn);
                Vector values = output.borrow(Stream.VALUES);
                BooleanVector nulls = (BooleanVector) output.borrowOrNull(Stream.NULLS);
                membershipScratch = allocator.allocateOrGrow(
                        ALLOCATION_CONTEXT,
                        membershipScratch,
                        I64Vector.class,
                        values.length(),
                        I64Vector::new);
                membership.assignGroups(values, nulls, mask, membershipScratch);
            }
            finally {
                batch.close();
            }
        }
        inner.close();
    }

    private Mask selectRows(Batch sourceBatch)
    {
        Mask sourceMask = sourceBatch.borrowMask();
        if (sourceMask.none()) {
            return allocator.allocateSparseMask(ALLOCATION_CONTEXT, new int[0], sourceMask.size());
        }

        Output output = sourceBatch.output(outerJoinColumn);
        Vector values = output.borrow(Stream.VALUES);
        BooleanVector nulls = (BooleanVector) output.borrowOrNull(Stream.NULLS);

        int[] positions = new int[sourceMask.count()];
        int selectedCount = 0;
        for (int position : sourceMask) {
            if (membership.contains(values, nulls, position) == includeMatches) {
                positions[selectedCount++] = position;
            }
        }

        if (selectedCount == sourceMask.count()) {
            return allocator.copyMask(ALLOCATION_CONTEXT, sourceMask);
        }
        return allocator.allocateSparseMask(ALLOCATION_CONTEXT, positions, selectedCount, sourceMask.size());
    }

    private BooleanVector computeMatchValues(BatchState batchState)
    {
        Mask mask = batchState.mask();
        BooleanVector matchValues = allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, mask.size(), BooleanVector::new);
        Arrays.fill(matchValues.values(), false);

        if (mask.none()) {
            return matchValues;
        }

        Output output = batchState.sourceBatch().output(outerJoinColumn);
        Vector values = output.borrow(Stream.VALUES);
        BooleanVector nulls = (BooleanVector) output.borrowOrNull(Stream.NULLS);
        for (int position : mask) {
            matchValues.values()[position] = membership.contains(values, nulls, position);
        }
        return matchValues;
    }

    private static final class BatchState
    {
        private final Batch sourceBatch;
        private final Mask[] maskHolder;
        private BooleanVector matchValues;

        private BatchState(Batch sourceBatch, Mask mask)
        {
            this.sourceBatch = sourceBatch;
            this.maskHolder = new Mask[] {mask};
        }

        private void constrain(Mask mask)
        {
            maskHolder[0] = mask;
            sourceBatch.constrain(mask);
        }

        private Batch sourceBatch()
        {
            return sourceBatch;
        }

        private Mask mask()
        {
            return maskHolder[0];
        }

        private BooleanVector borrowMatchValues(SemiJoinOperator operator)
        {
            if (matchValues == null) {
                matchValues = operator.computeMatchValues(this);
            }
            return matchValues;
        }
    }
}
