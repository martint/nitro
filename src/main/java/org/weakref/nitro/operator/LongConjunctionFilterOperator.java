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
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Arrays;
import java.util.function.Function;
import java.util.function.LongPredicate;

import static com.google.common.base.Preconditions.checkArgument;

public class LongConjunctionFilterOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("LongConjunctionFilterOperator");

    private final Operator source;
    private final Allocator allocator;
    private final Condition[] conditions;

    private BatchState currentBatchState;

    public LongConjunctionFilterOperator(Operator source, Allocator allocator, Condition... conditions)
    {
        this.source = source;
        this.allocator = allocator;
        this.conditions = conditions.clone();
        checkArgument(this.conditions.length > 0, "conditions is empty");
    }

    @Override
    public int outputCount()
    {
        return source.outputCount();
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

        Vector[] values = new Vector[conditions.length];
        boolean[][] nulls = new boolean[conditions.length][];
        for (int index = 0; index < conditions.length; index++) {
            Output output = sourceBatch.output(conditions[index].inputIndex());
            values[index] = output.borrow(Stream.VALUES);
            Vector nullVector = output.borrowOrNull(Stream.NULLS);
            nulls[index] = nullVector == null ? null : ((BooleanVector) nullVector).values();
        }

        Mask sourceMask = sourceBatch.borrowMask();
        int[] matchingPositions = new int[sourceMask.selectedCount()];
        int matched = 0;
        for (int position : sourceMask) {
            if (matches(values, nulls, position)) {
                matchingPositions[matched++] = position;
            }
        }

        int[] selectedPositions = Arrays.copyOf(matchingPositions, matched);
        Mask batchMask = allocator.allocateSparseMask(ALLOCATION_CONTEXT, selectedPositions, sourceMask.size());
        source.constrain(batchMask);
        sourceBatch.constrain(batchMask);
        batchState.constrain(batchMask);

        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            Output sourceOutput = sourceBatch.output(outputIndex);
            outputs[outputIndex] = new Output(sourceOutput.streams(), sourceOutput::borrow, (stream, vector) -> sourceOutput.take(stream));
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
            currentBatchState.sourceBatch().close();
            currentBatchState = null;
        }
        source.close();
        allocator.release(ALLOCATION_CONTEXT);
    }

    private boolean matches(Vector[] values, boolean[][] nulls, int position)
    {
        for (int index = 0; index < conditions.length; index++) {
            if (isNull(nulls[index], position)) {
                return false;
            }
            if (!conditions[index].predicate().test(readLong(values[index], position))) {
                return false;
            }
        }
        return true;
    }

    private static long readLong(Vector vector, int position)
    {
        return switch (vector) {
            case I64Vector values -> values.values()[position];
            case I32Vector values -> values.values()[position];
            case DictionaryVector values -> readLong(values.values(), values.ids()[position]);
            case RleVector values -> readLong(values.values(), values.runIndex(position));
            default -> throw new IllegalArgumentException("Expected integer vector but found " + vector.getClass().getSimpleName());
        };
    }

    private static boolean isNull(boolean[] nulls, int position)
    {
        return nulls != null && nulls[position];
    }

    private record BatchState(Batch sourceBatch, Mask[] maskHolder)
    {
        private BatchState(Batch sourceBatch, Mask mask)
        {
            this(sourceBatch, new Mask[] {mask});
        }

        private void constrain(Mask mask)
        {
            maskHolder[0] = mask;
            sourceBatch.constrain(mask);
        }
    }

    public record Condition(int inputIndex, LongPredicate predicate)
    {
        public Condition
        {
            checkArgument(inputIndex >= 0, "inputIndex is negative");
        }
    }
}
