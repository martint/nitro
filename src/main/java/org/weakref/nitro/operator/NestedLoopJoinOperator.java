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
import org.weakref.nitro.data.Mask;

import java.util.Iterator;

public class NestedLoopJoinOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("NestedLoopJoinOperator");
    private static final int BATCH_SIZE = 1024;

    private final Allocator allocator;
    private final Operator outer;
    private final Operator inner;
    private final JoinMatcher matcher;
    private final JoinBufferSupport buffers;
    private final BufferedJoinInput bufferedInner;
    private final JoinOutputBuffer outputBuffer;

    private int currentInnerBatch;
    private int currentInnerPosition;

    private Mask currentOuterMask;
    private Batch currentOuterBatch;
    private int outerRemaining;
    private Iterator<Integer> outerPositionIterator;
    private int currentOuterPosition;
    private boolean currentOuterPositionReady;

    private boolean done;

    public NestedLoopJoinOperator(Allocator allocator, Operator outer, Operator inner)
    {
        this(allocator, outer, inner, new CrossJoinMatcher());
    }

    public NestedLoopJoinOperator(Allocator allocator, Operator outer, int outerJoinColumn, Operator inner, int innerJoinColumn)
    {
        this(allocator, outer, inner, new EquiJoinMatcher(outerJoinColumn, innerJoinColumn));
    }

    public NestedLoopJoinOperator(Allocator allocator, Operator outer, int[] outerJoinColumns, Operator inner, int[] innerJoinColumns)
    {
        this(allocator, outer, inner, new EquiJoinMatcher(outerJoinColumns, innerJoinColumns));
    }

    private NestedLoopJoinOperator(Allocator allocator, Operator outer, Operator inner, JoinMatcher matcher)
    {
        this.allocator = allocator;
        this.outer = outer;
        this.inner = inner;
        this.matcher = matcher;
        this.buffers = new JoinBufferSupport(allocator, ALLOCATION_CONTEXT);
        this.bufferedInner = new BufferedJoinInput(buffers, inner.outputCount());
        this.outputBuffer = new JoinOutputBuffer(buffers, outer.outputCount(), inner.outputCount());
    }

    @Override
    public int outputCount()
    {
        return outer.outputCount() + inner.outputCount();
    }

    @Override
    public boolean hasNext()
    {
        return !done;
    }

    private Mask produceBatch()
    {
        if (!matcher.isCrossJoin()) {
            return produceEquiJoinBatch();
        }

        loadInnerIfNecessary();
        if (bufferedInner.rowCount() == 0) {
            done = true;
            return allocator.allocateAllMask(ALLOCATION_CONTEXT, 0);
        }

        if (outerRemaining == 0) {
            if (!loadNextOuterBatch()) {
                done = true;
                return allocator.allocateAllMask(ALLOCATION_CONTEXT, 0);
            }
        }

        if (currentInnerBatch == 0 && currentInnerPosition == 0 && outerPositionIterator.hasNext()) {
            currentOuterPosition = outerPositionIterator.next();
        }

        int innerRemaining = bufferedInner.batches().get(currentInnerBatch).length() - currentInnerPosition;

        int innerProcessed;
        int outerProcessed;

        Mask mask;
        if (outerRemaining < innerRemaining) {
            int batchSize = joinWithOuterRow();

            mask = allocator.allocateAllMask(ALLOCATION_CONTEXT, batchSize);
            innerProcessed = batchSize;
            outerProcessed = 1;
        }
        else {
            joinWithInnerRow();

            mask = allocator.lastMask(ALLOCATION_CONTEXT, currentOuterMask, outerRemaining);
            innerProcessed = 1;
            outerProcessed = outerRemaining;

            // TODO: should we compact outer if mask != all?
            //    tradeoff: if we don't compact, inner will have to be replicated to cover all outer rows
            //              if we do, extra copy for outer and inability to just transfer ownership of outer columns
            //    maybe, we need a way to represent an RLE vector with holes?
        }

        currentInnerPosition += innerProcessed;
        if (currentInnerPosition == bufferedInner.batches().get(currentInnerBatch).length()) {
            currentInnerBatch++;
            currentInnerPosition = 0;
        }

        if (currentInnerBatch == bufferedInner.batches().size()) {
            currentInnerBatch = 0;
            outerRemaining -= outerProcessed;
        }

        if (outerRemaining == 0 && !outer.hasNext()) {
            done = true;
        }

        return mask;
    }

    private Mask produceEquiJoinBatch()
    {
        loadInnerIfNecessary();
        if (bufferedInner.rowCount() == 0) {
            done = true;
            outputBuffer.clearResults();
            return allocator.allocateAllMask(ALLOCATION_CONTEXT, 0);
        }

        int outputPosition = 0;
        while (outputPosition < BATCH_SIZE) {
            if (outerRemaining == 0 && !loadNextOuterBatch()) {
                done = true;
                break;
            }

            if (!currentOuterPositionReady) {
                if (!outerPositionIterator.hasNext()) {
                    outerRemaining = 0;
                    continue;
                }
                currentOuterPosition = outerPositionIterator.next();
                currentOuterPositionReady = true;
                currentInnerBatch = 0;
                currentInnerPosition = 0;
            }

            while (currentInnerBatch < bufferedInner.batches().size() && outputPosition < BATCH_SIZE) {
                BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(currentInnerBatch);
                while (currentInnerPosition < innerBatch.length() && outputPosition < BATCH_SIZE) {
                    if (matcher.matches(currentOuterBatch, currentOuterPosition, innerBatch.columns(), currentInnerPosition)) {
                        outputBuffer.appendMatchAt(currentOuterBatch, currentOuterPosition, innerBatch, currentInnerPosition, outputPosition, BATCH_SIZE);
                        outputPosition++;
                    }
                    currentInnerPosition++;
                }
                if (currentInnerPosition == innerBatch.length()) {
                    currentInnerBatch++;
                    currentInnerPosition = 0;
                }
            }

            if (currentInnerBatch == bufferedInner.batches().size()) {
                currentInnerBatch = 0;
                currentInnerPosition = 0;
                outerRemaining--;
                currentOuterPositionReady = false;
            }
        }

        if (outputPosition == 0) {
            outputBuffer.clearResults();
            return allocator.allocateAllMask(ALLOCATION_CONTEXT, 0);
        }
        return allocator.allocateRangeMask(ALLOCATION_CONTEXT, 0, outputPosition);
    }

    private boolean loadNextOuterBatch()
    {
        while (outer.hasNext()) {
            currentOuterBatch = outer.next();
            outputBuffer.captureOuterSchema(currentOuterBatch);
            currentOuterMask = currentOuterBatch.borrowMask();
            if (!currentOuterMask.none()) {
                outerPositionIterator = currentOuterMask.iterator();
                outerRemaining = currentOuterMask.count();
                currentOuterPositionReady = false;
                return true;
            }
        }
        return false;
    }

    @Override
    public Batch next()
    {
        Mask batchMask = produceBatch();
        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            outputs[outputIndex] = outputBuffer.resultOutputForNestedLoop(outputIndex, currentOuterBatch, allocator, ALLOCATION_CONTEXT);
        }
        return new Batch(
                batchMask,
                takenMask -> takenMask == currentOuterMask ? currentOuterBatch.takeMask() : allocator.transfer(ALLOCATION_CONTEXT, takenMask),
                outputs);
    }

    private void joinWithInnerRow()
    {
        outputBuffer.joinWithInnerRow(currentOuterBatch, currentOuterMask, bufferedInner.batches().get(currentInnerBatch), currentInnerPosition);
    }

    private int joinWithOuterRow()
    {
        return outputBuffer.joinWithOuterRow(currentOuterBatch, currentOuterPosition, bufferedInner.batches().get(currentInnerBatch));
    }

    private void loadInnerIfNecessary()
    {
        bufferedInner.loadAll(inner, BATCH_SIZE);
    }

    @Override
    public void constrain(Mask mask)
    {
    }

    @Override
    public void close()
    {
        outer.close();
        inner.close();
        allocator.release(ALLOCATION_CONTEXT);
    }
}
