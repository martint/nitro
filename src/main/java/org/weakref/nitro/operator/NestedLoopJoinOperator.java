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
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class NestedLoopJoinOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("NestedLoopJoinOperator");
    private static final int BATCH_SIZE = 1024;

    private final Allocator allocator;
    private final Operator outer;
    private final Operator inner;

    private boolean innerLoaded;
    private final List<Batch> innerBatches = new ArrayList<>();
    private long innerRowCount;

    private int currentInnerBatch;
    private int currentInnerPosition;

    private Mask currentOuterMask;
    private int outerRemaining;
    private Iterator<Integer> outerPositionIterator;
    private int currentOuterPosition;

    private final Vector[] result;
    private final Vector[] outerBuffer; // buffer to hold output from outer columns when replicating the same outer row for multiple inner rows
    private final Vector[] innerBuffer; // buffer to hold output from inner columns when replicating the same inner row for multiple outer rows

    private boolean done;

    public NestedLoopJoinOperator(Allocator allocator, Operator outer, Operator inner)
    {
        this.allocator = allocator;
        this.outer = outer;
        this.inner = inner;
        result = new Vector[outer.columnCount() + inner.columnCount()];
        outerBuffer = new Vector[outer.columnCount()];
        innerBuffer = new Vector[inner.columnCount()];
    }

    @Override
    public int columnCount()
    {
        return outer.columnCount() + inner.columnCount();
    }

    @Override
    public boolean hasNext()
    {
        return !done;
    }

    @Override
    public Mask next()
    {
        loadInnerIfNecessary();
        if (innerRowCount == 0) {
            done = true;
            return Mask.all(0);
        }

        if (outerRemaining == 0) {
            while (outer.hasNext()) {
                currentOuterMask = outer.next();
                if (!currentOuterMask.none()) {
                    break;
                }
            }
            outerPositionIterator = currentOuterMask.iterator();
            outerRemaining = currentOuterMask.count();

            if (outerRemaining == 0) {
                done = true;
                return Mask.all(0);
            }
        }

        if (currentInnerBatch == 0 && currentInnerPosition == 0 && outerPositionIterator.hasNext()) {
            currentOuterPosition = outerPositionIterator.next();
        }

        int innerRemaining = innerBatches.get(currentInnerBatch).length() - currentInnerPosition;

        int innerProcessed;
        int outerProcessed;

        Mask mask;
        if (outerRemaining < innerRemaining) {
            int batchSize = joinWithOuterRow();

            mask = Mask.all(batchSize);
            innerProcessed = batchSize;
            outerProcessed = 1;
        }
        else {
            joinWithInnerRow();

            mask = currentOuterMask.last(outerRemaining);
            innerProcessed = 1;
            outerProcessed = outerRemaining;

            // TODO: should we compact outer if mask != all?
            //    tradeoff: if we don't compact, inner will have to be replicated to cover all outer rows
            //              if we do, extra copy for outer and inability to just transfer ownership of outer columns
            //    maybe, we need a way to represent an RLE vector with holes?
        }

        currentInnerPosition += innerProcessed;
        if (currentInnerPosition == innerBatches.get(currentInnerBatch).length()) {
            currentInnerBatch++;
            currentInnerPosition = 0;
        }

        if (currentInnerBatch == innerBatches.size()) {
            currentInnerBatch = 0;
            outerRemaining -= outerProcessed;
        }

        if (outerRemaining == 0 && !outer.hasNext()) {
            done = true;
        }

        return mask;
    }

    private void joinWithInnerRow()
    {
        int outerColumnCount = outer.columnCount();
        for (int i = 0; i < outerColumnCount; i++) {
            result[i] = outer.column(i);
        }
        for (int i = 0; i < inner.columnCount(); i++) {
            innerBuffer[i] = allocator.reallocateIfNecessary(ALLOCATION_CONTEXT, innerBuffer[i], currentOuterMask.maxPosition() + 1, I64Vector::new);
            replicate(
                    innerBuffer[i],
                    0,
                    currentOuterMask.maxPosition() + 1,
                    innerBatches.get(currentInnerBatch).columns[i],
                    currentInnerPosition);

            result[i + outerColumnCount] = innerBuffer[i];
        }
    }

    private int joinWithOuterRow()
    {
        int batchSize = innerBatches.get(currentInnerBatch).length();

        int outerColumnCount = outer.columnCount();
        for (int i = 0; i < outerColumnCount; i++) {
            outerBuffer[i] = allocator.reallocateIfNecessary(ALLOCATION_CONTEXT, outerBuffer[i], batchSize, I64Vector::new);
            replicate(
                    outerBuffer[i],
                    0,
                    batchSize,
                    outer.column(i),
                    currentOuterPosition);

            result[i] = outerBuffer[i];
        }
        System.arraycopy(innerBatches.get(currentInnerBatch).columns(), 0, result, outerColumnCount, inner.columnCount());
        return batchSize;
    }

    private void replicate(Vector output, int start, int length, Vector input, int position)
    {
        I64Vector outputVector = (I64Vector) output;
        I64Vector inputVector = (I64Vector) input;

        Arrays.fill(outputVector.values(), start, start + length, inputVector.values()[position]);
        Arrays.fill(outputVector.nulls(), start, start + length, inputVector.nulls()[position]);
    }

    private void loadInnerIfNecessary()
    {
        if (!innerLoaded) {
            innerLoaded = true;

            Vector[] columns = allocateNewBatch(inner.columnCount());
            int outputPosition = 0;
            innerRowCount = 0;

            while (inner.hasNext()) {
                Mask mask = inner.next();
                int maskOffset = 0;
                while (maskOffset < mask.count()) {
                    int copied = 0;
                    for (int i = 0; i < columns.length; i++) {
                        // TODO: allow transferring ownership from underlying operator in case we don't need to copy+compact
                        copied = copyAndCompact((I64Vector) inner.column(i), mask, maskOffset, (I64Vector) columns[i], outputPosition);
                    }
                    outputPosition += copied;
                    maskOffset += copied;
                    innerRowCount += copied;

                    if (outputPosition == BATCH_SIZE) {
                        outputPosition = 0;
                        innerBatches.add(new Batch(columns, BATCH_SIZE));
                        columns = allocateNewBatch(inner.columnCount());
                    }
                }
            }

            if (outputPosition > 0) {
                innerBatches.add(new Batch(columns, outputPosition));
            }
        }
    }

    private Vector[] allocateNewBatch(int columnCount)
    {
        Vector[] columns = new Vector[columnCount];
        for (int i = 0; i < columnCount; i++) {
            columns[i] = allocator.allocate(ALLOCATION_CONTEXT, BATCH_SIZE, I64Vector::new);
        }
        return columns;
    }

    /**
     * @return the number of elements copied
     */
    private int copyAndCompact(I64Vector input, Mask mask, int maskStart, I64Vector output, int outputStart)
    {
        int outputPosition = outputStart;
        int maskIndex = maskStart;

        if (mask.all()) {
            int length = Math.min(mask.count() - maskStart, output.length() - outputPosition);
            System.arraycopy(input.nulls(), maskStart, output.nulls(), outputPosition, length);
            System.arraycopy(input.values(), maskStart, output.values(), outputPosition, length);
            outputPosition += length;
        }
        else {
            while (outputPosition < output.length() && maskIndex < mask.count()) {
                int inputPosition = mask.position(maskIndex);
                output.nulls()[outputPosition] = input.nulls()[inputPosition];
                output.values()[outputPosition] = input.values()[inputPosition];
                outputPosition++;
                maskIndex++;
            }
        }

        return outputPosition - outputStart;
    }

    @Override
    public void constrain(Mask mask)
    {
    }

    @Override
    public Vector column(int column)
    {
        return result[column];
    }

    @Override
    public void close()
    {
        outer.close();
        inner.close();
        allocator.release(ALLOCATION_CONTEXT);
    }

    // TODO: could geeneralize (call it Chunk?) this to have a Mask instead. Not needed for NLJ, but might be useful
    //       for other operators
    record Batch(Vector[] columns, int length) {}
}
