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
import org.weakref.nitro.data.I64VectorWithNulls;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class NestedLoopJoinOperator
        implements BatchOperator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("NestedLoopJoinOperator");
    private static final int BATCH_SIZE = 1024;

    private final Allocator allocator;
    private final BatchOperator outer;
    private final BatchOperator inner;

    private boolean innerLoaded;
    private final List<InnerBatch> innerBatches = new ArrayList<>();
    private long innerRowCount;

    private int currentInnerBatch;
    private int currentInnerPosition;

    private Mask currentOuterMask;
    private Batch currentOuterBatch;
    private int outerRemaining;
    private Iterator<Integer> outerPositionIterator;
    private int currentOuterPosition;

    private final Vector[] result;
    private final Vector[] outerBuffer; // buffer to hold output from outer columns when replicating the same outer row for multiple inner rows
    private final Vector[] innerBuffer; // buffer to hold output from inner columns when replicating the same inner row for multiple outer rows

    private boolean done;

    public NestedLoopJoinOperator(Allocator allocator, BatchOperator outer, BatchOperator inner)
    {
        this.allocator = allocator;
        this.outer = outer;
        this.inner = inner;
        result = new Vector[outer.outputCount() + inner.outputCount()];
        outerBuffer = new Vector[outer.outputCount()];
        innerBuffer = new Vector[inner.outputCount()];
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
        loadInnerIfNecessary();
        if (innerRowCount == 0) {
            done = true;
            return Mask.all(0);
        }

        if (outerRemaining == 0) {
            while (outer.hasNext()) {
                currentOuterBatch = outer.nextBatch();
                currentOuterMask = currentOuterBatch.borrowMask();
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

    @Override
    public Batch nextBatch()
    {
        Mask batchMask = produceBatch();
        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            int output = outputIndex;
            outputs[outputIndex] = Output.lazyValues(() -> result[output]);
        }
        return new Batch(batchMask, outputs);
    }

    private void joinWithInnerRow()
    {
        int outerColumnCount = outer.outputCount();
        for (int i = 0; i < outerColumnCount; i++) {
            result[i] = currentOuterBatch.output(i).borrow(Stream.VALUES);
        }
        for (int i = 0; i < inner.outputCount(); i++) {
            innerBuffer[i] = allocator.reallocateIfNecessary(ALLOCATION_CONTEXT, innerBuffer[i], currentOuterMask.maxPosition() + 1, I64VectorWithNulls::new);
            replicate(
                    innerBuffer[i],
                    0,
                    currentOuterMask.maxPosition() + 1,
                    innerBatches.get(currentInnerBatch).columns()[i],
                    currentInnerPosition);

            result[i + outerColumnCount] = innerBuffer[i];
        }
    }

    private int joinWithOuterRow()
    {
        int batchSize = innerBatches.get(currentInnerBatch).length();

        int outerColumnCount = outer.outputCount();
        for (int i = 0; i < outerColumnCount; i++) {
            outerBuffer[i] = allocator.reallocateIfNecessary(ALLOCATION_CONTEXT, outerBuffer[i], batchSize, I64VectorWithNulls::new);
            replicate(
                    outerBuffer[i],
                    0,
                    batchSize,
                    currentOuterBatch.output(i).borrow(Stream.VALUES),
                    currentOuterPosition);

            result[i] = outerBuffer[i];
        }
        System.arraycopy(innerBatches.get(currentInnerBatch).columns(), 0, result, outerColumnCount, inner.outputCount());
        return batchSize;
    }

    private void replicate(Vector output, int start, int length, Vector input, int position)
    {
        I64VectorWithNulls outputVector = (I64VectorWithNulls) output;
        long value = values(input)[position];
        boolean isNull = input instanceof I64VectorWithNulls iv && iv.nulls()[position];

        Arrays.fill(outputVector.values(), start, start + length, value);
        Arrays.fill(outputVector.nulls(), start, start + length, isNull);
    }

    private void loadInnerIfNecessary()
    {
        if (!innerLoaded) {
            innerLoaded = true;

            Vector[] columns = allocateNewBatch(inner.outputCount());
            int outputPosition = 0;
            innerRowCount = 0;

            while (inner.hasNext()) {
                Batch batch = inner.nextBatch();
                Mask mask = batch.borrowMask();
                int maskOffset = 0;
                while (maskOffset < mask.count()) {
                    int copied = 0;
                    for (int i = 0; i < columns.length; i++) {
                        // TODO: allow transferring ownership from underlying operator in case we don't need to copy+compact
                        copied = copyAndCompact(batch.output(i).borrow(Stream.VALUES), mask, maskOffset, (I64VectorWithNulls) columns[i], outputPosition);
                    }
                    outputPosition += copied;
                    maskOffset += copied;
                    innerRowCount += copied;

                    if (outputPosition == BATCH_SIZE) {
                        outputPosition = 0;
                        innerBatches.add(new InnerBatch(columns, BATCH_SIZE));
                        columns = allocateNewBatch(inner.outputCount());
                    }
                }
            }

            if (outputPosition > 0) {
                innerBatches.add(new InnerBatch(columns, outputPosition));
            }
        }
    }

    private Vector[] allocateNewBatch(int columnCount)
    {
        Vector[] columns = new Vector[columnCount];
        for (int i = 0; i < columnCount; i++) {
            columns[i] = allocator.allocate(ALLOCATION_CONTEXT, BATCH_SIZE, I64VectorWithNulls::new);
        }
        return columns;
    }

    /**
     * @return the number of elements copied
     */
    private int copyAndCompact(Vector inputVec, Mask mask, int maskStart, I64VectorWithNulls output, int outputStart)
    {
        long[] inputValues = values(inputVec);
        boolean[] inputNulls = inputVec instanceof I64VectorWithNulls iv ? iv.nulls() : null;

        int outputPosition = outputStart;
        int maskIndex = maskStart;

        if (mask.all()) {
            int length = Math.min(mask.count() - maskStart, output.length() - outputPosition);
            if (inputNulls != null) {
                System.arraycopy(inputNulls, maskStart, output.nulls(), outputPosition, length);
            }
            System.arraycopy(inputValues, maskStart, output.values(), outputPosition, length);
            outputPosition += length;
        }
        else {
            while (outputPosition < output.length() && maskIndex < mask.count()) {
                int inputPosition = mask.position(maskIndex);
                output.nulls()[outputPosition] = inputNulls != null && inputNulls[inputPosition];
                output.values()[outputPosition] = inputValues[inputPosition];
                outputPosition++;
                maskIndex++;
            }
        }

        return outputPosition - outputStart;
    }

    private static long[] values(Vector v)
    {
        return switch (v) {
            case I64Vector iv -> iv.values();
            case I64VectorWithNulls iv -> iv.values();
            default -> throw new UnsupportedOperationException(v.getClass().getSimpleName());
        };
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

    // TODO: could geeneralize (call it Chunk?) this to have a Mask instead. Not needed for NLJ, but might be useful
    //       for other operators
    record InnerBatch(Vector[] columns, int length) {}
}
