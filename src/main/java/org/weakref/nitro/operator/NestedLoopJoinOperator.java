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
import org.weakref.nitro.operator.evaluator.ir.Stream;

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
    private final List<InnerBatch> innerBatches = new ArrayList<>();
    private long innerRowCount;

    private int currentInnerBatch;
    private int currentInnerPosition;

    private Mask currentOuterMask;
    private Batch currentOuterBatch;
    private int outerRemaining;
    private Iterator<Integer> outerPositionIterator;
    private int currentOuterPosition;

    private final Streams[] result;
    private final Streams[] outerBuffer; // buffer to hold output from outer columns when replicating the same outer row for multiple inner rows
    private final Streams[] innerBuffer; // buffer to hold output from inner columns when replicating the same inner row for multiple outer rows

    private boolean done;

    public NestedLoopJoinOperator(Allocator allocator, Operator outer, Operator inner)
    {
        this.allocator = allocator;
        this.outer = outer;
        this.inner = inner;
        result = new Streams[outer.outputCount() + inner.outputCount()];
        outerBuffer = new Streams[outer.outputCount()];
        innerBuffer = new Streams[inner.outputCount()];
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
            return allocator.allocateAllMask(ALLOCATION_CONTEXT, 0);
        }

        if (outerRemaining == 0) {
            while (outer.hasNext()) {
                currentOuterBatch = outer.next();
                currentOuterMask = currentOuterBatch.borrowMask();
                if (!currentOuterMask.none()) {
                    break;
                }
            }
            outerPositionIterator = currentOuterMask.iterator();
            outerRemaining = currentOuterMask.count();

            if (outerRemaining == 0) {
                done = true;
                return allocator.allocateAllMask(ALLOCATION_CONTEXT, 0);
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
    public Batch next()
    {
        Mask batchMask = produceBatch();
        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            outputs[outputIndex] = resultOutput(outputIndex);
        }
        return new Batch(
                batchMask,
                takenMask -> takenMask == currentOuterMask ? currentOuterBatch.takeMask() : allocator.transfer(ALLOCATION_CONTEXT, takenMask),
                outputs);
    }

    private Output resultOutput(int outputIndex)
    {
        Streams streams = result[outputIndex];
        if (streams == null) {
            I64Vector empty = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, 0, I64Vector::new);
            return new Output(java.util.Set.of(Stream.VALUES), stream -> empty, (stream, vector) -> allocator.transfer(ALLOCATION_CONTEXT, vector));
        }

        int outerColumnCount = outer.outputCount();
        if (outputIndex < outerColumnCount) {
            if (streams == outerBuffer[outputIndex]) {
                return new Output(
                        streams.asMap().keySet(),
                        streams::get,
                        (stream, vector) -> {
                            result[outputIndex] = null;
                            outerBuffer[outputIndex] = null;
                            return allocator.transfer(ALLOCATION_CONTEXT, vector);
                        });
            }

            Output sourceOutput = currentOuterBatch.output(outputIndex);
            return new Output(sourceOutput.streams(), sourceOutput::borrow, (stream, vector) -> sourceOutput.take(stream));
        }

        int innerIndex = outputIndex - outerColumnCount;
        if (streams == innerBuffer[innerIndex]) {
            return new Output(
                    streams.asMap().keySet(),
                    streams::get,
                    (stream, vector) -> {
                        result[outputIndex] = null;
                        innerBuffer[innerIndex] = null;
                        return allocator.transfer(ALLOCATION_CONTEXT, vector);
                    });
        }

        return new Output(
                streams.asMap().keySet(),
                streams::get,
                (stream, vector) -> vector.copy(vector.length()));
    }

    private void joinWithInnerRow()
    {
        int outerColumnCount = outer.outputCount();
        for (int i = 0; i < outerColumnCount; i++) {
            result[i] = toStreams(currentOuterBatch.output(i));
        }
        for (int i = 0; i < inner.outputCount(); i++) {
            innerBuffer[i] = allocateNullableI64Buffer(innerBuffer[i], currentOuterMask.maxPosition() + 1);
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
            outerBuffer[i] = allocateNullableI64Buffer(outerBuffer[i], batchSize);
            replicate(
                    outerBuffer[i],
                    0,
                    batchSize,
                    toStreams(currentOuterBatch.output(i)),
                    currentOuterPosition);

            result[i] = outerBuffer[i];
        }
        System.arraycopy(innerBatches.get(currentInnerBatch).columns(), 0, result, outerColumnCount, inner.outputCount());
        return batchSize;
    }

    private void replicate(Streams output, int start, int length, Streams input, int position)
    {
        I64Vector outputValues = (I64Vector) output.values();
        BooleanVector outputNulls = (BooleanVector) output.get(Stream.NULLS);
        I64Vector inputValues = (I64Vector) input.values();
        BooleanVector inputNulls = (BooleanVector) input.getOrNull(Stream.NULLS);
        long value = inputValues.values()[position];
        boolean isNull = inputNulls != null && inputNulls.values()[position];

        Arrays.fill(outputValues.values(), start, start + length, value);
        Arrays.fill(outputNulls.values(), start, start + length, isNull);
    }

    private void loadInnerIfNecessary()
    {
        if (!innerLoaded) {
            innerLoaded = true;

            Streams[] columns = allocateNewBatch(inner.outputCount());
            int outputPosition = 0;
            innerRowCount = 0;

            while (inner.hasNext()) {
                Batch batch = inner.next();
                Mask mask = batch.borrowMask();
                int maskOffset = 0;
                while (maskOffset < mask.count()) {
                    int copied = 0;
                    for (int i = 0; i < columns.length; i++) {
                        // TODO: allow transferring ownership from underlying operator in case we don't need to copy+compact
                        copied = copyAndCompact(batch.output(i), mask, maskOffset, columns[i], outputPosition);
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

    private Streams[] allocateNewBatch(int columnCount)
    {
        Streams[] columns = new Streams[columnCount];
        for (int i = 0; i < columnCount; i++) {
            columns[i] = allocateNullableI64Buffer(null, BATCH_SIZE);
        }
        return columns;
    }

    /**
     * @return the number of elements copied
     */
    private int copyAndCompact(Output input, Mask mask, int maskStart, Streams output, int outputStart)
    {
        long[] inputValues = ((I64Vector) input.borrow(Stream.VALUES)).values();
        BooleanVector inputNullsVector = (BooleanVector) input.borrowOrNull(Stream.NULLS);
        boolean[] inputNulls = inputNullsVector != null ? inputNullsVector.values() : null;
        I64Vector outputValues = (I64Vector) output.values();
        BooleanVector outputNulls = (BooleanVector) output.get(Stream.NULLS);

        int outputPosition = outputStart;
        int maskIndex = maskStart;

        if (mask.all()) {
            int length = Math.min(mask.count() - maskStart, outputValues.length() - outputPosition);
            if (inputNulls != null) {
                System.arraycopy(inputNulls, maskStart, outputNulls.values(), outputPosition, length);
            }
            else {
                Arrays.fill(outputNulls.values(), outputPosition, outputPosition + length, false);
            }
            System.arraycopy(inputValues, maskStart, outputValues.values(), outputPosition, length);
            outputPosition += length;
        }
        else {
            while (outputPosition < outputValues.length() && maskIndex < mask.count()) {
                int inputPosition = mask.position(maskIndex);
                outputNulls.values()[outputPosition] = inputNulls != null && inputNulls[inputPosition];
                outputValues.values()[outputPosition] = inputValues[inputPosition];
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
    public void close()
    {
        outer.close();
        inner.close();
        allocator.release(ALLOCATION_CONTEXT);
    }

    // TODO: could geeneralize (call it Chunk?) this to have a Mask instead. Not needed for NLJ, but might be useful
    //       for other operators
    private Streams allocateNullableI64Buffer(Streams existing, int size)
    {
        I64Vector values = allocator.reallocateIfNecessary(
                ALLOCATION_CONTEXT,
                existing != null && existing.has(Stream.VALUES) ? (I64Vector) existing.values() : null,
                I64Vector.class,
                size,
                I64Vector::new);
        BooleanVector nulls = allocator.reallocateIfNecessary(
                ALLOCATION_CONTEXT,
                existing != null && existing.has(Stream.NULLS) ? (BooleanVector) existing.get(Stream.NULLS) : null,
                BooleanVector.class,
                size,
                BooleanVector::new);
        return Streams.ofValuesAndNulls(values, nulls);
    }

    private static Streams toStreams(Output output)
    {
        I64Vector values = (I64Vector) output.borrow(Stream.VALUES);
        BooleanVector nulls = (BooleanVector) output.borrowOrNull(Stream.NULLS);
        if (nulls != null) {
            return Streams.ofValuesAndNulls(values, nulls);
        }
        return Streams.ofValues(values);
    }

    record InnerBatch(Streams[] columns, int length) {}
}
