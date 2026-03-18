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

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongLists;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.HashMap;
import java.util.Map;

public class HashJoinOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("HashJoinOperator");
    private static final int BATCH_SIZE = 1024;

    private final Allocator allocator;
    private final Operator outer;
    private final Operator inner;
    private final int[] outerJoinColumns;
    private final int[] innerJoinColumns;
    private final JoinBufferSupport buffers;
    private final BufferedJoinInput bufferedInner;
    private final JoinOutputBuffer outputBuffer;
    private final OperatorKeySemantics.Key[] outerProbeKeys;
    private final OperatorKeySemantics.Key[] innerProbeKeys;
    private final OperatorKeySemantics.CompositeProbeKey outerCompositeProbeKey;
    private final OperatorKeySemantics.CompositeProbeKey innerCompositeProbeKey;
    private final Vector[] currentOuterJoinValues;
    private final BooleanVector[] currentOuterJoinNulls;

    private final Map<OperatorKeySemantics.Key, LongArrayList> innerIndex = new HashMap<>();

    private Mask currentOuterMask;
    private Batch currentOuterBatch;
    private int currentOuterMaskIndex;
    private int outerRemaining;
    private int currentOuterPosition;
    private boolean currentOuterPositionReady;
    private LongList currentMatches = LongLists.emptyList();
    private int currentMatchIndex;

    private boolean done;

    public HashJoinOperator(Allocator allocator, Operator outer, int outerJoinColumn, Operator inner, int innerJoinColumn)
    {
        this(allocator, outer, new int[] {outerJoinColumn}, inner, new int[] {innerJoinColumn});
    }

    public HashJoinOperator(Allocator allocator, Operator outer, int[] outerJoinColumns, Operator inner, int[] innerJoinColumns)
    {
        if (outerJoinColumns.length != innerJoinColumns.length) {
            throw new IllegalArgumentException("Join key counts must match");
        }
        if (outerJoinColumns.length == 0) {
            throw new IllegalArgumentException("Hash join requires at least one join key");
        }

        this.allocator = allocator;
        this.outer = outer;
        this.inner = inner;
        this.outerJoinColumns = outerJoinColumns.clone();
        this.innerJoinColumns = innerJoinColumns.clone();
        this.buffers = new JoinBufferSupport(allocator, ALLOCATION_CONTEXT);
        this.bufferedInner = new BufferedJoinInput(buffers, inner.outputCount());
        this.outputBuffer = new JoinOutputBuffer(buffers, outer.outputCount(), inner.outputCount());
        this.outerProbeKeys = new OperatorKeySemantics.Key[outerJoinColumns.length];
        this.innerProbeKeys = new OperatorKeySemantics.Key[innerJoinColumns.length];
        this.outerCompositeProbeKey = outerJoinColumns.length > 1 ? OperatorKeySemantics.reusableCompositeProbeKey(outerJoinColumns.length) : null;
        this.innerCompositeProbeKey = innerJoinColumns.length > 1 ? OperatorKeySemantics.reusableCompositeProbeKey(innerJoinColumns.length) : null;
        this.currentOuterJoinValues = new Vector[outerJoinColumns.length];
        this.currentOuterJoinNulls = new BooleanVector[outerJoinColumns.length];
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

    @Override
    public Batch next()
    {
        Mask batchMask = produceBatch();
        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            outputs[outputIndex] = outputBuffer.resultOutputForHashJoin(outputIndex, allocator, ALLOCATION_CONTEXT);
        }
        return new Batch(
                batchMask,
                takenMask -> allocator.transfer(ALLOCATION_CONTEXT, takenMask),
                outputs);
    }

    private Mask produceBatch()
    {
        loadInnerIfNecessary();
        if (innerIndex.isEmpty()) {
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
                if (currentOuterMaskIndex >= currentOuterMask.count()) {
                    outerRemaining = 0;
                    continue;
                }
                currentOuterPosition = currentOuterMask.position(currentOuterMaskIndex++);
                currentOuterPositionReady = true;
                currentMatches = matchesForOuterPosition();
                currentMatchIndex = 0;
            }

            while (currentMatchIndex < currentMatches.size() && outputPosition < BATCH_SIZE) {
                long match = currentMatches.getLong(currentMatchIndex++);
                outputBuffer.appendMatchAt(
                        currentOuterBatch,
                        currentOuterPosition,
                        bufferedInner.batches().get(batchIndex(match)),
                        rowPosition(match),
                        outputPosition,
                        BATCH_SIZE);
                outputPosition++;
            }

            if (currentMatchIndex == currentMatches.size()) {
                outerRemaining--;
                currentOuterPositionReady = false;
                currentMatches = LongLists.emptyList();
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
                cacheOuterJoinInputs();
                currentOuterMaskIndex = 0;
                outerRemaining = currentOuterMask.count();
                currentOuterPositionReady = false;
                return true;
            }
        }
        return false;
    }

    private LongList matchesForOuterPosition()
    {
        OperatorKeySemantics.Key key = keyForOuterPosition();
        if (key == null) {
            return LongLists.emptyList();
        }
        LongArrayList matches = innerIndex.get(key);
        return matches == null ? LongLists.emptyList() : matches;
    }

    private OperatorKeySemantics.Key keyForOuterPosition()
    {
        for (int keyIndex = 0; keyIndex < outerJoinColumns.length; keyIndex++) {
            if (outerProbeKeys[keyIndex] == null) {
                outerProbeKeys[keyIndex] = OperatorKeySemantics.reusableProbeKey(currentOuterJoinValues[keyIndex]);
            }
            OperatorKeySemantics.Key key = OperatorKeySemantics.probeKey(
                    currentOuterJoinValues[keyIndex],
                    currentOuterJoinNulls[keyIndex],
                    currentOuterPosition,
                    outerProbeKeys[keyIndex]);
            if (key == null) {
                return null;
            }
            outerProbeKeys[keyIndex] = key;
        }
        return OperatorKeySemantics.probeCompositeKey(outerProbeKeys, outerCompositeProbeKey);
    }

    private void loadInnerIfNecessary()
    {
        int batchCountBefore = bufferedInner.batches().size();
        bufferedInner.loadAll(inner, BATCH_SIZE);
        for (int batchIndex = batchCountBefore; batchIndex < bufferedInner.batches().size(); batchIndex++) {
            BufferedJoinInput.InnerBatch batch = bufferedInner.batches().get(batchIndex);
            indexInnerRows(batch.columns(), 0, batch.length(), batchIndex);
        }
    }

    private void indexInnerRows(Streams[] columns, int startPosition, int length, int batchIndex)
    {
        Vector[] joinValues = new Vector[innerJoinColumns.length];
        BooleanVector[] joinNulls = new BooleanVector[innerJoinColumns.length];
        for (int keyIndex = 0; keyIndex < innerJoinColumns.length; keyIndex++) {
            Streams streams = columns[innerJoinColumns[keyIndex]];
            joinValues[keyIndex] = streams.values();
            joinNulls[keyIndex] = (BooleanVector) streams.getOrNull(Stream.NULLS);
            if (innerProbeKeys[keyIndex] == null) {
                innerProbeKeys[keyIndex] = OperatorKeySemantics.reusableProbeKey(joinValues[keyIndex]);
            }
        }

        for (int position = startPosition; position < startPosition + length; position++) {
            boolean hasNull = false;
            for (int keyIndex = 0; keyIndex < innerJoinColumns.length; keyIndex++) {
                OperatorKeySemantics.Key key = OperatorKeySemantics.probeKey(
                        joinValues[keyIndex],
                        joinNulls[keyIndex],
                        position,
                        innerProbeKeys[keyIndex]);
                if (key == null) {
                    hasNull = true;
                    break;
                }
                innerProbeKeys[keyIndex] = key;
            }
            if (hasNull) {
                continue;
            }
            OperatorKeySemantics.Key compositeKey = OperatorKeySemantics.probeCompositeKey(innerProbeKeys, innerCompositeProbeKey);
            LongArrayList rows = innerIndex.get(compositeKey);
            if (rows == null) {
                OperatorKeySemantics.Key ownedKey = OperatorKeySemantics.ownedKey(compositeKey);
                rows = new LongArrayList();
                innerIndex.put(ownedKey, rows);
            }
            rows.add(packRowReference(batchIndex, position));
        }
    }

    private void cacheOuterJoinInputs()
    {
        for (int keyIndex = 0; keyIndex < outerJoinColumns.length; keyIndex++) {
            Output output = currentOuterBatch.output(outerJoinColumns[keyIndex]);
            currentOuterJoinValues[keyIndex] = output.borrow(Stream.VALUES);
            currentOuterJoinNulls[keyIndex] = (BooleanVector) output.borrowOrNull(Stream.NULLS);
        }
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

    private static long packRowReference(int batchIndex, int position)
    {
        return ((long) batchIndex << Integer.SIZE) | (position & 0xFFFF_FFFFL);
    }

    private static int batchIndex(long rowReference)
    {
        return (int) (rowReference >>> Integer.SIZE);
    }

    private static int rowPosition(long rowReference)
    {
        return (int) rowReference;
    }
}
