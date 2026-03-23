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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
    private final Vector[] currentOuterJoinValues;
    private final BooleanVector[] currentOuterJoinNulls;
    private final int[] outputOuterPositions = new int[BATCH_SIZE];
    private final long[] outputInnerRows = new long[BATCH_SIZE];
    private final int[] innerPositionsScratch = new int[BATCH_SIZE];
    private final int[] retainedInnerPositionsScratch = new int[BATCH_SIZE];
    private final int[] retainedInnerMaskPositionsScratch = new int[BATCH_SIZE];
    private final Streams[] currentOutputs;
    private JoinIndex joinIndex;

    private Mask currentOuterMask;
    private Batch currentOuterBatch;
    private int currentOuterMaskIndex;
    private int outerRemaining;
    private int currentOuterPosition;
    private boolean currentOuterPositionReady;
    private LongList currentMatches = LongLists.emptyList();
    private int currentMatchIndex;
    private int currentOutputCount;
    private Mask currentOutputMask;
    private boolean outerConstrained;

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
        this.outputBuffer = new JoinOutputBuffer(buffers, BATCH_SIZE, outer.outputCount(), inner.outputCount());
        this.currentOuterJoinValues = new Vector[outerJoinColumns.length];
        this.currentOuterJoinNulls = new BooleanVector[outerJoinColumns.length];
        this.currentOutputs = new Streams[outputCount()];
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
        currentOutputMask = batchMask;
        outerConstrained = false;
        java.util.Arrays.fill(currentOutputs, null);
        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            outputs[outputIndex] = resultOutput(outputIndex);
        }
        return new Batch(
                batchMask,
                takenMask -> allocator.transfer(ALLOCATION_CONTEXT, takenMask),
                outputs);
    }

    private Mask produceBatch()
    {
        loadInnerIfNecessary();
        if (joinIndex == null || joinIndex.isEmpty()) {
            captureOuterSchemaIfAvailable();
            done = true;
            currentOutputCount = 0;
            return allocator.allocateAllMask(ALLOCATION_CONTEXT, 0);
        }

        int outputPosition = 0;
        Batch outputOuterBatch = currentOuterBatch;
        while (outputPosition < BATCH_SIZE) {
            if (outerRemaining == 0) {
                if (outputPosition > 0) {
                    break;
                }
                if (!loadNextOuterBatch()) {
                    done = true;
                    break;
                }
                outputOuterBatch = currentOuterBatch;
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
                outputOuterPositions[outputPosition] = currentOuterPosition;
                outputInnerRows[outputPosition] = currentMatches.getLong(currentMatchIndex++);
                outputPosition++;
            }

            if (currentMatchIndex == currentMatches.size()) {
                outerRemaining--;
                currentOuterPositionReady = false;
                currentMatches = LongLists.emptyList();
            }
        }

        if (outputPosition == 0) {
            currentOutputCount = 0;
            return allocator.allocateAllMask(ALLOCATION_CONTEXT, 0);
        }
        currentOutputCount = outputPosition;
        return allocator.allocateRangeMask(ALLOCATION_CONTEXT, 0, outputPosition);
    }

    private boolean loadNextOuterBatch()
    {
        while (outer.hasNext()) {
            currentOuterBatch = outer.next();
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
        if (joinIndex == null) {
            return LongLists.emptyList();
        }
        return joinIndex.matches(currentOuterJoinValues, currentOuterJoinNulls, currentOuterPosition);
    }

    private void loadInnerIfNecessary()
    {
        int batchCountBefore = bufferedInner.batches().size();
        bufferedInner.loadAll(inner, BATCH_SIZE, innerJoinColumns, inner.supportsRetainedBatches());
        outputBuffer.captureInnerSchema(bufferedInner.schema());
        for (int batchIndex = batchCountBefore; batchIndex < bufferedInner.batches().size(); batchIndex++) {
            BufferedJoinInput.InnerBatch batch = bufferedInner.batches().get(batchIndex);
            indexInnerRows(batch, 0, batch.length(), batchIndex);
        }
    }

    private void indexInnerRows(BufferedJoinInput.InnerBatch batch, int startPosition, int length, int batchIndex)
    {
        Vector[] joinValues = new Vector[innerJoinColumns.length];
        BooleanVector[] joinNulls = new BooleanVector[innerJoinColumns.length];
        for (int keyIndex = 0; keyIndex < innerJoinColumns.length; keyIndex++) {
            if (batch.retained()) {
                Output output = batch.retainedBatch().output(innerJoinColumns[keyIndex]);
                joinValues[keyIndex] = output.borrow(Stream.VALUES);
                joinNulls[keyIndex] = (BooleanVector) output.borrowOrNull(Stream.NULLS);
            }
            else {
                Streams streams = batch.columns()[innerJoinColumns[keyIndex]];
                joinValues[keyIndex] = streams.values();
                joinNulls[keyIndex] = (BooleanVector) streams.getOrNull(Stream.NULLS);
            }
        }
        if (joinIndex == null) {
            joinIndex = createJoinIndex(joinValues);
        }

        for (int position = startPosition; position < startPosition + length; position++) {
            int sourcePosition = batch.sourcePosition(position);
            joinIndex.add(joinValues, joinNulls, sourcePosition, packRowReference(batchIndex, position));
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

    private void captureOuterSchemaIfAvailable()
    {
        while (outer.hasNext()) {
            outputBuffer.captureOuterSchema(outer.next());
        }
    }

    private JoinIndex createJoinIndex(Vector[] joinValues)
    {
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(joinValues);
        if (layout != null) {
            return new FlatJoinIndex(layout);
        }
        return new ObjectJoinIndex(joinValues.length);
    }

    @Override
    public void constrain(Mask mask)
    {
        currentOutputMask = mask;
    }

    @Override
    public void close()
    {
        outer.close();
        inner.close();
        allocator.release(ALLOCATION_CONTEXT);
    }

    private Output resultOutput(int outputIndex)
    {
        if (currentOutputCount == 0) {
            Streams schema = outputSchema(outputIndex);
            if (schema == null) {
                return new Output(Set.of(), stream -> {
                    throw new IllegalArgumentException("Output does not expose stream: " + stream);
                });
            }
            Streams empty = buffers.emptyLike(schema);
            return new Output(empty.asMap().keySet(), empty::get, (stream, vector) -> allocator.transfer(ALLOCATION_CONTEXT, vector));
        }

        Set<Stream> streams = outputIndex < outer.outputCount()
                ? currentOuterBatch.output(outputIndex).streams()
                : bufferedInner.outputStreams(outputIndex - outer.outputCount());
        return new Output(
                streams,
                stream -> materializeOutput(outputIndex).get(stream),
                (stream, vector) -> allocator.transfer(ALLOCATION_CONTEXT, vector));
    }

    private Streams outputSchema(int outputIndex)
    {
        if (outputIndex < outer.outputCount()) {
            Streams schema = outputBuffer.outerSchema()[outputIndex];
            if (schema != null) {
                return schema;
            }
            if (currentOuterBatch != null) {
                Streams.Builder streams = Streams.builder();
                Output output = currentOuterBatch.output(outputIndex);
                for (Stream stream : output.streams()) {
                    streams.put(stream, output.borrow(stream));
                }
                return streams.build();
            }
            return null;
        }
        Streams schema = outputBuffer.innerSchema()[outputIndex - outer.outputCount()];
        if (schema != null) {
            return schema;
        }
        return bufferedInner.outputSchema(outputIndex - outer.outputCount());
    }

    private Streams materializeOutput(int outputIndex)
    {
        Streams existing = currentOutputs[outputIndex];
        if (existing != null) {
            return existing;
        }

        Streams materialized = outputIndex < outer.outputCount()
                ? materializeOuterOutput(outputIndex)
                : materializeInnerOutput(outputIndex - outer.outputCount());
        currentOutputs[outputIndex] = materialized;
        return materialized;
    }

    private Streams materializeOuterOutput(int outputIndex)
    {
        constrainOuterIfNecessary();
        Output sourceOutput = currentOuterBatch.output(outputIndex);
        if (currentOutputMask.all()) {
            return buffers.copyPositions(sourceOutput, null, outputOuterPositions, currentOutputCount, 0, currentOutputCount);
        }

        Streams result = null;
        for (int index = 0; index < currentOutputMask.count(); index++) {
            int outputPosition = currentOutputMask.position(index);
            result = buffers.copySinglePosition(sourceOutput, result, currentOutputCount, outputPosition, outputOuterPositions[outputPosition]);
        }
        return result == null ? buffers.emptyLike(outputSchema(outputIndex)) : result;
    }

    private Streams materializeInnerOutput(int innerOutputIndex)
    {
        if (currentOutputMask.all()) {
            Streams result = null;
            int outputStart = 0;
            int next = 0;
            while (next < currentOutputCount) {
                int batchIndex = batchIndex(outputInnerRows[next]);
                int runLength = 0;
                while (next + runLength < currentOutputCount && batchIndex(outputInnerRows[next + runLength]) == batchIndex) {
                    innerPositionsScratch[runLength] = rowPosition(outputInnerRows[next + runLength]);
                    runLength++;
                }
                BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(batchIndex);
                result = copyInnerPositions(result, innerBatch, innerOutputIndex, innerPositionsScratch, runLength, outputStart, currentOutputCount);
                outputStart += runLength;
                next += runLength;
            }
            return result;
        }

        Streams result = null;
        for (int index = 0; index < currentOutputMask.count(); index++) {
            int outputPosition = currentOutputMask.position(index);
            long rowReference = outputInnerRows[outputPosition];
            BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(batchIndex(rowReference));
            result = copyInnerSinglePosition(result, innerBatch, innerOutputIndex, currentOutputCount, outputPosition, rowPosition(rowReference));
        }
        return result == null ? buffers.emptyLike(outputSchema(innerOutputIndex + outer.outputCount())) : result;
    }

    private Streams copyInnerPositions(Streams existing, BufferedJoinInput.InnerBatch innerBatch, int innerOutputIndex, int[] positions, int positionCount, int outputStart, int size)
    {
        if (!innerBatch.retained()) {
            return buffers.copyPositions(existing, innerBatch.columns()[innerOutputIndex], positions, positionCount, outputStart, size);
        }

        constrainRetainedInnerBatch(innerBatch, positions, positionCount);
        Output output = innerBatch.retainedBatch().output(innerOutputIndex);
        for (int index = 0; index < positionCount; index++) {
            retainedInnerPositionsScratch[index] = innerBatch.sourcePosition(positions[index]);
        }
        return buffers.copyPositions(output, existing, retainedInnerPositionsScratch, positionCount, outputStart, size);
    }

    private Streams copyInnerSinglePosition(Streams existing, BufferedJoinInput.InnerBatch innerBatch, int innerOutputIndex, int size, int outputPosition, int logicalPosition)
    {
        if (!innerBatch.retained()) {
            return buffers.copySinglePosition(existing, innerBatch.columns()[innerOutputIndex], size, outputPosition, logicalPosition);
        }

        int sourcePosition = innerBatch.sourcePosition(logicalPosition);
        constrainRetainedInnerBatch(innerBatch, new int[] {logicalPosition}, 1);
        return buffers.copySinglePosition(innerBatch.retainedBatch().output(innerOutputIndex), existing, size, outputPosition, sourcePosition);
    }

    private void constrainRetainedInnerBatch(BufferedJoinInput.InnerBatch innerBatch, int[] logicalPositions, int positionCount)
    {
        if (!innerBatch.retained()) {
            return;
        }
        for (int index = 0; index < positionCount; index++) {
            retainedInnerMaskPositionsScratch[index] = innerBatch.sourcePosition(logicalPositions[index]);
        }
        Arrays.sort(retainedInnerMaskPositionsScratch, 0, positionCount);
        int uniqueCount = 0;
        int previous = -1;
        for (int index = 0; index < positionCount; index++) {
            int position = retainedInnerMaskPositionsScratch[index];
            if (position != previous) {
                retainedInnerMaskPositionsScratch[uniqueCount++] = position;
                previous = position;
            }
        }
        innerBatch.retainedBatch().constrain(allocator.allocateSparseMask(
                ALLOCATION_CONTEXT,
                Arrays.copyOf(retainedInnerMaskPositionsScratch, uniqueCount),
                innerBatch.retainedBatch().borrowMask().size()));
    }

    private void constrainOuterIfNecessary()
    {
        if (outerConstrained || currentOuterBatch == null) {
            return;
        }
        outerConstrained = true;
        outer.constrain(matchedOuterMask());
    }

    private Mask matchedOuterMask()
    {
        if (currentOutputMask.none()) {
            return allocator.allocateSparseMask(ALLOCATION_CONTEXT, new int[0], currentOuterMask.size());
        }

        int[] positions = new int[Math.min(currentOutputMask.count(), currentOuterMask.count())];
        int selectedCount = 0;
        int previous = -1;
        for (int index = 0; index < currentOutputMask.count(); index++) {
            int outputPosition = currentOutputMask.position(index);
            int outerPosition = outputOuterPositions[outputPosition];
            if (outerPosition != previous) {
                positions[selectedCount++] = outerPosition;
                previous = outerPosition;
            }
        }
        return allocator.allocateSparseMask(ALLOCATION_CONTEXT, java.util.Arrays.copyOf(positions, selectedCount), currentOuterMask.size());
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

    private interface JoinIndex
    {
        boolean isEmpty();

        void add(Vector[] values, BooleanVector[] nulls, int position, long rowReference);

        LongList matches(Vector[] values, BooleanVector[] nulls, int position);
    }

    private static final class FlatJoinIndex
            implements JoinIndex
    {
        private final FlatGroupingTable table;
        private LongArrayList[] rowsByGroup = new LongArrayList[16];
        private long nextGroupId;

        private FlatJoinIndex(FlatKeyLayout layout)
        {
            this.table = new FlatGroupingTable(layout, 1024);
        }

        @Override
        public boolean isEmpty()
        {
            return nextGroupId == 0;
        }

        @Override
        public void add(Vector[] values, BooleanVector[] nulls, int position, long rowReference)
        {
            if (hasNull(nulls, position)) {
                return;
            }
            long newGroupId = nextGroupId;
            long groupId = table.assignGroup(values, position, newGroupId);
            if (groupId == newGroupId) {
                ensureGroupCapacity((int) groupId);
                nextGroupId++;
            }
            rowsByGroup[(int) groupId].add(rowReference);
        }

        @Override
        public LongList matches(Vector[] values, BooleanVector[] nulls, int position)
        {
            if (hasNull(nulls, position)) {
                return LongLists.emptyList();
            }
            long groupId = table.findGroup(values, position);
            if (groupId < 0 || groupId >= nextGroupId) {
                return LongLists.emptyList();
            }
            LongArrayList rows = rowsByGroup[(int) groupId];
            return rows == null ? LongLists.emptyList() : rows;
        }

        private void ensureGroupCapacity(int groupId)
        {
            if (groupId >= rowsByGroup.length) {
                rowsByGroup = Arrays.copyOf(rowsByGroup, Math.max(groupId + 1, rowsByGroup.length * 2));
            }
            if (rowsByGroup[groupId] == null) {
                rowsByGroup[groupId] = new LongArrayList();
            }
        }

        private static boolean hasNull(BooleanVector[] nulls, int position)
        {
            for (BooleanVector nullsVector : nulls) {
                if (OperatorVectorSupport.isNull(nullsVector, position)) {
                    return true;
                }
            }
            return false;
        }
    }

    private static final class ObjectJoinIndex
            implements JoinIndex
    {
        private final Map<OperatorKeySemantics.Key, LongArrayList> rowsByKey = new HashMap<>();
        private final OperatorKeySemantics.Key[] innerProbeKeys;
        private final OperatorKeySemantics.Key[] outerProbeKeys;
        private final OperatorKeySemantics.CompositeProbeKey innerCompositeProbeKey;
        private final OperatorKeySemantics.CompositeProbeKey outerCompositeProbeKey;

        private ObjectJoinIndex(int keyCount)
        {
            this.innerProbeKeys = new OperatorKeySemantics.Key[keyCount];
            this.outerProbeKeys = new OperatorKeySemantics.Key[keyCount];
            this.innerCompositeProbeKey = keyCount > 1 ? OperatorKeySemantics.reusableCompositeProbeKey(keyCount) : null;
            this.outerCompositeProbeKey = keyCount > 1 ? OperatorKeySemantics.reusableCompositeProbeKey(keyCount) : null;
        }

        @Override
        public boolean isEmpty()
        {
            return rowsByKey.isEmpty();
        }

        @Override
        public void add(Vector[] values, BooleanVector[] nulls, int position, long rowReference)
        {
            OperatorKeySemantics.Key key = keyForPosition(values, nulls, position, innerProbeKeys, innerCompositeProbeKey);
            if (key == null) {
                return;
            }
            LongArrayList rows = rowsByKey.get(key);
            if (rows == null) {
                rows = new LongArrayList();
                rowsByKey.put(OperatorKeySemantics.ownedKey(key), rows);
            }
            rows.add(rowReference);
        }

        @Override
        public LongList matches(Vector[] values, BooleanVector[] nulls, int position)
        {
            OperatorKeySemantics.Key key = keyForPosition(values, nulls, position, outerProbeKeys, outerCompositeProbeKey);
            if (key == null) {
                return LongLists.emptyList();
            }
            LongArrayList rows = rowsByKey.get(key);
            return rows == null ? LongLists.emptyList() : rows;
        }

        private static OperatorKeySemantics.Key keyForPosition(Vector[] values, BooleanVector[] nulls, int position, OperatorKeySemantics.Key[] reusableProbeKeys, OperatorKeySemantics.CompositeProbeKey reusableCompositeProbeKey)
        {
            for (int keyIndex = 0; keyIndex < values.length; keyIndex++) {
                if (reusableProbeKeys[keyIndex] == null) {
                    reusableProbeKeys[keyIndex] = OperatorKeySemantics.reusableProbeKey(values[keyIndex]);
                }
                OperatorKeySemantics.Key key = OperatorKeySemantics.probeKey(values[keyIndex], nulls[keyIndex], position, reusableProbeKeys[keyIndex]);
                if (key == null) {
                    return null;
                }
                reusableProbeKeys[keyIndex] = key;
            }
            return OperatorKeySemantics.probeCompositeKey(reusableProbeKeys, reusableCompositeProbeKey);
        }
    }
}
