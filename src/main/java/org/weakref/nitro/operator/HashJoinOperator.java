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

import it.unimi.dsi.fastutil.longs.AbstractLongList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongLists;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class HashJoinOperator
        implements Operator
{
    private static final int BATCH_SIZE = Integer.getInteger("nitro.hash.join.maxBatchRows", 10_000);
    private static final long NO_MATCH_ROW_REFERENCE = -1L;
    private static final BooleanVector[] NO_NULL_STREAMS = new BooleanVector[0];

    private final Allocator allocator;
    private final Allocator.Context allocationContext = new Allocator.Context("HashJoinOperator");
    private final Operator outer;
    private final Operator inner;
    private final boolean probeOuterJoin;
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
    private boolean currentOuterJoinHasNulls;
    private int currentMatchIndex;
    private int currentOutputCount;
    private Mask currentOutputMask;

    private boolean done;

    public HashJoinOperator(Allocator allocator, Operator outer, int outerJoinColumn, Operator inner, int innerJoinColumn)
    {
        this(allocator, outer, new int[] {outerJoinColumn}, inner, new int[] {innerJoinColumn}, false);
    }

    public HashJoinOperator(Allocator allocator, Operator outer, int outerJoinColumn, Operator inner, int innerJoinColumn, boolean probeOuterJoin)
    {
        this(allocator, outer, new int[] {outerJoinColumn}, inner, new int[] {innerJoinColumn}, probeOuterJoin);
    }

    public HashJoinOperator(Allocator allocator, Operator outer, int[] outerJoinColumns, Operator inner, int[] innerJoinColumns)
    {
        this(allocator, outer, outerJoinColumns, inner, innerJoinColumns, false);
    }

    public HashJoinOperator(Allocator allocator, Operator outer, int[] outerJoinColumns, Operator inner, int[] innerJoinColumns, boolean probeOuterJoin)
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
        this.probeOuterJoin = probeOuterJoin;
        this.outerJoinColumns = outerJoinColumns.clone();
        this.innerJoinColumns = innerJoinColumns.clone();
        this.buffers = new JoinBufferSupport(allocator, allocationContext);
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
        java.util.Arrays.fill(currentOutputs, null);
        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            outputs[outputIndex] = resultOutput(outputIndex);
        }
        return new Batch(
                batchMask,
                takenMask -> allocator.transfer(allocationContext, takenMask),
                outputs);
    }

    private Mask produceBatch()
    {
        loadInnerIfNecessary();
        if ((joinIndex == null || joinIndex.isEmpty()) && !probeOuterJoin) {
            captureOuterSchemaIfAvailable();
            done = true;
            currentOutputCount = 0;
            return allocator.allocateAllMask(allocationContext, 0);
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

            if (currentMatches.isEmpty()) {
                if (probeOuterJoin) {
                    outputOuterPositions[outputPosition] = currentOuterPosition;
                    outputInnerRows[outputPosition] = NO_MATCH_ROW_REFERENCE;
                    outputPosition++;
                }
                outerRemaining--;
                currentOuterPositionReady = false;
                currentMatches = LongLists.emptyList();
                continue;
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
            return allocator.allocateAllMask(allocationContext, 0);
        }
        currentOutputCount = outputPosition;
        return allocator.allocateRangeMask(allocationContext, 0, outputPosition);
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
        if (joinIndex == null) {
            return LongLists.emptyList();
        }
        if (!currentOuterJoinHasNulls) {
            return joinIndex.matchesNoNulls(currentOuterJoinValues, currentOuterPosition);
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
        boolean hasNulls = false;
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
            hasNulls = hasNulls || joinNulls[keyIndex] != null;
        }
        if (joinIndex == null) {
            joinIndex = createJoinIndex(joinValues);
        }

        for (int position = startPosition; position < startPosition + length; position++) {
            int sourcePosition = batch.sourcePosition(position);
            if (hasNulls) {
                joinIndex.add(joinValues, joinNulls, sourcePosition, packRowReference(batchIndex, position));
            }
            else {
                joinIndex.addNoNulls(joinValues, sourcePosition, packRowReference(batchIndex, position));
            }
        }
    }

    private void cacheOuterJoinInputs()
    {
        currentOuterJoinHasNulls = false;
        for (int keyIndex = 0; keyIndex < outerJoinColumns.length; keyIndex++) {
            Output output = currentOuterBatch.output(outerJoinColumns[keyIndex]);
            currentOuterJoinValues[keyIndex] = output.borrow(Stream.VALUES);
            currentOuterJoinNulls[keyIndex] = (BooleanVector) output.borrowOrNull(Stream.NULLS);
            currentOuterJoinHasNulls = currentOuterJoinHasNulls || currentOuterJoinNulls[keyIndex] != null;
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
        int expectedSize = expectedInnerRowCount();
        if (joinValues.length == 1 && isSingleLongJoinCandidate(joinValues[0])) {
            return new LongJoinIndex(expectedSize);
        }
        if (joinValues.length == 2 && isSingleLongJoinCandidate(joinValues[0]) && isSingleLongJoinCandidate(joinValues[1])) {
            return new LongPairJoinIndex(expectedSize);
        }
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
        allocator.release(allocationContext);
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
            return new Output(empty.streams(), empty::get, (stream, vector) -> allocator.transfer(allocationContext, vector));
        }

        Set<Stream> streams = outputIndex < outer.outputCount()
                ? currentOuterBatch.output(outputIndex).streams()
                : innerOutputStreams(outputIndex - outer.outputCount());
        return new Output(
                streams,
                stream -> materializeOutput(outputIndex).get(stream),
                (stream, vector) -> allocator.transfer(allocationContext, vector));
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
                if (output.hasValues()) {
                    streams.put(Stream.VALUES, output.borrow(Stream.VALUES));
                }
                if (output.hasNulls()) {
                    streams.put(Stream.NULLS, output.borrow(Stream.NULLS));
                }
                if (output.hasErrors()) {
                    streams.put(Stream.ERRORS, output.borrow(Stream.ERRORS));
                }
                return streams.build();
            }
            return null;
        }
        Streams schema = outputBuffer.innerSchema()[outputIndex - outer.outputCount()];
        if (schema != null) {
            return probeOuterJoin ? ensureNullStream(schema) : schema;
        }
        Streams bufferedSchema = bufferedInner.outputSchema(outputIndex - outer.outputCount());
        return probeOuterJoin && bufferedSchema != null ? ensureNullStream(bufferedSchema) : bufferedSchema;
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
        if (currentOutputMask.all() && !hasNoMatchRows()) {
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
        boolean exposeNulls = probeOuterJoin || innerOutputStreams(innerOutputIndex).contains(Stream.NULLS);
        for (int index = 0; index < currentOutputMask.count(); index++) {
            int outputPosition = currentOutputMask.position(index);
            long rowReference = outputInnerRows[outputPosition];
            if (rowReference == NO_MATCH_ROW_REFERENCE) {
                result = copyNullInnerPosition(result, innerOutputIndex, currentOutputCount, outputPosition);
                continue;
            }
            BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(batchIndex(rowReference));
            result = copyInnerSinglePosition(result, innerBatch, innerOutputIndex, currentOutputCount, outputPosition, rowPosition(rowReference), exposeNulls);
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

    private Streams copyInnerSinglePosition(Streams existing, BufferedJoinInput.InnerBatch innerBatch, int innerOutputIndex, int size, int outputPosition, int logicalPosition, boolean exposeNulls)
    {
        if (!innerBatch.retained()) {
            return withSyntheticNulls(existing, buffers.copySinglePosition(existing, innerBatch.columns()[innerOutputIndex], size, outputPosition, logicalPosition), size, outputPosition, exposeNulls);
        }

        int sourcePosition = innerBatch.sourcePosition(logicalPosition);
        constrainRetainedInnerBatch(innerBatch, logicalPosition);
        return withSyntheticNulls(existing, buffers.copySinglePosition(innerBatch.retainedBatch().output(innerOutputIndex), existing, size, outputPosition, sourcePosition), size, outputPosition, exposeNulls);
    }

    private Streams copyNullInnerPosition(Streams existing, int innerOutputIndex, int size, int outputPosition)
    {
        Streams schema = outputSchema(innerOutputIndex + outer.outputCount());
        if (schema == null) {
            throw new IllegalStateException("Unable to determine inner output schema for left join");
        }

        Streams.Builder builder = Streams.builder();
        builder.put(Stream.VALUES, existing == null ? nullValuesLike(schema.values(), size) : existing.values());
        builder.put(Stream.NULLS, setBooleanPosition(existing == null ? null : (BooleanVector) existing.getOrNull(Stream.NULLS), size, outputPosition, true));
        if (schema.has(Stream.ERRORS)) {
            builder.put(Stream.ERRORS, setBooleanPosition(existing == null ? null : (BooleanVector) existing.getOrNull(Stream.ERRORS), size, outputPosition, false));
        }
        return builder.build();
    }

    private void constrainRetainedInnerBatch(BufferedJoinInput.InnerBatch innerBatch, int[] logicalPositions, int positionCount)
    {
        if (!innerBatch.retained()) {
            return;
        }
        boolean sorted = true;
        int previousSourcePosition = -1;
        for (int index = 0; index < positionCount; index++) {
            int sourcePosition = innerBatch.sourcePosition(logicalPositions[index]);
            retainedInnerMaskPositionsScratch[index] = sourcePosition;
            sorted &= sourcePosition >= previousSourcePosition;
            previousSourcePosition = sourcePosition;
        }
        if (!sorted) {
            Arrays.sort(retainedInnerMaskPositionsScratch, 0, positionCount);
        }
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
                allocationContext,
                retainedInnerMaskPositionsScratch,
                uniqueCount,
                innerBatch.retainedBatch().borrowMask().size()));
    }

    private void constrainRetainedInnerBatch(BufferedJoinInput.InnerBatch innerBatch, int logicalPosition)
    {
        if (!innerBatch.retained()) {
            return;
        }
        retainedInnerMaskPositionsScratch[0] = innerBatch.sourcePosition(logicalPosition);
        innerBatch.retainedBatch().constrain(allocator.allocateSparseMask(
                allocationContext,
                retainedInnerMaskPositionsScratch,
                1,
                innerBatch.retainedBatch().borrowMask().size()));
    }

    private static long packRowReference(int batchIndex, int position)
    {
        return ((long) batchIndex << Integer.SIZE) | (position & 0xFFFF_FFFFL);
    }

    private int expectedInnerRowCount()
    {
        long rowCount = bufferedInner.rowCount();
        if (rowCount <= 0) {
            return 16;
        }
        return (int) Math.max(16L, Math.min(Integer.MAX_VALUE, rowCount));
    }

    private static int batchIndex(long rowReference)
    {
        return (int) (rowReference >>> Integer.SIZE);
    }

    private static int rowPosition(long rowReference)
    {
        return (int) rowReference;
    }

    private boolean hasNoMatchRows()
    {
        for (int index = 0; index < currentOutputCount; index++) {
            if (outputInnerRows[index] == NO_MATCH_ROW_REFERENCE) {
                return true;
            }
        }
        return false;
    }

    private Set<Stream> innerOutputStreams(int innerOutputIndex)
    {
        Set<Stream> streams = bufferedInner.outputStreams(innerOutputIndex);
        if (!probeOuterJoin || streams == null || streams.contains(Stream.NULLS)) {
            return streams;
        }
        EnumSet<Stream> adjusted = EnumSet.copyOf(streams);
        adjusted.add(Stream.NULLS);
        return Set.copyOf(adjusted);
    }

    private Streams withSyntheticNulls(Streams existing, Streams streams, int size, int outputPosition, boolean exposeNulls)
    {
        if (!exposeNulls || streams.has(Stream.NULLS)) {
            return streams;
        }
        return streams.with(Stream.NULLS, setBooleanPosition(existing == null ? null : (BooleanVector) existing.getOrNull(Stream.NULLS), size, outputPosition, false));
    }

    private BooleanVector setBooleanPosition(BooleanVector existing, int size, int outputPosition, boolean value)
    {
        BooleanVector vector = allocator.allocateOrGrow(allocationContext, existing, BooleanVector.class, size, BooleanVector::new);
        vector.values()[outputPosition] = value;
        return vector;
    }

    private Streams ensureNullStream(Streams schema)
    {
        if (schema.has(Stream.NULLS)) {
            return schema;
        }
        return schema.with(Stream.NULLS, new BooleanVector(0));
    }

    private Vector nullValuesLike(Vector sample, int size)
    {
        return switch (sample) {
            case org.weakref.nitro.data.I64Vector _ -> allocator.allocate(allocationContext, org.weakref.nitro.data.I64Vector.class, size, org.weakref.nitro.data.I64Vector::new);
            case org.weakref.nitro.data.I32Vector _ -> allocator.allocate(allocationContext, org.weakref.nitro.data.I32Vector.class, size, org.weakref.nitro.data.I32Vector::new);
            case org.weakref.nitro.data.F64Vector _ -> allocator.allocate(allocationContext, org.weakref.nitro.data.F64Vector.class, size, org.weakref.nitro.data.F64Vector::new);
            case BooleanVector _ -> allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new);
            case org.weakref.nitro.data.BinaryVector binary -> {
                org.weakref.nitro.data.BinaryVector values = org.weakref.nitro.data.BinaryVector.allocate(allocator, allocationContext, size, 0);
                values.addTraits(binary.traits());
                yield values;
            }
            case org.weakref.nitro.data.DictionaryVector dictionary -> nullValuesLike(dictionary.values(), size);
            case org.weakref.nitro.data.RleVector rle -> nullValuesLike(rle.values(), size);
            case org.weakref.nitro.data.ArrayVector array -> {
                org.weakref.nitro.data.ArrayVector values = allocator.allocateArray(allocationContext, size);
                values.setElements(buffers.emptyLike(array.elements()));
                yield values;
            }
            case org.weakref.nitro.data.MapVector map -> {
                org.weakref.nitro.data.MapVector values = allocator.allocateMap(allocationContext, size);
                values.setEntries(buffers.emptyLike(map.keys()), buffers.emptyLike(map.values()));
                yield values;
            }
            case org.weakref.nitro.data.StructVector struct -> {
                org.weakref.nitro.data.StructVector values = allocator.allocate(allocationContext, org.weakref.nitro.data.StructVector.class, size, org.weakref.nitro.data.StructVector::new);
                for (Map.Entry<String, Streams> field : struct.fields().entrySet()) {
                    values.setField(field.getKey(), buffers.emptyLike(field.getValue()));
                }
                yield values;
            }
            default -> throw new IllegalArgumentException("Unsupported null materialization type: " + sample.getClass().getSimpleName());
        };
    }

    private static boolean isSingleLongJoinCandidate(Vector values)
    {
        FlatTypeHandler handler = FlatTypeHandlers.forVector(values);
        return handler != null && handler.kind() == FlatTypeHandler.Kind.LONG;
    }

    private interface JoinIndex
    {
        boolean isEmpty();

        void add(Vector[] values, BooleanVector[] nulls, int position, long rowReference);

        LongList matches(Vector[] values, BooleanVector[] nulls, int position);

        default void addNoNulls(Vector[] values, int position, long rowReference)
        {
            add(values, NO_NULL_STREAMS, position, rowReference);
        }

        default LongList matchesNoNulls(Vector[] values, int position)
        {
            return matches(values, NO_NULL_STREAMS, position);
        }
    }

    private static final class LongJoinIndex
            implements JoinIndex
    {
        private static final float LOAD_FACTOR = 0.75f;

        private long[] keys;
        private long[] singleRows;
        private LongArrayList[] rowsBySlot;
        private int mask;
        private int maxFill;
        private int size;
        private final SingleLongList singleMatch = new SingleLongList();

        private LongJoinIndex(int expectedSize)
        {
            int capacity = 16;
            while (capacity < expectedSize / LOAD_FACTOR) {
                capacity <<= 1;
            }
            keys = new long[capacity];
            singleRows = emptyRows(capacity);
            rowsBySlot = new LongArrayList[capacity];
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
        }

        @Override
        public boolean isEmpty()
        {
            return size == 0;
        }

        @Override
        public void add(Vector[] values, BooleanVector[] nulls, int position, long rowReference)
        {
            if (FlatJoinIndex.hasNull(nulls, position)) {
                return;
            }
            addNoNulls(values, position, rowReference);
        }

        @Override
        public void addNoNulls(Vector[] values, int position, long rowReference)
        {
            addRow(OperatorVectorSupport.longValue(values[0], position), rowReference);
        }

        @Override
        public LongList matches(Vector[] values, BooleanVector[] nulls, int position)
        {
            if (FlatJoinIndex.hasNull(nulls, position)) {
                return LongLists.emptyList();
            }
            return matchesNoNulls(values, position);
        }

        @Override
        public LongList matchesNoNulls(Vector[] values, int position)
        {
            return rowsForSlot(findSlot(OperatorVectorSupport.longValue(values[0], position)));
        }

        private int findSlot(long key)
        {
            int index = mix(key) & mask;
            while (true) {
                if (isEmptySlot(index) || keys[index] == key) {
                    return index;
                }
                index = (index + 1) & mask;
            }
        }

        private void rehash()
        {
            long[] previousKeys = keys;
            long[] previousSingleRows = singleRows;
            LongArrayList[] previousRowsBySlot = rowsBySlot;
            int capacity = previousRowsBySlot.length * 2;

            keys = new long[capacity];
            singleRows = emptyRows(capacity);
            rowsBySlot = new LongArrayList[capacity];
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
            size = 0;

            for (int index = 0; index < previousKeys.length; index++) {
                if (previousSingleRows[index] == NO_MATCH_ROW_REFERENCE) {
                    continue;
                }
                int newIndex = findSlot(previousKeys[index]);
                keys[newIndex] = previousKeys[index];
                singleRows[newIndex] = previousSingleRows[index];
                rowsBySlot[newIndex] = previousRowsBySlot[index];
                size++;
            }
        }

        private void addRow(long key, long rowReference)
        {
            int index = findSlot(key);
            if (isEmptySlot(index)) {
                keys[index] = key;
                singleRows[index] = rowReference;
                size++;
                if (size >= maxFill) {
                    rehash();
                }
                return;
            }
            if (rowsBySlot[index] == null) {
                LongArrayList rows = new LongArrayList(2);
                rows.add(singleRows[index]);
                rows.add(rowReference);
                rowsBySlot[index] = rows;
                return;
            }
            rowsBySlot[index].add(rowReference);
        }

        private LongList rowsForSlot(int index)
        {
            if (isEmptySlot(index)) {
                return LongLists.emptyList();
            }
            LongArrayList rows = rowsBySlot[index];
            if (rows != null) {
                return rows;
            }
            return singleMatch.withValue(singleRows[index]);
        }

        private boolean isEmptySlot(int index)
        {
            return singleRows[index] == NO_MATCH_ROW_REFERENCE;
        }

        private static int mix(long key)
        {
            long hash = key ^ (key >>> 33);
            hash *= 0xFF51AFD7ED558CCDL;
            hash ^= (hash >>> 33);
            hash *= 0xC4CEB9FE1A85EC53L;
            hash ^= (hash >>> 33);
            return (int) hash;
        }
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

    private static final class LongPairJoinIndex
            implements JoinIndex
    {
        private static final float LOAD_FACTOR = 0.75f;

        private long[] firstKeys;
        private long[] secondKeys;
        private long[] singleRows;
        private LongArrayList[] rowsBySlot;
        private int mask;
        private int maxFill;
        private int size;
        private final SingleLongList singleMatch = new SingleLongList();

        private LongPairJoinIndex(int expectedSize)
        {
            int capacity = 16;
            while (capacity < expectedSize / LOAD_FACTOR) {
                capacity <<= 1;
            }
            firstKeys = new long[capacity];
            secondKeys = new long[capacity];
            singleRows = emptyRows(capacity);
            rowsBySlot = new LongArrayList[capacity];
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
        }

        @Override
        public boolean isEmpty()
        {
            return size == 0;
        }

        @Override
        public void add(Vector[] values, BooleanVector[] nulls, int position, long rowReference)
        {
            if (FlatJoinIndex.hasNull(nulls, position)) {
                return;
            }
            addNoNulls(values, position, rowReference);
        }

        @Override
        public void addNoNulls(Vector[] values, int position, long rowReference)
        {
            addRow(
                    OperatorVectorSupport.longValue(values[0], position),
                    OperatorVectorSupport.longValue(values[1], position),
                    rowReference);
        }

        @Override
        public LongList matches(Vector[] values, BooleanVector[] nulls, int position)
        {
            if (FlatJoinIndex.hasNull(nulls, position)) {
                return LongLists.emptyList();
            }
            return matchesNoNulls(values, position);
        }

        @Override
        public LongList matchesNoNulls(Vector[] values, int position)
        {
            long first = OperatorVectorSupport.longValue(values[0], position);
            long second = OperatorVectorSupport.longValue(values[1], position);
            int slot = findSlot(first, second);
            if (isEmptySlot(slot) || firstKeys[slot] != first || secondKeys[slot] != second) {
                return LongLists.emptyList();
            }
            LongArrayList rows = rowsBySlot[slot];
            if (rows != null) {
                return rows;
            }
            return singleMatch.withValue(singleRows[slot]);
        }

        private int findSlot(long first, long second)
        {
            int slot = mix(first, second) & mask;
            while (!isEmptySlot(slot) && (firstKeys[slot] != first || secondKeys[slot] != second)) {
                slot = (slot + 1) & mask;
            }
            return slot;
        }

        private void rehash()
        {
            long[] previousFirstKeys = firstKeys;
            long[] previousSecondKeys = secondKeys;
            long[] previousSingleRows = singleRows;
            LongArrayList[] previousRowsBySlot = rowsBySlot;
            int capacity = previousRowsBySlot.length * 2;

            firstKeys = new long[capacity];
            secondKeys = new long[capacity];
            singleRows = emptyRows(capacity);
            rowsBySlot = new LongArrayList[capacity];
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
            size = 0;

            for (int index = 0; index < previousFirstKeys.length; index++) {
                if (previousSingleRows[index] == NO_MATCH_ROW_REFERENCE) {
                    continue;
                }
                int slot = findSlot(previousFirstKeys[index], previousSecondKeys[index]);
                firstKeys[slot] = previousFirstKeys[index];
                secondKeys[slot] = previousSecondKeys[index];
                singleRows[slot] = previousSingleRows[index];
                rowsBySlot[slot] = previousRowsBySlot[index];
                size++;
            }
        }

        private void addRow(long first, long second, long rowReference)
        {
            int slot = findSlot(first, second);
            if (isEmptySlot(slot)) {
                firstKeys[slot] = first;
                secondKeys[slot] = second;
                singleRows[slot] = rowReference;
                size++;
                if (size >= maxFill) {
                    rehash();
                }
                return;
            }
            if (rowsBySlot[slot] == null) {
                LongArrayList rows = new LongArrayList(2);
                rows.add(singleRows[slot]);
                rows.add(rowReference);
                rowsBySlot[slot] = rows;
                return;
            }
            rowsBySlot[slot].add(rowReference);
        }

        private boolean isEmptySlot(int slot)
        {
            return singleRows[slot] == NO_MATCH_ROW_REFERENCE;
        }

        private static int mix(long first, long second)
        {
            long hash = 31 * Long.hashCode(first) + Long.hashCode(second);
            hash ^= (hash >>> 16);
            return (int) hash;
        }
    }

    private static long[] emptyRows(int capacity)
    {
        long[] rows = new long[capacity];
        Arrays.fill(rows, NO_MATCH_ROW_REFERENCE);
        return rows;
    }

    private static final class SingleLongList
            extends AbstractLongList
    {
        private long value;

        public SingleLongList withValue(long value)
        {
            this.value = value;
            return this;
        }

        @Override
        public long getLong(int index)
        {
            if (index != 0) {
                throw new IndexOutOfBoundsException("index " + index);
            }
            return value;
        }

        @Override
        public int size()
        {
            return 1;
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
