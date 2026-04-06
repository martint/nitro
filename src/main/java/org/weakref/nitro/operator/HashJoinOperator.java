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
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class HashJoinOperator
        implements Operator
{
    public interface MaterializationProfile
    {
        void record(String operatorName, int outputIndex, Streams streams, int rowCount, long nanos);
    }

    private static final int BATCH_SIZE = Integer.getInteger("nitro.hash.join.maxBatchRows", 10_000);
    private static final long NO_MATCH_ROW_REFERENCE = -1L;
    private static final Vector[] NO_NULL_STREAMS = new Vector[0];
    private static final ThreadLocal<MaterializationProfile> CURRENT_MATERIALIZATION_PROFILE = new ThreadLocal<>();

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
    private final Vector[] currentOuterJoinNulls;
    private final int[] outputOuterPositions = new int[BATCH_SIZE];
    private final long[] outputInnerRows = new long[BATCH_SIZE];
    private final int[] outputInnerBatchIndexes = new int[BATCH_SIZE];
    private final int[] outputInnerLogicalPositions = new int[BATCH_SIZE];
    private final int[] outputInnerSourcePositions = new int[BATCH_SIZE];
    private final int[] outputInnerRunStarts = new int[BATCH_SIZE];
    private final int[] outputInnerRunLengths = new int[BATCH_SIZE];
    private final int[] outputInnerRunBatchIndexes = new int[BATCH_SIZE];
    private final int[] outputInnerRunUniqueStarts = new int[BATCH_SIZE];
    private final int[] outputInnerRunUniqueCounts = new int[BATCH_SIZE];
    private final int[] outputInnerUniqueSourcePositions = new int[BATCH_SIZE];
    private final int[] innerPositionsScratch = new int[BATCH_SIZE];
    private final int[] retainedInnerPositionsScratch = new int[BATCH_SIZE];
    private final int[] retainedInnerMaskPositionsScratch = new int[BATCH_SIZE];
    private final int[] preparedOuterPositions = new int[BATCH_SIZE];
    private final LongList[] preparedOuterMatches = new LongList[BATCH_SIZE];
    private final SingleLongList[] preparedSingleMatches = createSingleLongLists(BATCH_SIZE);
    private final Streams[] currentOutputs;
    private int[] retainedConstraintCountsByBatch = new int[16];
    private int[][] retainedConstraintPositionsByBatch = new int[16][];
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
    private int preparedOuterCount;
    private int preparedOuterIndex;
    private boolean done;
    private int preparedInnerRunCount = -1;
    private String profileName;
    public static <T> T withMaterializationProfile(MaterializationProfile profile, Supplier<T> supplier)
    {
        MaterializationProfile previous = CURRENT_MATERIALIZATION_PROFILE.get();
        CURRENT_MATERIALIZATION_PROFILE.set(profile);
        try {
            return supplier.get();
        }
        finally {
            if (previous == null) {
                CURRENT_MATERIALIZATION_PROFILE.remove();
            }
            else {
                CURRENT_MATERIALIZATION_PROFILE.set(previous);
            }
        }
    }

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
        this.currentOuterJoinNulls = new Vector[outerJoinColumns.length];
        this.currentOutputs = new Streams[outputCount()];
        Arrays.fill(retainedConstraintCountsByBatch, -1);
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
        preparedInnerRunCount = -1;
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
                if (preparedOuterIndex >= preparedOuterCount) {
                    if (currentOuterMaskIndex >= currentOuterMask.count()) {
                        outerRemaining = 0;
                        continue;
                    }
                    prepareOuterProbeChunk();
                }
                if (preparedOuterIndex >= preparedOuterCount) {
                    outerRemaining = 0;
                    continue;
                }
                currentOuterPosition = preparedOuterPositions[preparedOuterIndex];
                currentMatches = preparedOuterMatches[preparedOuterIndex];
                preparedOuterIndex++;
                currentOuterPositionReady = true;
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

    private void prepareOuterProbeChunk()
    {
        preparedOuterCount = Math.min(currentOuterMask.count() - currentOuterMaskIndex, BATCH_SIZE);
        preparedOuterIndex = 0;
        for (int index = 0; index < preparedOuterCount; index++) {
            preparedOuterPositions[index] = currentOuterMask.position(currentOuterMaskIndex++);
        }

        if (joinIndex instanceof LongJoinIndex longJoinIndex && outerJoinColumns.length == 1) {
            longJoinIndex.matchRows(currentOuterJoinValues[0], currentOuterJoinNulls[0], preparedOuterPositions, preparedOuterCount, preparedOuterMatches, preparedSingleMatches);
            return;
        }

        for (int index = 0; index < preparedOuterCount; index++) {
            LongList matches = matchesForOuterPosition(preparedOuterPositions[index]);
            if (matches instanceof SingleLongList singleMatch) {
                preparedOuterMatches[index] = preparedSingleMatches[index].withValue(singleMatch.getLong(0));
            }
            else {
                preparedOuterMatches[index] = matches;
            }
        }
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
                preparedOuterCount = 0;
                preparedOuterIndex = 0;
                return true;
            }
        }
        return false;
    }

    private LongList matchesForOuterPosition()
    {
        return matchesForOuterPosition(currentOuterPosition);
    }

    private LongList matchesForOuterPosition(int outerPosition)
    {
        if (joinIndex == null) {
            return LongLists.emptyList();
        }
        if (!currentOuterJoinHasNulls) {
            return joinIndex.matchesNoNulls(currentOuterJoinValues, outerPosition);
        }
        return joinIndex.matches(currentOuterJoinValues, currentOuterJoinNulls, outerPosition);
    }

    private void loadInnerIfNecessary()
    {
        int batchCountBefore = bufferedInner.batches().size();
        bufferedInner.loadAll(inner, BATCH_SIZE, innerJoinColumns, inner.supportsRetainedBatches());
        ensureRetainedConstraintCacheCapacity(bufferedInner.batches().size());
        outputBuffer.captureInnerSchema(bufferedInner.schema());
        for (int batchIndex = batchCountBefore; batchIndex < bufferedInner.batches().size(); batchIndex++) {
            BufferedJoinInput.InnerBatch batch = bufferedInner.batches().get(batchIndex);
            indexInnerRows(batch, 0, batch.length(), batchIndex);
        }
    }

    private void indexInnerRows(BufferedJoinInput.InnerBatch batch, int startPosition, int length, int batchIndex)
    {
        Vector[] joinValues = new Vector[innerJoinColumns.length];
        Vector[] joinNulls = new Vector[innerJoinColumns.length];
        boolean hasNulls = false;
        for (int keyIndex = 0; keyIndex < innerJoinColumns.length; keyIndex++) {
            if (batch.retained()) {
                Output output = batch.retainedBatch().output(innerJoinColumns[keyIndex]);
                joinValues[keyIndex] = output.borrow(Stream.VALUES);
                joinNulls[keyIndex] = output.borrowOrNull(Stream.NULLS);
            }
            else {
                Streams streams = batch.columns()[innerJoinColumns[keyIndex]];
                joinValues[keyIndex] = streams.values();
                joinNulls[keyIndex] = streams.getOrNull(Stream.NULLS);
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
            currentOuterJoinNulls[keyIndex] = output.borrowOrNull(Stream.NULLS);
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
        if (joinValues.length == 3 && isSingleLongJoinCandidate(joinValues[0]) && isSingleLongJoinCandidate(joinValues[1]) && isSingleLongJoinCandidate(joinValues[2])) {
            return new LongTripleJoinIndex(expectedSize);
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

    public HashJoinOperator withProfileName(String profileName)
    {
        this.profileName = profileName;
        return this;
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
        long start = System.nanoTime();
        Streams materialized = outputIndex < outer.outputCount()
                ? materializeOuterOutput(outputIndex)
                : materializeInnerOutput(outputIndex - outer.outputCount());
        MaterializationProfile profile = CURRENT_MATERIALIZATION_PROFILE.get();
        if (profile != null) {
            profile.record(profileName != null ? profileName : "hash_join", outputIndex, materialized, currentOutputCount, System.nanoTime() - start);
        }
        currentOutputs[outputIndex] = materialized;
        return materialized;
    }

    private Streams materializeOuterOutput(int outputIndex)
    {
        Output sourceOutput = currentOuterBatch.output(outputIndex);
        int[] dictionaryIds = Arrays.copyOf(outputOuterPositions, currentOutputCount);
        if (sourceOutput.isValuesOnly()) {
            return Streams.ofValues(allocator.adopt(allocationContext, wrapComposedDictionary(dictionaryIds, sourceOutput.borrow(Stream.VALUES))));
        }

        Streams.Builder streams = Streams.builder();
        if (sourceOutput.hasValues()) {
            streams.put(Stream.VALUES, allocator.adopt(allocationContext, wrapComposedDictionary(dictionaryIds, sourceOutput.borrow(Stream.VALUES))));
        }
        Output sideOutput = new Output(sideStreams(sourceOutput), sourceOutput::borrow);
        Streams copiedSideStreams = null;
        if (!sideOutput.streams().isEmpty()) {
            if (currentOutputMask.all()) {
                copiedSideStreams = buffers.copyPositions(sideOutput, null, outputOuterPositions, currentOutputCount, 0, currentOutputCount);
            }
            else {
                Streams result = null;
                for (int index = 0; index < currentOutputMask.count(); index++) {
                    int outputPosition = currentOutputMask.position(index);
                    result = buffers.copySinglePosition(sideOutput, result, currentOutputCount, outputPosition, outputOuterPositions[outputPosition]);
                }
                copiedSideStreams = result == null ? buffers.emptyLike(outputSchema(outputIndex)) : result;
            }
        }
        if (sourceOutput.hasNulls()) {
            streams.put(Stream.NULLS, copiedSideStreams.get(Stream.NULLS));
        }
        if (sourceOutput.hasErrors()) {
            streams.put(Stream.ERRORS, copiedSideStreams.get(Stream.ERRORS));
        }
        return streams.build();
    }

    private static Set<Stream> sideStreams(Output output)
    {
        EnumSet<Stream> streams = EnumSet.noneOf(Stream.class);
        if (output.hasNulls()) {
            streams.add(Stream.NULLS);
        }
        if (output.hasErrors()) {
            streams.add(Stream.ERRORS);
        }
        return streams;
    }

    private Streams materializeInnerOutput(int innerOutputIndex)
    {
        if (currentOutputMask.all() && !hasNoMatchRows()) {
            prepareInnerOutputRuns();
            Streams wrapped = tryWrapSingleBatchInnerOutput(innerOutputIndex);
            if (wrapped != null) {
                return wrapped;
            }
            Streams result = null;
            for (int runIndex = 0; runIndex < preparedInnerRunCount; runIndex++) {
                int outputStart = outputInnerRunStarts[runIndex];
                int runLength = outputInnerRunLengths[runIndex];
                BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(outputInnerRunBatchIndexes[runIndex]);
                result = copyInnerPositions(result, innerBatch, runIndex, outputInnerRunBatchIndexes[runIndex], innerOutputIndex, outputStart, runLength, currentOutputCount);
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
            int innerBatchIndex = batchIndex(rowReference);
            BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(innerBatchIndex);
            result = copyInnerSinglePosition(result, innerBatch, innerBatchIndex, innerOutputIndex, currentOutputCount, outputPosition, rowPosition(rowReference), exposeNulls);
        }
        return result == null ? buffers.emptyLike(outputSchema(innerOutputIndex + outer.outputCount())) : result;
    }


    private Streams tryWrapSingleBatchInnerOutput(int innerOutputIndex)
    {
        if (currentOutputCount == 0) {
            return null;
        }

        if (preparedInnerRunCount != 1) {
            return null;
        }

        int innerBatchIndex = outputInnerRunBatchIndexes[0];
        BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(innerBatchIndex);
        if (!innerBatch.retained()) {
            Streams output = innerBatch.columns()[innerOutputIndex];
            if (output == null || !output.hasValues()) {
                return null;
            }

            if (output.isValuesOnly()) {
                return Streams.ofValues(allocator.adopt(allocationContext, wrapComposedDictionary(Arrays.copyOf(outputInnerLogicalPositions, currentOutputCount), output.values())));
            }

            Streams.Builder wrapped = Streams.builder();
            wrapped.put(Stream.VALUES, allocator.adopt(allocationContext, wrapComposedDictionary(Arrays.copyOf(outputInnerLogicalPositions, currentOutputCount), output.values())));
            Streams copiedSideStreams = null;
            Streams sideStreams = sideStreamValues(output);
            if (!sideStreams.streams().isEmpty()) {
                copiedSideStreams = buffers.copyPositions((Streams) null, sideStreams, outputInnerLogicalPositions, currentOutputCount, 0, currentOutputCount);
            }
            if (output.hasNulls()) {
                wrapped.put(Stream.NULLS, copiedSideStreams.get(Stream.NULLS));
            }
            if (output.hasErrors()) {
                wrapped.put(Stream.ERRORS, copiedSideStreams.get(Stream.ERRORS));
            }
            return wrapped.build();
        }

        return tryWrapRetainedInnerOutput(innerOutputIndex, innerBatch);
    }

    private Streams tryWrapRetainedInnerOutput(int innerOutputIndex, BufferedJoinInput.InnerBatch innerBatch)
    {
        if (!innerBatch.retained()) {
            return null;
        }

        Output output = innerBatch.retainedBatch().output(innerOutputIndex);
        if (output.isValuesOnly()) {
            return Streams.ofValues(allocator.adopt(allocationContext, wrapComposedDictionary(Arrays.copyOf(outputInnerSourcePositions, currentOutputCount), output.borrow(Stream.VALUES))));
        }

        Streams.Builder wrapped = Streams.builder();
        if (output.hasValues()) {
            wrapped.put(Stream.VALUES, allocator.adopt(allocationContext, wrapComposedDictionary(Arrays.copyOf(outputInnerSourcePositions, currentOutputCount), output.borrow(Stream.VALUES))));
        }
        Streams copiedSideStreams = null;
        Streams sideStreams = sideStreamValues(output);
        if (!sideStreams.streams().isEmpty()) {
            copiedSideStreams = buffers.copyPositions((Streams) null, sideStreams, outputInnerSourcePositions, currentOutputCount, 0, currentOutputCount);
        }
        if (output.hasNulls()) {
            wrapped.put(Stream.NULLS, copiedSideStreams.get(Stream.NULLS));
        }
        if (output.hasErrors()) {
            wrapped.put(Stream.ERRORS, copiedSideStreams.get(Stream.ERRORS));
        }
        return wrapped.build();
    }

    private static Streams sideStreamValues(Streams streams)
    {
        Streams.Builder sideInput = Streams.builder();
        if (streams.hasNulls()) {
            sideInput.put(Stream.NULLS, streams.get(Stream.NULLS));
        }
        if (streams.hasErrors()) {
            sideInput.put(Stream.ERRORS, streams.get(Stream.ERRORS));
        }
        return sideInput.build();
    }

    private static DictionaryVector wrapComposedDictionary(int[] dictionaryIds, Vector values)
    {
        Vector baseValues = values;
        while (baseValues instanceof DictionaryVector dictionary) {
            int[] baseIds = dictionary.ids();
            for (int index = 0; index < dictionaryIds.length; index++) {
                dictionaryIds[index] = baseIds[dictionaryIds[index]];
            }
            baseValues = dictionary.values();
        }
        return DictionaryVector.wrap(dictionaryIds, baseValues);
    }

    private static Streams sideStreamValues(Output output)
    {
        Streams.Builder sideInput = Streams.builder();
        if (output.hasNulls()) {
            sideInput.put(Stream.NULLS, output.borrow(Stream.NULLS));
        }
        if (output.hasErrors()) {
            sideInput.put(Stream.ERRORS, output.borrow(Stream.ERRORS));
        }
        return sideInput.build();
    }



    private Streams copyInnerPositions(Streams existing, BufferedJoinInput.InnerBatch innerBatch, int runIndex, int innerBatchIndex, int innerOutputIndex, int positionStart, int positionCount, int size)
    {
        if (!innerBatch.retained()) {
            return buffers.copyPositions(existing, innerBatch.columns()[innerOutputIndex], outputInnerLogicalPositions, positionStart, positionCount, positionStart, size);
        }

        constrainRetainedInnerBatch(innerBatchIndex, innerBatch, outputInnerRunUniqueStarts[runIndex], outputInnerRunUniqueCounts[runIndex]);
        Output output = innerBatch.retainedBatch().output(innerOutputIndex);
        return buffers.copyPositions(output, existing, outputInnerSourcePositions, positionStart, positionCount, positionStart, size);
    }


    private Streams copyInnerSinglePosition(Streams existing, BufferedJoinInput.InnerBatch innerBatch, int innerBatchIndex, int innerOutputIndex, int size, int outputPosition, int logicalPosition, boolean exposeNulls)
    {
        if (!innerBatch.retained()) {
            return withSyntheticNulls(existing, buffers.copySinglePosition(existing, innerBatch.columns()[innerOutputIndex], size, outputPosition, logicalPosition), size, outputPosition, exposeNulls);
        }

        int sourcePosition = innerBatch.sourcePosition(logicalPosition);
        constrainRetainedInnerBatch(innerBatchIndex, innerBatch, logicalPosition);
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

    private void constrainRetainedInnerBatch(int innerBatchIndex, BufferedJoinInput.InnerBatch innerBatch, int[] logicalPositions, int positionCount)
    {
        constrainRetainedInnerBatch(innerBatchIndex, innerBatch, logicalPositions, 0, positionCount);
    }

    private void constrainRetainedInnerBatch(int innerBatchIndex, BufferedJoinInput.InnerBatch innerBatch, int[] logicalPositions, int positionStart, int positionCount)
    {
        if (!innerBatch.retained()) {
            return;
        }
        boolean sorted = true;
        int previousSourcePosition = -1;
        for (int index = 0; index < positionCount; index++) {
            int sourcePosition = innerBatch.sourcePosition(logicalPositions[positionStart + index]);
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
        int cachedCount = retainedConstraintCountsByBatch[innerBatchIndex];
        int[] cachedPositions = retainedConstraintPositionsByBatch[innerBatchIndex];
        if (uniqueCount == cachedCount &&
                cachedPositions != null &&
                Arrays.equals(retainedInnerMaskPositionsScratch, 0, uniqueCount, cachedPositions, 0, uniqueCount)) {
            return;
        }
        innerBatch.retainedBatch().constrain(allocator.allocateSparseMask(
                allocationContext,
                retainedInnerMaskPositionsScratch,
                uniqueCount,
                innerBatch.retainedBatch().borrowMask().size()));
        if (cachedPositions == null || cachedPositions.length < uniqueCount) {
            cachedPositions = new int[Math.max(uniqueCount, 4)];
            retainedConstraintPositionsByBatch[innerBatchIndex] = cachedPositions;
        }
        System.arraycopy(retainedInnerMaskPositionsScratch, 0, cachedPositions, 0, uniqueCount);
        retainedConstraintCountsByBatch[innerBatchIndex] = uniqueCount;
    }

    private void constrainRetainedInnerBatch(int innerBatchIndex, BufferedJoinInput.InnerBatch innerBatch, int uniquePositionStart, int uniqueCount)
    {
        if (!innerBatch.retained()) {
            return;
        }
        int cachedCount = retainedConstraintCountsByBatch[innerBatchIndex];
        int[] cachedPositions = retainedConstraintPositionsByBatch[innerBatchIndex];
        if (uniqueCount == cachedCount &&
                cachedPositions != null &&
                Arrays.equals(outputInnerUniqueSourcePositions, uniquePositionStart, uniquePositionStart + uniqueCount, cachedPositions, 0, uniqueCount)) {
            return;
        }
        System.arraycopy(outputInnerUniqueSourcePositions, uniquePositionStart, retainedInnerMaskPositionsScratch, 0, uniqueCount);
        innerBatch.retainedBatch().constrain(allocator.allocateSparseMask(
                allocationContext,
                retainedInnerMaskPositionsScratch,
                uniqueCount,
                innerBatch.retainedBatch().borrowMask().size()));
        if (cachedPositions == null || cachedPositions.length < uniqueCount) {
            cachedPositions = new int[Math.max(uniqueCount, 4)];
            retainedConstraintPositionsByBatch[innerBatchIndex] = cachedPositions;
        }
        System.arraycopy(outputInnerUniqueSourcePositions, uniquePositionStart, cachedPositions, 0, uniqueCount);
        retainedConstraintCountsByBatch[innerBatchIndex] = uniqueCount;
    }

    private int copySortedUniquePositions(int[] sourcePositions, int positionStart, int positionCount, int[] targetPositions, int targetStart)
    {
        System.arraycopy(sourcePositions, positionStart, retainedInnerMaskPositionsScratch, 0, positionCount);
        Arrays.sort(retainedInnerMaskPositionsScratch, 0, positionCount);
        int uniqueCount = 0;
        int previous = -1;
        for (int index = 0; index < positionCount; index++) {
            int position = retainedInnerMaskPositionsScratch[index];
            if (uniqueCount == 0 || position != previous) {
                targetPositions[targetStart + uniqueCount] = position;
                previous = position;
                uniqueCount++;
            }
        }
        return uniqueCount;
    }

    private void prepareInnerOutputRuns()
    {
        if (preparedInnerRunCount >= 0) {
            return;
        }

        int runCount = 0;
        int runStart = 0;
        int uniquePositionStart = 0;
        while (runStart < currentOutputCount) {
            long rowReference = outputInnerRows[runStart];
            int batchIndex = batchIndex(rowReference);
            BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(batchIndex);
            boolean retained = innerBatch.retained();

            int runEnd = runStart;
            while (runEnd < currentOutputCount && batchIndex(outputInnerRows[runEnd]) == batchIndex) {
                int logicalPosition = rowPosition(outputInnerRows[runEnd]);
                outputInnerBatchIndexes[runEnd] = batchIndex;
                outputInnerLogicalPositions[runEnd] = logicalPosition;
                outputInnerSourcePositions[runEnd] = retained ? innerBatch.sourcePosition(logicalPosition) : logicalPosition;
                runEnd++;
            }

            outputInnerRunStarts[runCount] = runStart;
            outputInnerRunLengths[runCount] = runEnd - runStart;
            outputInnerRunBatchIndexes[runCount] = batchIndex;
            if (retained) {
                int uniqueCount = copySortedUniquePositions(outputInnerSourcePositions, runStart, runEnd - runStart, outputInnerUniqueSourcePositions, uniquePositionStart);
                outputInnerRunUniqueStarts[runCount] = uniquePositionStart;
                outputInnerRunUniqueCounts[runCount] = uniqueCount;
                uniquePositionStart += uniqueCount;
            }
            else {
                outputInnerRunUniqueStarts[runCount] = uniquePositionStart;
                outputInnerRunUniqueCounts[runCount] = 0;
            }
            runCount++;
            runStart = runEnd;
        }
        preparedInnerRunCount = runCount;
    }

    private void constrainRetainedInnerBatch(int innerBatchIndex, BufferedJoinInput.InnerBatch innerBatch, int logicalPosition)
    {
        if (!innerBatch.retained()) {
            return;
        }
        int sourcePosition = innerBatch.sourcePosition(logicalPosition);
        int cachedCount = retainedConstraintCountsByBatch[innerBatchIndex];
        int[] cachedPositions = retainedConstraintPositionsByBatch[innerBatchIndex];
        if (cachedCount == 1 &&
                cachedPositions != null &&
                cachedPositions[0] == sourcePosition) {
            return;
        }
        retainedInnerMaskPositionsScratch[0] = sourcePosition;
        innerBatch.retainedBatch().constrain(allocator.allocateSparseMask(
                allocationContext,
                retainedInnerMaskPositionsScratch,
                1,
                innerBatch.retainedBatch().borrowMask().size()));
        if (cachedPositions == null || cachedPositions.length == 0) {
            cachedPositions = new int[4];
            retainedConstraintPositionsByBatch[innerBatchIndex] = cachedPositions;
        }
        cachedPositions[0] = sourcePosition;
        retainedConstraintCountsByBatch[innerBatchIndex] = 1;
    }

    private void ensureRetainedConstraintCacheCapacity(int batchCount)
    {
        if (retainedConstraintCountsByBatch.length >= batchCount) {
            return;
        }
        int previousLength = retainedConstraintCountsByBatch.length;
        retainedConstraintCountsByBatch = Arrays.copyOf(retainedConstraintCountsByBatch, batchCount);
        Arrays.fill(retainedConstraintCountsByBatch, previousLength, batchCount, -1);
        retainedConstraintPositionsByBatch = Arrays.copyOf(retainedConstraintPositionsByBatch, batchCount);
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

        void add(Vector[] values, Vector[] nulls, int position, long rowReference);

        LongList matches(Vector[] values, Vector[] nulls, int position);

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
        public void add(Vector[] values, Vector[] nulls, int position, long rowReference)
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
        public LongList matches(Vector[] values, Vector[] nulls, int position)
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

        public void matchRows(Vector values, Vector nulls, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            switch (values) {
                case org.weakref.nitro.data.I64Vector longValues -> matchLongRows(longValues.values(), nulls, positions, positionCount, matches, singleMatches);
                case org.weakref.nitro.data.I32Vector intValues -> matchIntRows(intValues.values(), nulls, positions, positionCount, matches, singleMatches);
                case DictionaryVector dictionary -> matchDictionaryRows(dictionary, nulls, positions, positionCount, matches, singleMatches);
                case org.weakref.nitro.data.RleVector rle -> matchRleRows(rle, nulls, positions, positionCount, matches, singleMatches);
                default -> {
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (OperatorVectorSupport.isNull(nulls, position)) {
                            matches[index] = LongLists.emptyList();
                        }
                        else {
                            matches[index] = batchedRowsForSlot(findSlot(OperatorVectorSupport.longValue(values, position)), singleMatches[index]);
                        }
                    }
                }
            }
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

        private void matchLongRows(long[] values, Vector nulls, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (OperatorVectorSupport.isNull(nulls, position)) {
                    matches[index] = LongLists.emptyList();
                }
                else {
                    matches[index] = batchedRowsForSlot(findSlot(values[position]), singleMatches[index]);
                }
            }
        }

        private void matchIntRows(int[] values, Vector nulls, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (OperatorVectorSupport.isNull(nulls, position)) {
                    matches[index] = LongLists.emptyList();
                }
                else {
                    matches[index] = batchedRowsForSlot(findSlot(values[position]), singleMatches[index]);
                }
            }
        }

        private void matchDictionaryRows(DictionaryVector values, Vector nulls, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            int[] ids = values.ids();
            switch (values.values()) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] dictionaryValues = longValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (OperatorVectorSupport.isNull(nulls, position)) {
                            matches[index] = LongLists.emptyList();
                        }
                        else {
                            matches[index] = batchedRowsForSlot(findSlot(dictionaryValues[ids[position]]), singleMatches[index]);
                        }
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] dictionaryValues = intValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (OperatorVectorSupport.isNull(nulls, position)) {
                            matches[index] = LongLists.emptyList();
                        }
                        else {
                            matches[index] = batchedRowsForSlot(findSlot(dictionaryValues[ids[position]]), singleMatches[index]);
                        }
                    }
                }
                default -> {
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (OperatorVectorSupport.isNull(nulls, position)) {
                            matches[index] = LongLists.emptyList();
                        }
                        else {
                            matches[index] = batchedRowsForSlot(findSlot(OperatorVectorSupport.longValue(values, position)), singleMatches[index]);
                        }
                    }
                }
            }
        }

        private void matchRleRows(org.weakref.nitro.data.RleVector values, Vector nulls, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (OperatorVectorSupport.isNull(nulls, position)) {
                    matches[index] = LongLists.emptyList();
                }
                else {
                    matches[index] = batchedRowsForSlot(findSlot(OperatorVectorSupport.longValue(values, position)), singleMatches[index]);
                }
            }
        }

        private LongList batchedRowsForSlot(int index, SingleLongList singleMatch)
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
        public void add(Vector[] values, Vector[] nulls, int position, long rowReference)
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
        public LongList matches(Vector[] values, Vector[] nulls, int position)
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

        private static boolean hasNull(Vector[] nulls, int position)
        {
            for (Vector nullsVector : nulls) {
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
        public void add(Vector[] values, Vector[] nulls, int position, long rowReference)
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
        public LongList matches(Vector[] values, Vector[] nulls, int position)
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

    private static final class LongTripleJoinIndex
            implements JoinIndex
    {
        private static final float LOAD_FACTOR = 0.75f;

        private long[] firstKeys;
        private long[] secondKeys;
        private long[] thirdKeys;
        private long[] singleRows;
        private LongArrayList[] rowsBySlot;
        private int mask;
        private int maxFill;
        private int size;
        private final SingleLongList singleMatch = new SingleLongList();

        private LongTripleJoinIndex(int expectedSize)
        {
            int capacity = 16;
            while (capacity < expectedSize / LOAD_FACTOR) {
                capacity <<= 1;
            }
            firstKeys = new long[capacity];
            secondKeys = new long[capacity];
            thirdKeys = new long[capacity];
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
        public void add(Vector[] values, Vector[] nulls, int position, long rowReference)
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
                    OperatorVectorSupport.longValue(values[2], position),
                    rowReference);
        }

        @Override
        public LongList matches(Vector[] values, Vector[] nulls, int position)
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
            long third = OperatorVectorSupport.longValue(values[2], position);
            int slot = findSlot(first, second, third);
            if (isEmptySlot(slot) || firstKeys[slot] != first || secondKeys[slot] != second || thirdKeys[slot] != third) {
                return LongLists.emptyList();
            }
            LongArrayList rows = rowsBySlot[slot];
            if (rows != null) {
                return rows;
            }
            return singleMatch.withValue(singleRows[slot]);
        }

        private int findSlot(long first, long second, long third)
        {
            int slot = mix(first, second, third) & mask;
            while (!isEmptySlot(slot) && (firstKeys[slot] != first || secondKeys[slot] != second || thirdKeys[slot] != third)) {
                slot = (slot + 1) & mask;
            }
            return slot;
        }

        private void rehash()
        {
            long[] previousFirstKeys = firstKeys;
            long[] previousSecondKeys = secondKeys;
            long[] previousThirdKeys = thirdKeys;
            long[] previousSingleRows = singleRows;
            LongArrayList[] previousRowsBySlot = rowsBySlot;
            int capacity = previousRowsBySlot.length * 2;

            firstKeys = new long[capacity];
            secondKeys = new long[capacity];
            thirdKeys = new long[capacity];
            singleRows = emptyRows(capacity);
            rowsBySlot = new LongArrayList[capacity];
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
            size = 0;

            for (int index = 0; index < previousFirstKeys.length; index++) {
                if (previousSingleRows[index] == NO_MATCH_ROW_REFERENCE) {
                    continue;
                }
                int slot = findSlot(previousFirstKeys[index], previousSecondKeys[index], previousThirdKeys[index]);
                firstKeys[slot] = previousFirstKeys[index];
                secondKeys[slot] = previousSecondKeys[index];
                thirdKeys[slot] = previousThirdKeys[index];
                singleRows[slot] = previousSingleRows[index];
                rowsBySlot[slot] = previousRowsBySlot[index];
                size++;
            }
        }

        private void addRow(long first, long second, long third, long rowReference)
        {
            int slot = findSlot(first, second, third);
            if (isEmptySlot(slot)) {
                firstKeys[slot] = first;
                secondKeys[slot] = second;
                thirdKeys[slot] = third;
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

        private static int mix(long first, long second, long third)
        {
            long hash = 31 * Long.hashCode(first) + Long.hashCode(second);
            hash = 31 * hash + Long.hashCode(third);
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

    private static SingleLongList[] createSingleLongLists(int size)
    {
        SingleLongList[] matches = new SingleLongList[size];
        for (int index = 0; index < size; index++) {
            matches[index] = new SingleLongList();
        }
        return matches;
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
        public void add(Vector[] values, Vector[] nulls, int position, long rowReference)
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
        public LongList matches(Vector[] values, Vector[] nulls, int position)
        {
            OperatorKeySemantics.Key key = keyForPosition(values, nulls, position, outerProbeKeys, outerCompositeProbeKey);
            if (key == null) {
                return LongLists.emptyList();
            }
            LongArrayList rows = rowsByKey.get(key);
            return rows == null ? LongLists.emptyList() : rows;
        }

        private static OperatorKeySemantics.Key keyForPosition(Vector[] values, Vector[] nulls, int position, OperatorKeySemantics.Key[] reusableProbeKeys, OperatorKeySemantics.CompositeProbeKey reusableCompositeProbeKey)
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
