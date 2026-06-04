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
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.ConcatenatedBooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.builtin.VectorAccess;
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

    private static final int BATCH_SIZE = Integer.getInteger("nitro.hash.join.maxBatchRows", 4_096);
    private static final long NO_MATCH_ROW_REFERENCE = -1L;
    private static final Vector[] NO_NULL_STREAMS = new Vector[0];
    private static final ThreadLocal<MaterializationProfile> CURRENT_MATERIALIZATION_PROFILE = new ThreadLocal<>();
    private final Allocator allocator;
    private final Allocator.Context allocationContext = new Allocator.Context("HashJoinOperator");
    private final Operator outer;
    private final Operator inner;
    private final int outerOutputCount;
    private final int innerOutputCount;
    private final int totalOutputCount;
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
    // Lazily built, per (inner batch, inner column) unified dictionary view for non-retained build
    // columns whose VALUES are a BinaryVector. Built once over the (small) build side; reused to emit
    // every probe-output batch's matched rows as a DictionaryVector over the shared dictionary instead
    // of flattening the bytes per output row. Keyed by batchIndex * innerOutputCount + innerOutputIndex.
    private final Map<Integer, BuildDictionary> buildDictionaries = new HashMap<>();
    private JoinIndex joinIndex;

    private Mask currentOuterMask;
    private Batch currentOuterBatch;
    private int currentOuterMaskIndex;
    private int outerRemaining;
    private int currentOuterPosition;
    private boolean currentOuterPositionReady;
    private LongList currentMatches = LongLists.emptyList();
    private int currentMatchCount;
    private boolean currentOuterJoinHasNulls;
    private int currentMatchIndex;
    private int currentOutputCount;
    private Mask currentOutputMask;
    private int[] currentOuterDictionaryIds;
    private int preparedOuterCount;
    private int preparedOuterIndex;
    private boolean done;
    private boolean outerConstrained;
    private final boolean outerSupportsReborrow;
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
        this.outerSupportsReborrow = outer.supportsConstrainedReborrow();
        this.outerOutputCount = outer.outputCount();
        this.innerOutputCount = inner.outputCount();
        this.totalOutputCount = outerOutputCount + innerOutputCount;
        this.probeOuterJoin = probeOuterJoin;
        this.outerJoinColumns = outerJoinColumns.clone();
        this.innerJoinColumns = innerJoinColumns.clone();
        this.buffers = new JoinBufferSupport(allocator, allocationContext);
        this.bufferedInner = new BufferedJoinInput(buffers, innerOutputCount);
        this.outputBuffer = new JoinOutputBuffer(buffers, BATCH_SIZE, outerOutputCount, innerOutputCount);
        this.currentOuterJoinValues = new Vector[outerJoinColumns.length];
        this.currentOuterJoinNulls = new Vector[outerJoinColumns.length];
        this.currentOutputs = new Streams[totalOutputCount];
        Arrays.fill(retainedConstraintCountsByBatch, -1);
    }

    @Override
    public int outputCount()
    {
        return totalOutputCount;
    }

    @Override
    public boolean hasNext()
    {
        return !done;
    }

    @Override
    public Batch next()
    {
        long start = System.nanoTime();
        Mask batchMask = produceBatch();
        long afterProduceBatch = System.nanoTime();
        preparedInnerRunCount = -1;
        currentOutputMask = batchMask;
        outerConstrained = false;
        currentOuterDictionaryIds = null;
        java.util.Arrays.fill(currentOutputs, null);
        Output[] outputs = new Output[totalOutputCount];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            outputs[outputIndex] = resultOutput(outputIndex);
        }
        long afterBuildOutputs = System.nanoTime();
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
                currentMatchCount = currentMatches.size();
                preparedOuterIndex++;
                currentOuterPositionReady = true;
                currentMatchIndex = 0;
            }

            if (currentMatchCount == 0) {
                if (probeOuterJoin) {
                    outputOuterPositions[outputPosition] = currentOuterPosition;
                    outputInnerRows[outputPosition] = NO_MATCH_ROW_REFERENCE;
                    outputPosition++;
                }
                outerRemaining--;
                currentOuterPositionReady = false;
                currentMatches = LongLists.emptyList();
                currentMatchCount = 0;
                continue;
            }

            while (currentMatchIndex < currentMatchCount && outputPosition < BATCH_SIZE) {
                outputOuterPositions[outputPosition] = currentOuterPosition;
                outputInnerRows[outputPosition] = currentMatches.getLong(currentMatchIndex++);
                outputPosition++;
            }

            if (currentMatchIndex == currentMatchCount) {
                outerRemaining--;
                currentOuterPositionReady = false;
                currentMatches = LongLists.emptyList();
                currentMatchCount = 0;
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
        long start = System.nanoTime();
        preparedOuterCount = Math.min(currentOuterMask.count() - currentOuterMaskIndex, BATCH_SIZE);
        preparedOuterIndex = 0;
        for (int index = 0; index < preparedOuterCount; index++) {
            preparedOuterPositions[index] = currentOuterMask.position(currentOuterMaskIndex++);
        }

        joinIndex.matchRows(currentOuterJoinValues, currentOuterJoinNulls, currentOuterJoinHasNulls, preparedOuterPositions, preparedOuterCount, preparedOuterMatches, preparedSingleMatches);
    }

    private String genericProbeKind()
    {
        return switch (joinIndex) {
            case FlatJoinIndex _ -> "flat";
            case LongPairJoinIndex _ -> "pair";
            case LongTripleJoinIndex _ -> "triple";
            case ObjectJoinIndex _ -> "object";
            case null, default -> "object";
        };
    }

    private boolean loadNextOuterBatch()
    {
        while (outer.hasNext()) {
            currentOuterBatch = outer.next();
            // When the outer can satisfy a constrained re-borrow, skip eager outer schema capture:
            // it borrows a representative VALUES vector for every outer column, forcing lazy
            // projected payloads to materialize. The schema is then derived on demand in
            // outputSchema() from currentOuterBatch. When the outer's reader advances irreversibly,
            // capture the schema eagerly now while the batch is live (deferring past the advance
            // would read an already-advanced source).
            if (!outerSupportsReborrow) {
                outputBuffer.captureOuterSchema(currentOuterBatch);
            }
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
        String operatorName = profileName != null ? profileName : "hash_join";
        String probeKind = genericProbeKind();
        if (!currentOuterJoinHasNulls) {
            return joinIndex.matchesNoNulls(currentOuterJoinValues, outerPosition);
        }
        return joinIndex.matches(currentOuterJoinValues, currentOuterJoinNulls, outerPosition);
    }

    private void loadInnerIfNecessary()
    {
        int batchCountBefore = bufferedInner.batches().size();
        bufferedInner.loadAll(inner, BATCH_SIZE, innerJoinColumns, inner.supportsRetainedBatches(), !inner.supportsRetainedBatches() && inner.supportsConstrainedReborrow());
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
            long start = System.nanoTime();
            currentOuterJoinValues[keyIndex] = output.borrow(Stream.VALUES);
            currentOuterJoinNulls[keyIndex] = output.isKnownAllFalse(Stream.NULLS) ? null : output.borrowOrNull(Stream.NULLS);
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

        Set<Stream> streams = outputIndex < outerOutputCount
                ? currentOuterBatch.output(outputIndex).streams()
                : innerOutputStreams(outputIndex - outerOutputCount);
        Set<Stream> knownAllFalseStreams = resultKnownAllFalseStreams(outputIndex, streams);
        return new Output(
                streams,
                stream -> {
                    Streams materialized = materializeOutput(outputIndex);
                    if (materialized.has(stream)) {
                        return materialized.get(stream);
                    }
                    if (knownAllFalseStreams.contains(stream)) {
                        return allFalseBooleanStream(currentOutputCount);
                    }
                    throw new IllegalArgumentException("Output does not expose stream: " + stream);
                },
                (stream, vector) -> allocator.transfer(allocationContext, vector),
                (_, _) -> {},
                null,
                null)
                .withKnownAllFalse(knownAllFalseStreams);
    }

    private Streams outputSchema(int outputIndex)
    {
        if (outputIndex < outerOutputCount) {
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
                    streams.put(Stream.NULLS, new BooleanVector(0));
                }
                if (output.hasErrors()) {
                    streams.put(Stream.ERRORS, new BooleanVector(0));
                }
                return streams.build();
            }
            return null;
        }
        Streams schema = outputBuffer.innerSchema()[outputIndex - outerOutputCount];
        if (schema != null) {
            return probeOuterJoin ? ensureNullStream(schema) : schema;
        }
        Streams bufferedSchema = bufferedInner.outputSchema(outputIndex - outerOutputCount);
        return probeOuterJoin && bufferedSchema != null ? ensureNullStream(bufferedSchema) : bufferedSchema;
    }

    private Streams materializeOutput(int outputIndex)
    {
        Streams existing = currentOutputs[outputIndex];
        if (existing != null) {
            return existing;
        }
        long start = System.nanoTime();
        Streams materialized = outputIndex < outerOutputCount
                ? materializeOuterOutput(outputIndex)
                : materializeInnerOutput(outputIndex - outerOutputCount);
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
        if (!outerSupportsReborrow) {
            // Outer cannot satisfy a constrained re-borrow (e.g. a Parquet-backed subplan or a join
            // result): keep the baseline dictionary-wrap, which borrows the live outer column once
            // and indexes it by the matched output positions. No constraint is pushed and no deferral
            // happens past the source's advance.
            if (sourceOutput.isValuesOnly()) {
                return Streams.ofValues(allocator.adopt(allocationContext, buildOuterDictionaryStream(sourceOutput.borrow(Stream.VALUES))));
            }
            Streams.Builder streams = Streams.builder();
            if (sourceOutput.hasValues()) {
                streams.put(Stream.VALUES, allocator.adopt(allocationContext, buildOuterDictionaryStream(sourceOutput.borrow(Stream.VALUES))));
            }
            if (sourceOutput.hasNulls() && !sourceOutput.isKnownAllFalse(Stream.NULLS)) {
                streams.put(Stream.NULLS, allocator.adopt(allocationContext, buildOuterDictionaryStream(sourceOutput.borrow(Stream.NULLS))));
            }
            if (sourceOutput.hasErrors() && !sourceOutput.isKnownAllFalse(Stream.ERRORS)) {
                streams.put(Stream.ERRORS, allocator.adopt(allocationContext, buildOuterDictionaryStream(sourceOutput.borrow(Stream.ERRORS))));
            }
            return streams.build();
        }

        // Outer can satisfy a constrained re-borrow: narrow it to the matched rows so a lazy
        // projected payload computes only the rows the join emits, then flat-copy the matched
        // positions into a dense vector indexable directly by output position. Materialization stays
        // lazy: it only runs when an outer stream is borrowed.
        constrainOuterIfNecessary();
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

    private int[] outerDictionaryIds()
    {
        if (currentOuterDictionaryIds == null) {
            currentOuterDictionaryIds = Arrays.copyOf(outputOuterPositions, currentOutputCount);
        }
        return currentOuterDictionaryIds;
    }

    private Vector buildOuterDictionaryStream(Vector source)
    {
        // wrapComposedDictionary mutates the passed ids array through nested encodings, so pass a per-column copy
        int[] ids = Arrays.copyOf(outerDictionaryIds(), currentOutputCount);
        return wrapComposedDictionary(ids, source);
    }

    private void constrainOuterIfNecessary()
    {
        // The first time an outer column of this output batch is materialized, narrow the outer
        // operator to exactly the outer positions that survived the join for this batch. This lets a
        // lazy outer subplan (a deferred projected payload) compute only the rows the join emits.
        // Only push a constraint to an outer that can satisfy a constrained re-borrow; a source
        // whose reader advances irreversibly (a Parquet scan) must not be constrained and re-borrowed
        // - for those, the flat copy above borrows the live batch column once without deferral.
        if (!outerSupportsReborrow || outerConstrained || currentOuterBatch == null || !currentOuterBatchFullyConsumed()) {
            return;
        }
        outerConstrained = true;
        outer.constrain(matchedOuterMask());
    }

    /**
     * Whether this output batch is the last one drawing from the current outer batch — i.e. the
     * outer batch's probe positions are fully exhausted, so no later output batch will reference
     * {@code currentOuterBatch}. Only then is it safe to push a narrowing constraint to the outer:
     * its borrowed columns would otherwise be shared by later output batches drawing from the same
     * outer batch.
     */
    private boolean currentOuterBatchFullyConsumed()
    {
        return outerRemaining == 0
                && !currentOuterPositionReady
                && preparedOuterIndex >= preparedOuterCount
                && currentOuterMaskIndex >= currentOuterMask.count();
    }

    private Mask matchedOuterMask()
    {
        int totalPositions = currentOuterMask.size();
        if (currentOutputCount == 0 || currentOutputMask.none()) {
            return allocator.allocateSparseMask(allocationContext, new int[0], 0, totalPositions);
        }

        int count = currentOutputMask.count();
        int[] positions = new int[Math.min(count, currentOuterMask.count())];
        int selectedCount = 0;
        int previous = -1;
        for (int index = 0; index < count; index++) {
            int outputPosition = currentOutputMask.position(index);
            int outerPosition = outputOuterPositions[outputPosition];
            if (outerPosition != previous) {
                positions[selectedCount++] = outerPosition;
                previous = outerPosition;
            }
        }
        return allocator.allocateSparseMask(allocationContext, positions, selectedCount, totalPositions);
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
            Vector dictionaryValues = tryWrapNonRetainedDictionaryValues(innerOutputIndex);
            if (dictionaryValues != null) {
                Streams result = Streams.ofValues(allocator.adopt(allocationContext, dictionaryValues));
                // VALUES are carried as a unified dictionary (no byte copy); the cheap boolean side
                // streams still flatten through the standard per-run copy path.
                for (int runIndex = 0; runIndex < preparedInnerRunCount; runIndex++) {
                    int outputStart = outputInnerRunStarts[runIndex];
                    int runLength = outputInnerRunLengths[runIndex];
                    BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(outputInnerRunBatchIndexes[runIndex]);
                    result = copyInnerPositions(result, innerBatch, runIndex, outputInnerRunBatchIndexes[runIndex], innerOutputIndex, outputStart, runLength, currentOutputCount, false, true, true);
                }
                return result;
            }
            Streams wrappedSideStreams = tryWrapMultiRunInnerBooleanSideStreams(innerOutputIndex);
            Streams result = wrappedSideStreams;
            boolean copyNulls = wrappedSideStreams == null || !wrappedSideStreams.hasNulls();
            boolean copyErrors = wrappedSideStreams == null || !wrappedSideStreams.hasErrors();
            for (int runIndex = 0; runIndex < preparedInnerRunCount; runIndex++) {
                int outputStart = outputInnerRunStarts[runIndex];
                int runLength = outputInnerRunLengths[runIndex];
                BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(outputInnerRunBatchIndexes[runIndex]);
                result = copyInnerPositions(result, innerBatch, runIndex, outputInnerRunBatchIndexes[runIndex], innerOutputIndex, outputStart, runLength, currentOutputCount, true, copyNulls, copyErrors);
            }
            return result == null ? buffers.emptyLike(outputSchema(innerOutputIndex + outerOutputCount)) : result;
        }

        Streams result = null;
        boolean exposeNulls = probeOuterJoin || innerOutputStreams(innerOutputIndex).contains(Stream.NULLS);
        Streams nullInnerSchema = null;
        // The constraint mask can select no rows while a downstream consumer still borrows this column
        // and indexes a sample position (for example ProjectOperator probes one row to discover which
        // streams a column exposes). Outer-side columns are always materialized over the full output
        // range, so an inner-side column must do the same when its constraint is empty; otherwise the
        // borrowed column has length zero and indexing the sample position throws.
        boolean materializeFullRange = currentOutputMask.none();
        int positionCount = materializeFullRange ? currentOutputCount : currentOutputMask.count();
        for (int index = 0; index < positionCount; index++) {
            int outputPosition = materializeFullRange ? index : currentOutputMask.position(index);
            long rowReference = outputInnerRows[outputPosition];
            if (rowReference == NO_MATCH_ROW_REFERENCE) {
                if (nullInnerSchema == null) {
                    nullInnerSchema = outputSchema(innerOutputIndex + outerOutputCount);
                    if (nullInnerSchema == null) {
                        throw new IllegalStateException("Unable to determine inner output schema for left join");
                    }
                }
                result = copyNullInnerPosition(result, nullInnerSchema, currentOutputCount, outputPosition);
                continue;
            }
            int innerBatchIndex = batchIndex(rowReference);
            BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(innerBatchIndex);
            result = copyInnerSinglePosition(result, innerBatch, innerBatchIndex, innerOutputIndex, currentOutputCount, outputPosition, rowPosition(rowReference), exposeNulls);
        }
        return result == null ? buffers.emptyLike(outputSchema(innerOutputIndex + outerOutputCount)) : result;
    }

    private Streams tryWrapMultiRunInnerBooleanSideStreams(int innerOutputIndex)
    {
        if (currentOutputCount == 0 || preparedInnerRunCount <= 1) {
            return null;
        }

        Streams.Builder wrapped = Streams.builder();
        Vector nulls = tryWrapMultiRunInnerBooleanStream(innerOutputIndex, Stream.NULLS);
        if (nulls != null) {
            wrapped.put(Stream.NULLS, allocator.adopt(allocationContext, nulls));
        }
        Vector errors = tryWrapMultiRunInnerBooleanStream(innerOutputIndex, Stream.ERRORS);
        if (errors != null) {
            wrapped.put(Stream.ERRORS, allocator.adopt(allocationContext, errors));
        }

        Streams wrappedStreams = wrapped.build();
        return wrappedStreams.streams().isEmpty() ? null : wrappedStreams;
    }

    private Vector tryWrapMultiRunInnerBooleanStream(int innerOutputIndex, Stream stream)
    {
        if (preparedInnerRunCount <= 1) {
            return null;
        }

        Vector[] segments = new Vector[preparedInnerRunCount];
        int[] dictionaryIds = new int[currentOutputCount];
        int segmentOffset = 0;
        for (int runIndex = 0; runIndex < preparedInnerRunCount; runIndex++) {
            int innerBatchIndex = outputInnerRunBatchIndexes[runIndex];
            BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(innerBatchIndex);
            int positionStart = outputInnerRunStarts[runIndex];
            int positionCount = outputInnerRunLengths[runIndex];

            Vector source;
            if (!innerBatch.retained()) {
                Streams output = innerBatch.columns()[innerOutputIndex];
                if (output == null || !output.has(stream)) {
                    return null;
                }
                source = output.get(stream);
                for (int index = 0; index < positionCount; index++) {
                    dictionaryIds[positionStart + index] = segmentOffset + outputInnerLogicalPositions[positionStart + index];
                }
            }
            else {
                constrainRetainedInnerBatch(innerBatchIndex, innerBatch, outputInnerRunUniqueStarts[runIndex], outputInnerRunUniqueCounts[runIndex]);
                Output output = innerBatch.retainedBatch().output(innerOutputIndex);
                if (!output.has(stream)) {
                    return null;
                }
                source = output.borrow(stream);
                for (int index = 0; index < positionCount; index++) {
                    dictionaryIds[positionStart + index] = segmentOffset + outputInnerSourcePositions[positionStart + index];
                }
            }

            segments[runIndex] = source;
            segmentOffset += source.length();
        }

        return DictionaryVector.wrap(dictionaryIds, new ConcatenatedBooleanVector(segments));
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
        if (innerBatch.deferred() || !innerBatch.retained()) {
            // Deferred batches must materialize lazily by constraining the source to the matched
            // positions and re-borrowing; non-retained (compacted) batches are flat-copied so the
            // inner VALUES are a dense vector indexable directly by output position. Both fall
            // through to the per-position copy path (copyInnerPositions): for deferred batches it
            // constrains the retained batch before borrowing (keeping projected payloads deferred);
            // for non-retained batches it flat-copies without any constrain. Genuinely retained
            // batches keep the dictionary-wrap shortcut below.
            return null;
        }

        return tryWrapRetainedInnerOutput(innerOutputIndex, innerBatch);
    }

    /**
     * Emits a single-run, non-retained build column's VALUES as a {@link DictionaryVector} over a
     * unified per-build-column dictionary, rather than flattening the (variable-width) bytes once per
     * matched output row. Restricted to the common, safe shape: the whole output batch draws from one
     * build batch ({@code preparedInnerRunCount == 1}), the build column is values-only with a
     * {@link BinaryVector} payload, and there are no NO-MATCH rows. Any other shape (multiple runs,
     * retained batch, null/error side streams, non-binary payload) returns {@code null} so the caller
     * falls back to the existing flatten path. Grouping downstream still settles equality by value, so
     * the unified dictionary only changes representation, never which rows group together.
     */
    private Vector tryWrapNonRetainedDictionaryValues(int innerOutputIndex)
    {
        if (currentOutputCount == 0 || preparedInnerRunCount != 1) {
            return null;
        }
        int innerBatchIndex = outputInnerRunBatchIndexes[0];
        BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(innerBatchIndex);
        if (innerBatch.retained()) {
            return null;
        }
        Streams column = innerBatch.columns()[innerOutputIndex];
        if (column == null || !column.hasValues() || !(column.values() instanceof BinaryVector binarySource)) {
            return null;
        }

        BuildDictionary dictionary = buildDictionaryFor(innerBatchIndex, innerOutputIndex, binarySource, innerBatch.length());
        if (dictionary == NOT_DICTIONARY) {
            return null;
        }
        int[] sourceIdByPosition = dictionary.idByPosition();
        int[] valueIds = new int[currentOutputCount];
        for (int index = 0; index < currentOutputCount; index++) {
            valueIds[index] = sourceIdByPosition[outputInnerLogicalPositions[index]];
        }
        return DictionaryVector.wrap(valueIds, dictionary.values());
    }

    private BuildDictionary buildDictionaryFor(int innerBatchIndex, int innerOutputIndex, BinaryVector source, int length)
    {
        int key = innerBatchIndex * Math.max(1, innerOutputCount) + innerOutputIndex;
        BuildDictionary cached = buildDictionaries.get(key);
        if (cached != null) {
            return cached;
        }

        // Scan only the build batch's valid row range: the buffered BinaryVector may be pre-sized with
        // stale trailing offsets beyond the real rows, which are never referenced by a logical position.
        int[] idByPosition = new int[length];
        // Deduplicate distinct byte values into a single dictionary. This only pays off when the column
        // is low cardinality: a near-unique build column (e.g. a natural key carried straight to the
        // output and never grouped) gains nothing from dictionary grouping while the dedup scan and the
        // per-output-row id remap are pure overhead. Abandon as soon as the distinct count shows the
        // column is high cardinality and cache a NOT_DICTIONARY marker so the caller flattens normally.
        int distinctLimit = Math.max(16, length / 2);
        Map<BinaryValue, Integer> distinct = new HashMap<>();
        java.util.List<Integer> distinctPositions = new java.util.ArrayList<>();
        long totalBytes = 0;
        for (int position = 0; position < length; position++) {
            BinaryValue value = new BinaryValue(source.copyBytes(position));
            Integer id = distinct.get(value);
            if (id == null) {
                if (distinctPositions.size() >= distinctLimit) {
                    buildDictionaries.put(key, NOT_DICTIONARY);
                    return NOT_DICTIONARY;
                }
                id = distinctPositions.size();
                distinct.put(value, id);
                distinctPositions.add(position);
                totalBytes += value.bytes().length;
            }
            idByPosition[position] = id;
        }

        if (totalBytes > Integer.MAX_VALUE) {
            buildDictionaries.put(key, NOT_DICTIONARY);
            return NOT_DICTIONARY;
        }
        BinaryVector values = BinaryVector.allocate(allocator, allocationContext, distinctPositions.size(), (int) totalBytes);
        Arrays.fill(values.offsets(), 0);
        values.clearTraits();
        values.addTraits(source.traits());
        for (int id = 0; id < distinctPositions.size(); id++) {
            values.setBytes(id, source.copyBytes(distinctPositions.get(id)));
        }
        BuildDictionary dictionary = new BuildDictionary(idByPosition, values);
        buildDictionaries.put(key, dictionary);
        return dictionary;
    }

    private static final BuildDictionary NOT_DICTIONARY = new BuildDictionary(new int[0], null);

    private record BuildDictionary(int[] idByPosition, Vector values) {}

    private record BinaryValue(byte[] bytes)
    {
        @Override
        public boolean equals(Object other)
        {
            return other instanceof BinaryValue value && Arrays.equals(bytes, value.bytes);
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode(bytes);
        }
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
        int[] wrappedPositions = Arrays.copyOf(outputInnerSourcePositions, currentOutputCount);
        if (output.hasValues()) {
            wrapped.put(Stream.VALUES, allocator.adopt(allocationContext, wrapComposedDictionary(Arrays.copyOf(wrappedPositions, wrappedPositions.length), output.borrow(Stream.VALUES))));
        }
        if (output.hasNulls()) {
            wrapped.put(Stream.NULLS, allocator.adopt(allocationContext, DictionaryVector.wrap(Arrays.copyOf(wrappedPositions, wrappedPositions.length), output.borrow(Stream.NULLS))));
        }
        if (output.hasErrors()) {
            wrapped.put(Stream.ERRORS, allocator.adopt(allocationContext, DictionaryVector.wrap(Arrays.copyOf(wrappedPositions, wrappedPositions.length), output.borrow(Stream.ERRORS))));
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
        while (true) {
            if (baseValues instanceof DictionaryVector dictionary) {
                int[] baseIds = dictionary.ids();
                for (int index = 0; index < dictionaryIds.length; index++) {
                    dictionaryIds[index] = baseIds[dictionaryIds[index]];
                }
                baseValues = dictionary.values();
                continue;
            }

            if (baseValues instanceof org.weakref.nitro.data.RleVector rle) {
                for (int index = 0; index < dictionaryIds.length; index++) {
                    dictionaryIds[index] = rle.runIndex(dictionaryIds[index]);
                }
                baseValues = rle.values();
                continue;
            }

            break;
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
        return copyInnerPositions(existing, innerBatch, runIndex, innerBatchIndex, innerOutputIndex, positionStart, positionCount, size, true, true, true);
    }

    private Streams copyInnerPositions(Streams existing, BufferedJoinInput.InnerBatch innerBatch, int runIndex, int innerBatchIndex, int innerOutputIndex, int positionStart, int positionCount, int size, boolean includeValues, boolean includeNulls, boolean includeErrors)
    {
        if (!includeValues && !includeNulls && !includeErrors) {
            return existing;
        }
        if (!innerBatch.retained()) {
            Streams input = selectedStreams(innerBatch.columns()[innerOutputIndex], includeValues, includeNulls, includeErrors);
            if (input.streams().isEmpty()) {
                return existing;
            }
            return buffers.copyPositionsFresh(existing, input, outputInnerLogicalPositions, positionStart, positionCount, positionStart, size);
        }

        constrainRetainedInnerBatch(innerBatchIndex, innerBatch, outputInnerRunUniqueStarts[runIndex], outputInnerRunUniqueCounts[runIndex]);
        Output output = innerBatch.retainedBatch().output(innerOutputIndex);
        Output selected = selectedStreams(output, includeValues, includeNulls, includeErrors);
        if (selected == null) {
            return existing;
        }
        return buffers.copyPositionsFresh(selected, existing, outputInnerSourcePositions, positionStart, positionCount, positionStart, size);
    }

    private static Streams selectedStreams(Streams input, boolean includeValues, boolean includeNulls, boolean includeErrors)
    {
        Streams.Builder selected = Streams.builder();
        if (includeValues && input.hasValues()) {
            selected.put(Stream.VALUES, input.values());
        }
        if (includeNulls && input.hasNulls()) {
            selected.put(Stream.NULLS, input.get(Stream.NULLS));
        }
        if (includeErrors && input.hasErrors()) {
            selected.put(Stream.ERRORS, input.get(Stream.ERRORS));
        }
        return selected.build();
    }

    private static Output selectedStreams(Output input, boolean includeValues, boolean includeNulls, boolean includeErrors)
    {
        Set<Stream> selected = EnumSet.noneOf(Stream.class);
        if (includeValues && input.hasValues()) {
            selected.add(Stream.VALUES);
        }
        if (includeNulls && input.hasNulls()) {
            selected.add(Stream.NULLS);
        }
        if (includeErrors && input.hasErrors()) {
            selected.add(Stream.ERRORS);
        }
        if (selected.isEmpty()) {
            return null;
        }
        return input.select(selected);
    }

    private Streams copyInnerSinglePosition(Streams existing, BufferedJoinInput.InnerBatch innerBatch, int innerBatchIndex, int innerOutputIndex, int size, int outputPosition, int logicalPosition, boolean exposeNulls)
    {
        if (!innerBatch.retained()) {
            return withSyntheticNulls(existing, buffers.copySinglePositionFresh(existing, innerBatch.columns()[innerOutputIndex], size, outputPosition, logicalPosition), size, outputPosition, exposeNulls);
        }

        int sourcePosition = innerBatch.sourcePosition(logicalPosition);
        constrainRetainedInnerBatch(innerBatchIndex, innerBatch, logicalPosition);
        return withSyntheticNulls(existing, buffers.copySinglePositionFresh(innerBatch.retainedBatch().output(innerOutputIndex), existing, size, outputPosition, sourcePosition), size, outputPosition, exposeNulls);
    }

    private Streams copyNullInnerPosition(Streams existing, Streams schema, int size, int outputPosition)
    {
        Streams.Builder builder = Streams.builder();
        builder.put(Stream.VALUES, existing == null ? nullValuesLike(schema.values(), size) : existing.values());
        builder.put(Stream.NULLS, setBooleanPosition(existing == null ? null : existing.getOrNull(Stream.NULLS), size, outputPosition, true));
        if (schema.has(Stream.ERRORS)) {
            builder.put(Stream.ERRORS, setBooleanPosition(existing == null ? null : existing.getOrNull(Stream.ERRORS), size, outputPosition, false));
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
        return streams;
    }

    private Set<Stream> resultKnownAllFalseStreams(int outputIndex, Set<Stream> streams)
    {
        if (streams.isEmpty()) {
            return Set.of();
        }

        if (outputIndex < outerOutputCount) {
            Output sourceOutput = currentOuterBatch.output(outputIndex);
            EnumSet<Stream> known = EnumSet.noneOf(Stream.class);
            for (Stream stream : streams) {
                if (sourceOutput.isKnownAllFalse(stream)) {
                    known.add(stream);
                }
            }
            return known.isEmpty() ? Set.of() : Set.copyOf(known);
        }

        int innerOutputIndex = outputIndex - outerOutputCount;
        EnumSet<Stream> known = EnumSet.noneOf(Stream.class);
        if (streams.contains(Stream.NULLS) && innerOutputKnownAllFalseNulls(innerOutputIndex)) {
            known.add(Stream.NULLS);
        }
        if (streams.contains(Stream.ERRORS) && bufferedInner.outputKnownAllFalse(innerOutputIndex, Stream.ERRORS)) {
            known.add(Stream.ERRORS);
        }
        return known.isEmpty() ? Set.of() : Set.copyOf(known);
    }

    private boolean innerOutputKnownAllFalseNulls(int innerOutputIndex)
    {
        Set<Stream> streams = innerOutputStreams(innerOutputIndex);
        if (streams == null || !streams.contains(Stream.NULLS)) {
            return false;
        }
        if (hasNoMatchRows()) {
            return false;
        }

        Set<Stream> sourceStreams = bufferedInner.outputStreams(innerOutputIndex);
        boolean sourceExposesNulls = sourceStreams != null && sourceStreams.contains(Stream.NULLS);
        if (!sourceExposesNulls) {
            return probeOuterJoin;
        }
        return bufferedInner.outputKnownAllFalse(innerOutputIndex, Stream.NULLS);
    }

    private BooleanVector allFalseBooleanStream(int size)
    {
        return allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new);
    }

    private BooleanVector setBooleanPosition(Vector existing, int size, int outputPosition, boolean value)
    {
        BooleanVector vector;
        if (existing instanceof BooleanVector booleanVector) {
            vector = allocator.allocateOrGrow(allocationContext, booleanVector, BooleanVector.class, size, BooleanVector::new);
        }
        else {
            vector = allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new);
            if (existing != null) {
                VectorAccess.BooleanValues existingValues = VectorAccess.booleanValues(existing);
                int existingLength = Math.min(existing.length(), size);
                for (int position = 0; position < existingLength; position++) {
                    vector.values()[position] = existingValues.value(position);
                }
            }
        }
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

        /**
         * Batched probe entry point. Looks up matches for {@code positionCount} outer rows listed in
         * {@code positions}, writing results into {@code matches} (which may reuse the supplied
         * {@code singleMatches} reusable wrappers for single-row matches).
         *
         * <p>Implementations should override this to hoist any Vector type dispatch once per call
         * rather than paying it per position. The default implementation delegates to the
         * per-position {@link #matches} / {@link #matchesNoNulls} entry points and exists so that
         * join-index implementations that have not yet been batch-aware continue to work — the
         * operator always calls this method.
         */
        default void matchRows(Vector[] values, Vector[] nulls, boolean hasNulls, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                LongList result = hasNulls ? matches(values, nulls, position) : matchesNoNulls(values, position);
                if (result instanceof SingleLongList single) {
                    matches[index] = singleMatches[index].withValue(single.getLong(0));
                }
                else {
                    matches[index] = result;
                }
            }
        }
    }

    private static final class LongJoinIndex
            implements JoinIndex
    {
        private static final float LOAD_FACTOR = 0.75f;
        private static final int EMPTY = -1;

        // Open-addressing table of distinct keys; a slot is occupied iff slotHead[slot] != EMPTY.
        private long[] keys;
        private int[] slotHead;
        private int[] slotTail;
        private int[] slotCount;
        // Build rows, indexed by a dense ordinal: rowReferences[o] with chainNext[o] linking each key's
        // rows in insertion (FIFO) order. One flat int[] chain replaces a per-key growable list.
        private long[] rowReferences;
        private int[] chainNext;
        private int rowCount;
        private int mask;
        private int maxFill;
        private int size;
        private final SingleLongList singleMatch = new SingleLongList();
        private final ChainLongList scalarChain = new ChainLongList();
        private final ChainLongList[] chainMatches = createChainLongLists(BATCH_SIZE);

        private LongJoinIndex(int expectedSize)
        {
            int capacity = 16;
            while (capacity < expectedSize / LOAD_FACTOR) {
                capacity <<= 1;
            }
            keys = new long[capacity];
            slotHead = new int[capacity];
            Arrays.fill(slotHead, EMPTY);
            slotTail = new int[capacity];
            slotCount = new int[capacity];
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
            int initialRows = Math.max(16, expectedSize);
            rowReferences = new long[initialRows];
            chainNext = new int[initialRows];
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
            return rowsForSlot(findSlot(OperatorVectorSupport.longValue(values[0], position)), singleMatch, scalarChain);
        }

        @Override
        public void matchRows(Vector[] valuesArray, Vector[] nullsArray, boolean hasNulls, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            Vector values = valuesArray[0];
            Vector nulls = nullsArray == null ? null : nullsArray[0];
            VectorAccess.BooleanValues nullValues = VectorAccess.booleanValues(nulls);
            switch (values) {
                case org.weakref.nitro.data.I64Vector longValues -> matchLongRows(longValues.values(), nullValues, positions, positionCount, matches, singleMatches);
                case org.weakref.nitro.data.I32Vector intValues -> matchIntRows(intValues.values(), nullValues, positions, positionCount, matches, singleMatches);
                case DictionaryVector dictionary -> matchDictionaryRows(dictionary, nullValues, positions, positionCount, matches, singleMatches);
                case org.weakref.nitro.data.RleVector rle -> matchRleRows(rle, nullValues, positions, positionCount, matches, singleMatches);
                default -> {
                    VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            matches[index] = LongLists.emptyList();
                        }
                        else {
                            matches[index] = rowsForSlot(findSlot(rowValues.value(position)), singleMatches[index], chainMatches[index]);
                        }
                    }
                }
            }
        }

        private int findSlot(long key)
        {
            int index = mix(key) & mask;
            while (true) {
                if (slotHead[index] == EMPTY || keys[index] == key) {
                    return index;
                }
                index = (index + 1) & mask;
            }
        }

        private void rehash()
        {
            long[] previousKeys = keys;
            int[] previousHead = slotHead;
            int[] previousTail = slotTail;
            int[] previousCount = slotCount;
            int capacity = previousKeys.length * 2;

            keys = new long[capacity];
            slotHead = new int[capacity];
            Arrays.fill(slotHead, EMPTY);
            slotTail = new int[capacity];
            slotCount = new int[capacity];
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);

            for (int index = 0; index < previousKeys.length; index++) {
                if (previousHead[index] == EMPTY) {
                    continue;
                }
                int newIndex = findSlot(previousKeys[index]);
                keys[newIndex] = previousKeys[index];
                slotHead[newIndex] = previousHead[index];
                slotTail[newIndex] = previousTail[index];
                slotCount[newIndex] = previousCount[index];
            }
        }

        private void addRow(long key, long rowReference)
        {
            int slot = findSlot(key);
            boolean newKey = slotHead[slot] == EMPTY;
            if (rowCount == rowReferences.length) {
                int newCapacity = rowReferences.length * 2;
                rowReferences = Arrays.copyOf(rowReferences, newCapacity);
                chainNext = Arrays.copyOf(chainNext, newCapacity);
            }
            int ordinal = rowCount++;
            rowReferences[ordinal] = rowReference;
            chainNext[ordinal] = EMPTY;
            if (newKey) {
                keys[slot] = key;
                slotHead[slot] = ordinal;
                slotTail[slot] = ordinal;
                slotCount[slot] = 1;
                size++;
                // Rehash after the slot is populated so it carries a non-empty head into the new table.
                if (size >= maxFill) {
                    rehash();
                }
                return;
            }
            // Append at the tail to preserve insertion (FIFO) order within a key.
            chainNext[slotTail[slot]] = ordinal;
            slotTail[slot] = ordinal;
            slotCount[slot]++;
        }

        private LongList rowsForSlot(int slot, SingleLongList single, ChainLongList chain)
        {
            int head = slotHead[slot];
            if (head == EMPTY) {
                return LongLists.emptyList();
            }
            int count = slotCount[slot];
            if (count == 1) {
                return single.withValue(rowReferences[head]);
            }
            return chain.reset(rowReferences, chainNext, head, count);
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

        private void matchLongRows(long[] values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (nullValues.value(position)) {
                    matches[index] = LongLists.emptyList();
                }
                else {
                    matches[index] = rowsForSlot(findSlot(values[position]), singleMatches[index], chainMatches[index]);
                }
            }
        }

        private void matchIntRows(int[] values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (nullValues.value(position)) {
                    matches[index] = LongLists.emptyList();
                }
                else {
                    matches[index] = rowsForSlot(findSlot(values[position]), singleMatches[index], chainMatches[index]);
                }
            }
        }

        private void matchDictionaryRows(DictionaryVector values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            int[] ids = values.ids();
            switch (values.values()) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] dictionaryValues = longValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            matches[index] = LongLists.emptyList();
                        }
                        else {
                            matches[index] = rowsForSlot(findSlot(dictionaryValues[ids[position]]), singleMatches[index], chainMatches[index]);
                        }
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] dictionaryValues = intValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            matches[index] = LongLists.emptyList();
                        }
                        else {
                            matches[index] = rowsForSlot(findSlot(dictionaryValues[ids[position]]), singleMatches[index], chainMatches[index]);
                        }
                    }
                }
                default -> {
                    VectorAccess.LongValues dictionaryValues = VectorAccess.longValues(values.values());
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            matches[index] = LongLists.emptyList();
                        }
                        else {
                            matches[index] = rowsForSlot(findSlot(dictionaryValues.value(ids[position])), singleMatches[index], chainMatches[index]);
                        }
                    }
                }
            }
        }

        private void matchRleRows(org.weakref.nitro.data.RleVector values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (nullValues.value(position)) {
                    matches[index] = LongLists.emptyList();
                }
                else {
                    matches[index] = rowsForSlot(findSlot(rowValues.value(position)), singleMatches[index], chainMatches[index]);
                }
            }
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
            boolean hasNull = false;
            for (Vector nullsVector : nulls) {
                if (OperatorVectorSupport.isNull(nullsVector, position)) {
                    hasNull = true;
                    break;
                }
            }
            return hasNull;
        }
    }

    private static final class LongPairJoinIndex
            implements JoinIndex
    {
        private static final float LOAD_FACTOR = 0.75f;
        // Entries are stored in a single interleaved long[] — {firstKey, secondKey, singleRow} per
        // slot — so each probe step touches a contiguous 24-byte range and fits in one cache line.
        // The previous layout used three separate long[] (firstKeys, secondKeys, singleRows) which
        // forced three independent cache-line loads per probe step on large tables (Q80's
        // store_sales ⨝ store_returns table holds ~2.87M entries, well past L3).
        private static final int ENTRY_STRIDE = 3;
        private static final int FIRST_KEY_OFFSET = 0;
        private static final int SECOND_KEY_OFFSET = 1;
        private static final int SINGLE_ROW_OFFSET = 2;

        private long[] entries;
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
            entries = allocateEntries(capacity);
            rowsBySlot = new LongArrayList[capacity];
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
        }

        private static long[] allocateEntries(int capacity)
        {
            long[] array = new long[capacity * ENTRY_STRIDE];
            for (int slot = 0; slot < capacity; slot++) {
                array[slot * ENTRY_STRIDE + SINGLE_ROW_OFFSET] = NO_MATCH_ROW_REFERENCE;
            }
            return array;
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
            int base = slot * ENTRY_STRIDE;
            long singleRow = entries[base + SINGLE_ROW_OFFSET];
            if (singleRow == NO_MATCH_ROW_REFERENCE || entries[base + FIRST_KEY_OFFSET] != first || entries[base + SECOND_KEY_OFFSET] != second) {
                return LongLists.emptyList();
            }
            LongArrayList rows = rowsBySlot[slot];
            if (rows != null) {
                return rows;
            }
            return singleMatch.withValue(singleRow);
        }

        @Override
        public void matchRows(Vector[] values, Vector[] nulls, boolean hasNulls, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            VectorAccess.LongValues firstValues = VectorAccess.longValues(values[0]);
            VectorAccess.LongValues secondValues = VectorAccess.longValues(values[1]);
            long[] table = entries;
            if (!hasNulls) {
                for (int index = 0; index < positionCount; index++) {
                    int position = positions[index];
                    long first = firstValues.value(position);
                    long second = secondValues.value(position);
                    int slot = findSlot(first, second);
                    int base = slot * ENTRY_STRIDE;
                    long singleRow = table[base + SINGLE_ROW_OFFSET];
                    if (singleRow == NO_MATCH_ROW_REFERENCE || table[base + FIRST_KEY_OFFSET] != first || table[base + SECOND_KEY_OFFSET] != second) {
                        matches[index] = LongLists.emptyList();
                        continue;
                    }
                    LongArrayList rows = rowsBySlot[slot];
                    if (rows != null) {
                        matches[index] = rows;
                    }
                    else {
                        matches[index] = singleMatches[index].withValue(singleRow);
                    }
                }
                return;
            }
            VectorAccess.BooleanValues firstNulls = VectorAccess.booleanValues(nulls[0]);
            VectorAccess.BooleanValues secondNulls = VectorAccess.booleanValues(nulls[1]);
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (firstNulls.value(position) || secondNulls.value(position)) {
                    matches[index] = LongLists.emptyList();
                    continue;
                }
                long first = firstValues.value(position);
                long second = secondValues.value(position);
                int slot = findSlot(first, second);
                int base = slot * ENTRY_STRIDE;
                long singleRow = table[base + SINGLE_ROW_OFFSET];
                if (singleRow == NO_MATCH_ROW_REFERENCE || table[base + FIRST_KEY_OFFSET] != first || table[base + SECOND_KEY_OFFSET] != second) {
                    matches[index] = LongLists.emptyList();
                    continue;
                }
                LongArrayList rows = rowsBySlot[slot];
                if (rows != null) {
                    matches[index] = rows;
                }
                else {
                    matches[index] = singleMatches[index].withValue(singleRow);
                }
            }
        }

        private int findSlot(long first, long second)
        {
            long[] table = entries;
            int slot = mix(first, second) & mask;
            while (true) {
                int base = slot * ENTRY_STRIDE;
                long singleRow = table[base + SINGLE_ROW_OFFSET];
                if (singleRow == NO_MATCH_ROW_REFERENCE) {
                    return slot;
                }
                if (table[base + FIRST_KEY_OFFSET] == first && table[base + SECOND_KEY_OFFSET] == second) {
                    return slot;
                }
                slot = (slot + 1) & mask;
            }
        }

        private void rehash()
        {
            long[] previousEntries = entries;
            LongArrayList[] previousRowsBySlot = rowsBySlot;
            int previousCapacity = previousRowsBySlot.length;
            int capacity = previousCapacity * 2;

            entries = allocateEntries(capacity);
            rowsBySlot = new LongArrayList[capacity];
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
            size = 0;

            for (int oldSlot = 0; oldSlot < previousCapacity; oldSlot++) {
                int previousBase = oldSlot * ENTRY_STRIDE;
                long singleRow = previousEntries[previousBase + SINGLE_ROW_OFFSET];
                if (singleRow == NO_MATCH_ROW_REFERENCE) {
                    continue;
                }
                long first = previousEntries[previousBase + FIRST_KEY_OFFSET];
                long second = previousEntries[previousBase + SECOND_KEY_OFFSET];
                int slot = findSlot(first, second);
                int base = slot * ENTRY_STRIDE;
                entries[base + FIRST_KEY_OFFSET] = first;
                entries[base + SECOND_KEY_OFFSET] = second;
                entries[base + SINGLE_ROW_OFFSET] = singleRow;
                rowsBySlot[slot] = previousRowsBySlot[oldSlot];
                size++;
            }
        }

        private void addRow(long first, long second, long rowReference)
        {
            int slot = findSlot(first, second);
            int base = slot * ENTRY_STRIDE;
            if (entries[base + SINGLE_ROW_OFFSET] == NO_MATCH_ROW_REFERENCE) {
                entries[base + FIRST_KEY_OFFSET] = first;
                entries[base + SECOND_KEY_OFFSET] = second;
                entries[base + SINGLE_ROW_OFFSET] = rowReference;
                size++;
                if (size >= maxFill) {
                    rehash();
                }
                return;
            }
            if (rowsBySlot[slot] == null) {
                LongArrayList rows = new LongArrayList(2);
                rows.add(entries[base + SINGLE_ROW_OFFSET]);
                rows.add(rowReference);
                rowsBySlot[slot] = rows;
                return;
            }
            rowsBySlot[slot].add(rowReference);
        }

        private boolean isEmptySlot(int slot)
        {
            return entries[slot * ENTRY_STRIDE + SINGLE_ROW_OFFSET] == NO_MATCH_ROW_REFERENCE;
        }

        private static int mix(long first, long second)
        {
            // Fibonacci-prime combine + Murmur3 64-bit finalizer. See GroupingState.LongPairGroupingTable.mix
            // for rationale; TPC-DS surrogate keys have zero upper 32 bits and collide heavily under the
            // former `31 * Long.hashCode(a) + Long.hashCode(b)` hash.
            long hash = first * 0x9E3779B97F4A7C15L + second * 0xC4CEB9FE1A85EC53L;
            hash ^= hash >>> 33;
            hash *= 0xFF51AFD7ED558CCDL;
            hash ^= hash >>> 33;
            hash *= 0xC4CEB9FE1A85EC53L;
            hash ^= hash >>> 33;
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

        @Override
        public void matchRows(Vector[] values, Vector[] nulls, boolean hasNulls, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            VectorAccess.LongValues firstValues = VectorAccess.longValues(values[0]);
            VectorAccess.LongValues secondValues = VectorAccess.longValues(values[1]);
            VectorAccess.LongValues thirdValues = VectorAccess.longValues(values[2]);
            if (!hasNulls) {
                for (int index = 0; index < positionCount; index++) {
                    int position = positions[index];
                    long first = firstValues.value(position);
                    long second = secondValues.value(position);
                    long third = thirdValues.value(position);
                    int slot = findSlot(first, second, third);
                    if (isEmptySlot(slot) || firstKeys[slot] != first || secondKeys[slot] != second || thirdKeys[slot] != third) {
                        matches[index] = LongLists.emptyList();
                        continue;
                    }
                    LongArrayList rows = rowsBySlot[slot];
                    if (rows != null) {
                        matches[index] = rows;
                    }
                    else {
                        matches[index] = singleMatches[index].withValue(singleRows[slot]);
                    }
                }
                return;
            }
            VectorAccess.BooleanValues firstNulls = VectorAccess.booleanValues(nulls[0]);
            VectorAccess.BooleanValues secondNulls = VectorAccess.booleanValues(nulls[1]);
            VectorAccess.BooleanValues thirdNulls = VectorAccess.booleanValues(nulls[2]);
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (firstNulls.value(position) || secondNulls.value(position) || thirdNulls.value(position)) {
                    matches[index] = LongLists.emptyList();
                    continue;
                }
                long first = firstValues.value(position);
                long second = secondValues.value(position);
                long third = thirdValues.value(position);
                int slot = findSlot(first, second, third);
                if (isEmptySlot(slot) || firstKeys[slot] != first || secondKeys[slot] != second || thirdKeys[slot] != third) {
                    matches[index] = LongLists.emptyList();
                    continue;
                }
                LongArrayList rows = rowsBySlot[slot];
                if (rows != null) {
                    matches[index] = rows;
                }
                else {
                    matches[index] = singleMatches[index].withValue(singleRows[slot]);
                }
            }
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
            long hash = first * 0x9E3779B97F4A7C15L + second * 0xC4CEB9FE1A85EC53L + third * 0x94D049BB133111EBL;
            hash ^= hash >>> 33;
            hash *= 0xFF51AFD7ED558CCDL;
            hash ^= hash >>> 33;
            hash *= 0xC4CEB9FE1A85EC53L;
            hash ^= hash >>> 33;
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

    /**
     * Reusable view over one key's build rows, threaded through a shared chain ({@code next}) starting at
     * {@code head}. Reading is cursor-cached so the sequential {@code getLong(0..size-1)} access the join
     * output loop performs is O(1) per element; out-of-order access falls back to a walk from the head.
     */
    private static final class ChainLongList
            extends AbstractLongList
    {
        private long[] rows;
        private int[] next;
        private int head;
        private int length;
        private int cursorIndex;
        private int cursorOrdinal;

        public ChainLongList reset(long[] rows, int[] next, int head, int length)
        {
            this.rows = rows;
            this.next = next;
            this.head = head;
            this.length = length;
            this.cursorIndex = 0;
            this.cursorOrdinal = head;
            return this;
        }

        @Override
        public long getLong(int index)
        {
            if (index < 0 || index >= length) {
                throw new IndexOutOfBoundsException("index " + index);
            }
            if (index < cursorIndex) {
                cursorIndex = 0;
                cursorOrdinal = head;
            }
            while (cursorIndex < index) {
                cursorOrdinal = next[cursorOrdinal];
                cursorIndex++;
            }
            return rows[cursorOrdinal];
        }

        @Override
        public int size()
        {
            return length;
        }
    }

    private static ChainLongList[] createChainLongLists(int size)
    {
        ChainLongList[] matches = new ChainLongList[size];
        for (int index = 0; index < size; index++) {
            matches[index] = new ChainLongList();
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
