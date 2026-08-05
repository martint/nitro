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

import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.SelectedPositions;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

final class BufferedJoinInput
{
    @FunctionalInterface
    interface BatchMaskPruner
    {
        void prune(Batch batch, Mask mask);
    }

    private static final int VALUES_FLAG = 1;
    private static final int NULLS_FLAG = 1 << 1;
    private static final int ERRORS_FLAG = 1 << 2;

    private final BufferedJoinInputPolicy policy;
    private final JoinBufferSupport buffers;
    private final PrimitiveArrayPool arrayPool;
    private final int columnCount;
    private final Streams[] schema;
    private final java.util.Set<Stream>[] outputStreams;
    private final int[] outputKnownAllFalseFlags;
    private final boolean[] outputKnownAllFalseInitialized;
    private final List<InnerBatch> batches = new ArrayList<>();
    // Coalesced vectors may preserve dictionary/value storage from their source batches. Keep those owners alive
    // until the join closes, while removing them from the active probe list.
    private final List<InnerBatch> coalescedSources = new ArrayList<>();
    private Batch firstRetainedBatch;
    private int[] densePositionsCache = new int[0];
    private final PositionBuffer compactionPositions;
    private final JoinBufferSupport.PositionMappingCache compactionMappings;

    private boolean loaded;
    private boolean sharedNonRetainedBatches;
    private long rowCount;
    private boolean directExactCoalesce;
    private boolean directBoundedCoalesce;

    @SuppressWarnings("unchecked")
    BufferedJoinInput(BufferedJoinInputPolicy policy, JoinBufferSupport buffers, int columnCount)
    {
        this.policy = requireNonNull(policy, "policy is null");
        this.buffers = buffers;
        this.arrayPool = buffers.primitiveArrays();
        this.compactionPositions = new PositionBuffer(arrayPool);
        this.columnCount = columnCount;
        this.schema = new Streams[columnCount];
        this.outputStreams = (java.util.Set<Stream>[]) new java.util.Set<?>[columnCount];
        this.outputKnownAllFalseFlags = new int[columnCount];
        this.outputKnownAllFalseInitialized = new boolean[columnCount];
        this.compactionMappings = buffers.newRecyclingPositionMappingCache();
        this.directExactCoalesce = policy.directExactCoalesce();
    }

    public void loadAll(Operator source, int batchSize)
    {
        loadAll(source, batchSize, new int[0], false, false);
    }

    /**
     * Initializes this probe-local view from a completed build owner's immutable compacted payload.
     *
     * <p>Only non-retained batches are shareable: their streams are detached, dense, and never constrained during
     * probe materialization. Retained and deferred batches contain mutable batch selections and must continue to be
     * recreated independently for each probe session.
     */
    boolean shareLoadedNonRetainedBatches(BufferedJoinInput owner)
    {
        if (loaded) {
            throw new IllegalStateException("Build input is already loaded");
        }
        if (!owner.loaded || owner.batches.stream().anyMatch(InnerBatch::retained)) {
            return false;
        }
        loaded = true;
        sharedNonRetainedBatches = true;
        rowCount = owner.rowCount;
        batches.addAll(owner.batches);
        System.arraycopy(owner.schema, 0, schema, 0, columnCount);
        System.arraycopy(owner.outputStreams, 0, outputStreams, 0, columnCount);
        System.arraycopy(owner.outputKnownAllFalseFlags, 0, outputKnownAllFalseFlags, 0, columnCount);
        System.arraycopy(owner.outputKnownAllFalseInitialized, 0, outputKnownAllFalseInitialized, 0, columnCount);
        return true;
    }

    void enableDirectExactCoalesce()
    {
        if (loaded) {
            throw new IllegalStateException("Build input is already loaded");
        }
        directExactCoalesce = true;
    }

    boolean enableDirectBoundedCoalesce()
    {
        if (loaded) {
            throw new IllegalStateException("Build input is already loaded");
        }
        directBoundedCoalesce = policy.directBoundedCoalesce();
        return directBoundedCoalesce;
    }

    public void loadAll(Operator source, int batchSize, int[] eagerColumns, boolean retainBatches)
    {
        loadAll(source, batchSize, eagerColumns, retainBatches, false);
    }

    /**
     * Loads the source's batches.
     *
     * @param deferSingleBatch when {@code true} and a non-retained source yields exactly one batch,
     *     the batch is kept as a <em>deferred</em> {@link InnerBatch} so that non-key payload columns
     *     are materialized lazily by constraining the batch to the matched source positions and
     *     re-borrowing, rather than eagerly compacting every column at load time. Only set this when
     *     the source reports {@link Operator#supportsConstrainedReborrow()}.
     */
    public void loadAll(Operator source, int batchSize, int[] eagerColumns, boolean retainBatches, boolean deferSingleBatch)
    {
        loadAll(source, batchSize, eagerColumns, retainBatches, deferSingleBatch, null);
    }

    public void loadAll(
            Operator source,
            int batchSize,
            int[] eagerColumns,
            boolean retainBatches,
            boolean deferSingleBatch,
            BatchMaskPruner maskPruner)
    {
        if (loaded) {
            return;
        }
        loaded = true;

        if (retainBatches) {
            loadRetained(source, maskPruner);
            coalesceSmallBatches();
            return;
        }

        Streams[] columns = new Streams[columnCount];
        int outputPosition = 0;
        long exactRows = source.exactOutputRows();
        int outputBatchSize;
        boolean automaticExactCandidate = policy.automaticDirectExactCoalesce() &&
                exactRows >= policy.minAutomaticDirectExactRows() &&
                exactRows <= policy.maxPostLoadCoalescedRows();
        if (directExactCoalesce && exactRows > 0 && exactRows <= policy.maxCoalescedRows()) {
            outputBatchSize = toIntExact(exactRows);
        }
        else if (directBoundedCoalesce) {
            outputBatchSize = policy.maxCoalescedRows();
        }
        else {
            outputBatchSize = batchSize;
        }

        boolean first = true;
        while (source.hasNext()) {
            Batch batch = source.next();
            captureStreams(batch, outputStreams);
            captureKnownAllFalse(batch, outputKnownAllFalseFlags, outputKnownAllFalseInitialized);
            Mask mask = batch.borrowMask();
            if (maskPruner != null) {
                maskPruner.prune(batch, mask);
            }

            // Single-batch defer fast path: when the source yields exactly one batch and supports a
            // constrained re-borrow, retain it as a deferred batch. Build borrows only the join-key
            // columns; non-key payload columns are materialized lazily (constrain + re-borrow) only
            // when a downstream output stream is actually requested. Genuinely retained sources keep
            // using loadRetained above; this path serves sources whose batch does not outlive an
            // advance but can still satisfy a constrained re-borrow before the next advance.
            if (deferSingleBatch && first && !source.hasNext() && batches.isEmpty() && outputPosition == 0) {
                // Do not capture a representative VALUES schema here: borrowing a payload column's
                // VALUES would force the very materialization we are trying to defer. The schema is
                // derived lazily from the retained batch in outputSchema() if an empty/null result
                // ever needs it.
                if (firstRetainedBatch == null) {
                    firstRetainedBatch = batch;
                }
                if (!mask.none()) {
                    int[] positions = positions(mask);
                    rowCount += positions.length;
                    batches.add(InnerBatch.deferred(batch, positions));
                }
                coalesceSmallBatches();
                return;
            }
            if (first && automaticExactCandidate && hasVariableWidthMajority(batch)) {
                outputBatchSize = toIntExact(exactRows);
            }
            first = false;
            captureSchema(batch, schema);
            int maskOffset = 0;
            while (maskOffset < mask.count()) {
                int copied = Math.min(mask.count() - maskOffset, outputBatchSize - outputPosition);
                compactionPositions.reset();
                for (int columnIndex = 0; columnIndex < columns.length; columnIndex++) {
                    if (compactionMappings != null) {
                        compactionMappings.reset();
                    }
                    columns[columnIndex] = buffers.copyAndCompact(
                            batch.output(columnIndex),
                            mask,
                            maskOffset,
                            columns[columnIndex],
                            outputPosition,
                            copied,
                            outputBatchSize,
                            policy.sharedCompactionPositions() ? compactionPositions : null,
                            policy.recycledCompactionMappings() ? compactionMappings : null);
                }
                outputPosition += copied;
                maskOffset += copied;
                rowCount += copied;

                if (outputPosition == outputBatchSize) {
                    batches.add(new InnerBatch(columns, outputBatchSize));
                    columns = new Streams[columnCount];
                    outputPosition = 0;
                }
            }
            // Every selected stream and position has been copied into this buffer. Close the non-retained wrapper
            // now so operators above the scan can recycle their masks and borrowed streams before the next batch.
            // Retained and deferred paths deliberately keep their batches open and do not reach this point.
            if (policy.closeCopiedBatches()) {
                batch.close();
            }
        }

        if (outputPosition > 0) {
            batches.add(new InnerBatch(columns, outputPosition));
        }

        coalesceSmallBatches();
    }

    private boolean hasVariableWidthMajority(Batch batch)
    {
        int valueColumns = 0;
        int variableWidthColumns = 0;
        for (int outputIndex = 0; outputIndex < columnCount; outputIndex++) {
            Output output = batch.output(outputIndex);
            if (!output.hasValues()) {
                continue;
            }
            valueColumns++;
            if (output.borrow(Stream.VALUES).isVariableWidth()) {
                variableWidthColumns++;
            }
        }
        return variableWidthColumns * 2 > valueColumns;
    }

    private void loadRetained(Operator source, BatchMaskPruner maskPruner)
    {
        while (source.hasNext()) {
            Batch batch = source.next();
            if (firstRetainedBatch == null) {
                firstRetainedBatch = batch;
            }
            captureSchema(batch, schema);
            captureStreams(batch, outputStreams);
            captureKnownAllFalse(batch, outputKnownAllFalseFlags, outputKnownAllFalseInitialized);
            Mask mask = batch.borrowMask();
            if (maskPruner != null) {
                maskPruner.prune(batch, mask);
            }
            if (mask.none()) {
                continue;
            }
            if (mask.all()) {
                rowCount += mask.count();
                batches.add(InnerBatch.retained(batch, mask.count()));
                continue;
            }
            if (!mask.all()) {
                Streams[] columns = new Streams[columnCount];
                compactionPositions.reset();
                for (int columnIndex = 0; columnIndex < columns.length; columnIndex++) {
                    if (compactionMappings != null) {
                        compactionMappings.reset();
                    }
                    columns[columnIndex] = buffers.copyAndCompact(
                            batch.output(columnIndex),
                            mask,
                            0,
                            null,
                            0,
                            mask.count(),
                            mask.count(),
                            policy.sharedCompactionPositions() ? compactionPositions : null,
                            policy.recycledCompactionMappings() ? compactionMappings : null);
                }
                rowCount += mask.count();
                batches.add(new InnerBatch(columns, mask.count()));
                continue;
            }
            int[] positions = positions(mask);
            rowCount += positions.length;
            batches.add(InnerBatch.retained(batch, positions));
        }
    }

    private void coalesceSmallBatches()
    {
        if (batches.size() <= 1 || rowCount == 0 || rowCount > policy.maxPostLoadCoalescedRows()) {
            return;
        }

        int size = toIntExact(rowCount);
        Streams[] columns = new Streams[columnCount];
        int outputStart = 0;
        for (InnerBatch batch : batches) {
            SelectedPositions sourceRange = batch.retained() ? null : SelectedPositions.range(0, batch.length());
            int[] sourcePositions = null;
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                if (batch.retained()) {
                    int[] positions = batch.positions() == null ? densePositions(batch.length()) : batch.positions();
                    columns[columnIndex] = buffers.copyPositions(batch.retainedBatch().output(columnIndex), columns[columnIndex], positions, batch.length(), outputStart, size);
                }
                else if (policy.coalesceRangeSelection() && buffers.canCopyRangeWithoutMaterializing(batch.columns()[columnIndex])) {
                    columns[columnIndex] = buffers.copyPositions(columns[columnIndex], batch.columns()[columnIndex], sourceRange, outputStart, size);
                }
                else {
                    sourcePositions = sourcePositions != null ? sourcePositions : densePositions(batch.length());
                    columns[columnIndex] = buffers.copyPositions(columns[columnIndex], batch.columns()[columnIndex], sourcePositions, batch.length(), outputStart, size);
                }
            }
            outputStart += batch.length();
        }
        java.util.Set<org.weakref.nitro.data.Vector> retained = policy.releaseCopiedCoalesceSources() ? buffers.identities(columns) : null;
        for (InnerBatch batch : batches) {
            if (retained != null && !batch.retained()) {
                buffers.releaseUnreferenced(batch.columns(), retained);
                batch.releasePositions(arrayPool);
            }
            else {
                coalescedSources.add(batch);
            }
        }
        batches.clear();
        batches.add(new InnerBatch(columns, size));
    }

    public List<InnerBatch> batches()
    {
        return batches;
    }

    public Streams[] schema()
    {
        return schema;
    }

    public long rowCount()
    {
        return rowCount;
    }

    public java.util.Set<Stream> outputStreams(int outputIndex)
    {
        return outputStreams[outputIndex];
    }

    public boolean outputKnownAllFalse(int outputIndex, Stream stream)
    {
        return (outputKnownAllFalseFlags[outputIndex] & streamFlag(stream)) != 0;
    }

    public Streams outputSchema(int outputIndex)
    {
        Streams outputSchema = schema[outputIndex];
        if (outputSchema != null) {
            return outputSchema;
        }
        for (InnerBatch batch : batches) {
            if (!batch.retained() && batch.columns()[outputIndex] != null) {
                schema[outputIndex] = batch.columns()[outputIndex];
                return schema[outputIndex];
            }
        }
        if (firstRetainedBatch == null) {
            return null;
        }
        Output output = firstRetainedBatch.output(outputIndex);
        Streams.Builder streams = Streams.builder();
        if (output.hasValues()) {
            streams.put(Stream.VALUES, output.borrow(Stream.VALUES));
        }
        if (output.hasNulls()) {
            streams.put(Stream.NULLS, new BooleanVector(0));
        }
        if (output.hasErrors()) {
            streams.put(Stream.ERRORS, new BooleanVector(0));
        }
        schema[outputIndex] = streams.build();
        return schema[outputIndex];
    }

    public static void captureSchema(Batch batch, Streams[] schema)
    {
        for (int outputIndex = 0; outputIndex < schema.length; outputIndex++) {
            if (schema[outputIndex] != null) {
                continue;
            }
            Output output = batch.output(outputIndex);
            Streams.Builder streams = Streams.builder();
            try {
                if (output.hasValues()) {
                    streams.put(Stream.VALUES, output.borrow(Stream.VALUES));
                }
                if (output.hasNulls()) {
                    streams.put(Stream.NULLS, new BooleanVector(0));
                }
                if (output.hasErrors()) {
                    streams.put(Stream.ERRORS, new BooleanVector(0));
                }
                schema[outputIndex] = streams.build();
            }
            catch (IllegalArgumentException ignored) {
                // Some projected outputs cannot yield a representative VALUES stream at
                // schema-capture time. Later buffered rows or retained batches can still
                // establish the schema when the join needs it.
            }
        }
    }

    private static void captureStreams(Batch batch, java.util.Set<Stream>[] outputStreams)
    {
        for (int outputIndex = 0; outputIndex < outputStreams.length; outputIndex++) {
            if (outputStreams[outputIndex] == null) {
                outputStreams[outputIndex] = batch.output(outputIndex).streams();
            }
        }
    }

    private static void captureKnownAllFalse(Batch batch, int[] outputKnownAllFalseFlags, boolean[] outputKnownAllFalseInitialized)
    {
        for (int outputIndex = 0; outputIndex < outputKnownAllFalseFlags.length; outputIndex++) {
            Output output = batch.output(outputIndex);
            int flags = streamFlags(output.knownAllFalseStreams());
            if (!outputKnownAllFalseInitialized[outputIndex]) {
                outputKnownAllFalseFlags[outputIndex] = flags;
                outputKnownAllFalseInitialized[outputIndex] = true;
            }
            else {
                outputKnownAllFalseFlags[outputIndex] &= flags;
            }
        }
    }

    private int[] positions(Mask mask)
    {
        int[] positions = arrayPool.borrowInts(mask.count());
        if (mask.all()) {
            for (int index = 0; index < positions.length; index++) {
                positions[index] = index;
            }
            return positions;
        }
        for (int index = 0; index < positions.length; index++) {
            positions[index] = mask.position(index);
        }
        return positions;
    }

    private int[] densePositions(int length)
    {
        if (!policy.bufferedDensePositionsCache()) {
            int[] positions = new int[length];
            for (int index = 0; index < length; index++) {
                positions[index] = index;
            }
            return positions;
        }
        if (densePositionsCache.length < length) {
            int[] previous = densePositionsCache;
            densePositionsCache = arrayPool.borrowInts(length);
            for (int index = 0; index < length; index++) {
                densePositionsCache[index] = index;
            }
            arrayPool.release(previous);
        }
        return densePositionsCache;
    }

    public void releaseBuffers()
    {
        if (!sharedNonRetainedBatches) {
            for (InnerBatch batch : batches) {
                batch.releasePositions(arrayPool);
            }
        }
        batches.clear();
        for (InnerBatch batch : coalescedSources) {
            batch.releasePositions(arrayPool);
        }
        coalescedSources.clear();
        // Retained source vectors can escape through dictionary-preserving join outputs. Their downstream owner,
        // rather than the join's scratch teardown, controls when those batches can be closed.
        firstRetainedBatch = null;
        arrayPool.release(densePositionsCache);
        densePositionsCache = new int[0];
        compactionPositions.release();
        if (compactionMappings != null) {
            compactionMappings.release();
        }
    }

    private static int streamFlags(java.util.Set<Stream> streams)
    {
        int flags = 0;
        if (streams.contains(Stream.VALUES)) {
            flags |= VALUES_FLAG;
        }
        if (streams.contains(Stream.NULLS)) {
            flags |= NULLS_FLAG;
        }
        if (streams.contains(Stream.ERRORS)) {
            flags |= ERRORS_FLAG;
        }
        return flags;
    }

    private static int streamFlag(Stream stream)
    {
        return switch (stream) {
            case VALUES -> VALUES_FLAG;
            case NULLS -> NULLS_FLAG;
            case ERRORS -> ERRORS_FLAG;
        };
    }

    static final class InnerBatch
    {
        private final Streams[] columns;
        private final int length;
        private final Batch retainedBatch;
        private final int[] positions;

        InnerBatch(Streams[] columns, int length)
        {
            this(columns, length, null, null);
        }

        private final boolean deferred;

        private InnerBatch(Streams[] columns, int length, Batch retainedBatch, int[] positions)
        {
            this(columns, length, retainedBatch, positions, false);
        }

        private InnerBatch(Streams[] columns, int length, Batch retainedBatch, int[] positions, boolean deferred)
        {
            this.columns = columns;
            this.length = length;
            this.retainedBatch = retainedBatch;
            this.positions = positions;
            this.deferred = deferred;
        }

        public static InnerBatch retained(Batch batch, int[] positions)
        {
            return new InnerBatch(null, positions.length, batch, positions);
        }

        public static InnerBatch retained(Batch batch, int length)
        {
            return new InnerBatch(null, length, batch, null);
        }

        public static InnerBatch deferred(Batch batch, int[] positions)
        {
            return new InnerBatch(null, positions.length, batch, positions, true);
        }

        public Streams[] columns()
        {
            return columns;
        }

        public int length()
        {
            return length;
        }

        public boolean retained()
        {
            return retainedBatch != null;
        }

        public boolean deferred()
        {
            return deferred;
        }

        public Batch retainedBatch()
        {
            return retainedBatch;
        }

        public int sourcePosition(int position)
        {
            return positions == null ? position : positions[position];
        }

        public int[] positions()
        {
            return positions;
        }

        private void releasePositions(PrimitiveArrayPool arrayPool)
        {
            arrayPool.release(positions);
        }
    }
}
