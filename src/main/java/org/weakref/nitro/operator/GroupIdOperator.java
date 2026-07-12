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
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Set;

public final class GroupIdOperator
        implements Operator
{
    private static final boolean SHARE_DENSE_DICTIONARY_IDS =
            Boolean.parseBoolean(System.getProperty("nitro.groupId.shareDenseDictionaryIds", "true"));
    private static final boolean USE_KNOWN_FALSE_METADATA =
            Boolean.parseBoolean(System.getProperty("nitro.groupId.useKnownFalseMetadata", "true"));
    private static final boolean COMPACT_ALL_FALSE_STREAMS =
            Boolean.parseBoolean(System.getProperty("nitro.groupId.compactAllFalseStreams", "true"));
    private final Allocator allocator;
    private final Allocator.Context allocationContext = new Allocator.Context("GroupIdOperator");
    private final Operator source;
    private final int[][] groupingSetInputs;
    private final boolean[] outputCanBeNullExtended;
    private int[] currentSourcePositions;

    private Batch currentSourceBatch;
    private Mask currentSourceMask;
    private boolean currentSourceDense;
    private int currentGroupingSet;
    private Batch stagedBatch;
    private boolean done;

    public GroupIdOperator(Allocator allocator, Operator source, int[][] groupingSetInputs)
    {
        this.allocator = allocator;
        this.source = source;
        this.groupingSetInputs = copyGroupingSetInputs(groupingSetInputs);
        this.outputCanBeNullExtended = computeOutputNullExtension(groupingSetInputs);
    }

    @Override
    public int outputCount()
    {
        return groupingSetInputs[0].length + 1;
    }

    @Override
    public boolean hasNext()
    {
        if (stagedBatch == null && !done) {
            stagedBatch = loadNextBatch();
        }
        return stagedBatch != null;
    }

    @Override
    public Batch next()
    {
        if (!hasNext()) {
            throw new IllegalStateException("No more batches");
        }
        Batch result = stagedBatch;
        stagedBatch = null;
        return result;
    }

    @Override
    public void constrain(Mask mask)
    {
    }

    @Override
    public void close()
    {
        if (currentSourceBatch != null) {
            currentSourceBatch.close();
            currentSourceBatch = null;
        }
        source.close();
        allocator.release(allocationContext);
    }

    private Batch loadNextBatch()
    {
        while (true) {
            if (currentSourceBatch == null) {
                if (!loadNextSourceBatch()) {
                    done = true;
                    return null;
                }
            }

            if (currentGroupingSet >= groupingSetInputs.length) {
                currentSourceBatch.close();
                currentSourceBatch = null;
                currentSourceMask = null;
                currentSourceDense = false;
                currentSourcePositions = null;
                currentGroupingSet = 0;
                continue;
            }

            return materializeGroupingSetBatch(currentGroupingSet++);
        }
    }

    private boolean loadNextSourceBatch()
    {
        while (source.hasNext()) {
            Batch batch = source.next();
            Mask mask = batch.borrowMask();
            if (mask.none()) {
                batch.close();
                continue;
            }
            currentSourceBatch = batch;
            currentSourceMask = mask;
            currentSourceDense = mask.all() && mask.count() == mask.size();
            currentSourcePositions = materializedPositions(mask);
            currentGroupingSet = 0;
            return true;
        }
        return false;
    }

    private Batch materializeGroupingSetBatch(int groupingSetIndex)
    {
        int rowCount = currentSourceMask.count();
        Output[] outputs = new Output[outputCount()];
        int[] groupingSet = groupingSetInputs[groupingSetIndex];

        for (int outputIndex = 0; outputIndex < groupingSet.length; outputIndex++) {
            outputs[outputIndex] = resultOutput(materializeOutput(outputIndex, groupingSet[outputIndex], rowCount));
        }
        outputs[groupingSet.length] = resultOutput(groupIdStreams(groupingSetIndex, rowCount));

        return new Batch(
                allocator.allocateRangeMask(allocationContext, 0, rowCount),
                takenMask -> allocator.transfer(allocationContext, takenMask),
                outputs);
    }

    private Streams materializeOutput(int outputIndex, int sourceIndex, int rowCount)
    {
        Output sourceOutput = currentSourceBatch.output(sourceIndex >= 0 ? sourceIndex : outputIndex);
        Vector values = selectValues(sourceOutput.borrow(Stream.VALUES), currentSourcePositions);

        Streams.Builder streams = Streams.builder()
                .put(Stream.VALUES, values);

        if (sourceIndex >= 0) {
            if (sourceOutput.streams().contains(Stream.NULLS)) {
                if (USE_KNOWN_FALSE_METADATA && sourceOutput.isKnownAllFalse(Stream.NULLS)) {
                    if (outputCanBeNullExtended[outputIndex]) {
                        streams.put(Stream.NULLS, allFalseVector(rowCount));
                    }
                }
                else {
                    streams.put(Stream.NULLS, copySelectedVector(sourceOutput.borrow(Stream.NULLS)));
                }
            }
            else if (outputCanBeNullExtended[outputIndex]) {
                streams.put(Stream.NULLS, allFalseVector(rowCount));
            }

            if (sourceOutput.streams().contains(Stream.ERRORS)) {
                streams.put(Stream.ERRORS,
                        USE_KNOWN_FALSE_METADATA && sourceOutput.isKnownAllFalse(Stream.ERRORS)
                                ? allFalseVector(rowCount)
                                : copySelectedVector(sourceOutput.borrow(Stream.ERRORS)));
            }
            return streams.build();
        }

        streams.put(Stream.NULLS, booleanVector(rowCount, true));
        if (sourceOutput.streams().contains(Stream.ERRORS)) {
            streams.put(Stream.ERRORS, allFalseVector(rowCount));
        }
        return streams.build();
    }

    /**
     * Selects {@code positions} out of {@code source} for one grouping-set expansion. A
     * dictionary-encoded column is carried through by reference: only its id array is remapped to the
     * selected positions while the underlying dictionary values are shared across every grouping set.
     * This avoids copying the (often variable-width) values N times for an N-way rollup and, crucially,
     * keeps the same dictionary instance flowing into grouping so {@link FlatGroupingTable} can hash
     * by id rather than by raw bytes. All other shapes fall back to a dense copy.
     */
    private Vector selectValues(Vector source, int[] positions)
    {
        if (source instanceof DictionaryVector dictionary) {
            if (SHARE_DENSE_DICTIONARY_IDS && currentSourceDense) {
                // The source batch stays open until every grouping-set output derived from it has been consumed,
                // so a dense expansion can safely share its immutable dictionary mapping.  This is the same
                // BufferPtr-style lifetime used for the dictionary values and avoids allocating/copying the ids
                // once per grouping set merely to reproduce an identity selection.
                return allocator.adopt(allocationContext, DictionaryVector.wrap(dictionary.ids(), dictionary.length(), dictionary.values()));
            }
            int[] sourceIds = dictionary.ids();
            int[] ids = new int[positions.length];
            for (int index = 0; index < positions.length; index++) {
                ids[index] = sourceIds[positions[index]];
            }
            return allocator.allocateDictionary(allocationContext, ids, dictionary.values());
        }
        return copySelectedVector(source);
    }

    private Vector copySelectedVector(Vector source)
    {
        return allocator.copyVector(allocationContext, source, currentSourcePositions);
    }

    private static int[] materializedPositions(Mask mask)
    {
        int[] positions = new int[mask.count()];
        for (int index = 0; index < positions.length; index++) {
            positions[index] = mask.position(index);
        }
        return positions;
    }

    private Streams groupIdStreams(int groupingSetIndex, int rowCount)
    {
        I64Vector values = allocator.allocate(allocationContext, I64Vector.class, rowCount, I64Vector::new);
        long[] groupIds = values.values();
        for (int position = 0; position < rowCount; position++) {
            groupIds[position] = groupingSetIndex;
        }
        return Streams.ofValues(values);
    }

    private Output resultOutput(Streams streams)
    {
        Set<Stream> exposedStreams = streams.streams();
        return new Output(exposedStreams, streams::get, (stream, vector) -> allocator.transfer(allocationContext, vector));
    }

    private BooleanVector booleanVector(int size, boolean value)
    {
        BooleanVector vector = allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new);
        if (value) {
            boolean[] values = vector.values();
            for (int position = 0; position < size; position++) {
                values[position] = true;
            }
        }
        return vector;
    }

    private Vector allFalseVector(int size)
    {
        if (!COMPACT_ALL_FALSE_STREAMS) {
            return booleanVector(size, false);
        }
        BooleanVector sentinel = allocator.allocate(allocationContext, BooleanVector.class, 1, BooleanVector::new);
        sentinel.markAllFalse();
        return allocator.allocateSingleRunRle(allocationContext, size, sentinel);
    }

    private static int[][] copyGroupingSetInputs(int[][] groupingSetInputs)
    {
        if (groupingSetInputs.length == 0) {
            throw new IllegalArgumentException("groupingSetInputs is empty");
        }

        int outputCount = groupingSetInputs[0].length;
        int[][] result = new int[groupingSetInputs.length][outputCount];
        for (int groupingSetIndex = 0; groupingSetIndex < groupingSetInputs.length; groupingSetIndex++) {
            if (groupingSetInputs[groupingSetIndex].length != outputCount) {
                throw new IllegalArgumentException("Grouping set output counts must match");
            }
            System.arraycopy(groupingSetInputs[groupingSetIndex], 0, result[groupingSetIndex], 0, outputCount);
        }
        return result;
    }

    private static boolean[] computeOutputNullExtension(int[][] groupingSetInputs)
    {
        int outputCount = groupingSetInputs[0].length;
        boolean[] result = new boolean[outputCount];
        for (int outputIndex = 0; outputIndex < outputCount; outputIndex++) {
            for (int[] groupingSet : groupingSetInputs) {
                if (groupingSet[outputIndex] < 0) {
                    result[outputIndex] = true;
                    break;
                }
            }
        }
        return result;
    }
}
