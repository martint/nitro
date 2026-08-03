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

import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public final class GroupIdOperator
        implements Operator
{
    private final Allocator allocator;
    private final Allocator.Context allocationContext = new Allocator.Context("GroupIdOperator");
    private final Operator source;
    private final int[][] groupingSetInputs;
    private final int[] representativeSourceInputs;
    private final GroupIdOperatorPolicy policy;
    private final boolean[] outputCanBeNullExtended;
    private final Schema outputSchema;
    private final DictionaryVector[] currentDenseDictionaryMappings;
    private int[] currentSourcePositions;

    private Batch currentSourceBatch;
    private Mask currentSourceMask;
    private boolean currentSourceDense;
    private int currentGroupingSet;
    private Batch stagedBatch;

    public GroupIdOperator(Allocator allocator, Operator source, int[][] groupingSetInputs, GroupIdOperatorPolicy policy)
    {
        this(
                allocator,
                source,
                groupingSetInputs,
                new Field(Schema.unspecified(1).field(0).type(), false),
                policy);
    }

    public GroupIdOperator(
            Allocator allocator,
            Operator source,
            int[][] groupingSetInputs,
            Field groupIdField,
            GroupIdOperatorPolicy policy)
    {
        this.allocator = allocator;
        this.source = source;
        this.groupingSetInputs = copyGroupingSetInputs(groupingSetInputs);
        this.representativeSourceInputs = representativeSourceInputs(this.groupingSetInputs);
        this.policy = requireNonNull(policy, "policy is null");
        this.outputCanBeNullExtended = computeOutputNullExtension(this.groupingSetInputs);
        this.outputSchema = outputSchema(source.outputSchema(), this.groupingSetInputs, outputCanBeNullExtended, groupIdField);
        this.currentDenseDictionaryMappings = new DictionaryVector[outputCanBeNullExtended.length];
    }

    @Override
    public int outputCount()
    {
        return outputSchema.size();
    }

    @Override
    public Schema outputSchema()
    {
        return outputSchema;
    }

    @Override
    public boolean hasNext()
    {
        if (stagedBatch == null) {
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
        clearDenseDictionaryMappings();
        source.close();
        allocator.release(allocationContext);
    }

    private Batch loadNextBatch()
    {
        while (true) {
            if (currentSourceBatch == null) {
                if (!loadNextSourceBatch()) {
                    return null;
                }
            }

            if (currentGroupingSet >= groupingSetInputs.length) {
                currentSourceBatch.close();
                currentSourceBatch = null;
                currentSourceMask = null;
                currentSourceDense = false;
                currentSourcePositions = null;
                clearDenseDictionaryMappings();
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
                _ -> {},
                takenMask -> allocator.transfer(allocationContext, takenMask),
                releasedMask -> allocator.release(allocationContext, releasedMask),
                () -> {},
                outputs);
    }

    private Streams materializeOutput(int outputIndex, int sourceIndex, int rowCount)
    {
        Output sourceOutput = currentSourceBatch.output(sourceIndex >= 0 ? sourceIndex : representativeSourceInputs[outputIndex]);
        Vector values = selectValues(sourceOutput.borrow(Stream.VALUES), currentSourcePositions, outputIndex);

        Streams.Builder streams = Streams.builder()
                .put(Stream.VALUES, values);

        if (sourceIndex >= 0) {
            if (sourceOutput.streams().contains(Stream.NULLS)) {
                if (policy.useKnownFalseMetadata() && sourceOutput.isKnownAllFalse(Stream.NULLS)) {
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
                        policy.useKnownFalseMetadata() && sourceOutput.isKnownAllFalse(Stream.ERRORS)
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
     * Dense flat inputs use the same identity-dictionary view. This avoids copying values N times for an
     * N-way rollup and, crucially, keeps the same mapping identity flowing into grouping so
     * {@link FlatGroupingTable} can reuse dictionary-derived work.
     */
    private Vector selectValues(Vector source, int[] positions, int outputIndex)
    {
        if (source instanceof DictionaryVector dictionary) {
            if (policy.shareDenseDictionaryIds() && currentSourceDense) {
                // The source batch stays open until every grouping-set output derived from it has been consumed,
                // so a dense expansion can safely share its immutable dictionary mapping.  This is the same
                // BufferPtr-style lifetime used for the dictionary values and avoids allocating/copying the ids
                // once per grouping set merely to reproduce an identity selection.
                DictionaryVector selected;
                if (shouldPropagateMappingIdentity()) {
                    selected = currentDenseDictionaryMappings[outputIndex];
                    if (selected == null) {
                        selected = DictionaryVector.wrap(dictionary.ids(), dictionary.length(), dictionary.values());
                        currentDenseDictionaryMappings[outputIndex] = selected;
                    }
                    selected = selected.sharedMappingView();
                }
                else {
                    selected = DictionaryVector.wrap(dictionary.ids(), dictionary.length(), dictionary.values());
                }
                return allocator.adopt(allocationContext, selected);
            }
            int[] sourceIds = dictionary.ids();
            int[] ids = new int[positions.length];
            for (int index = 0; index < positions.length; index++) {
                ids[index] = sourceIds[positions[index]];
            }
            return allocator.allocateDictionary(allocationContext, ids, dictionary.values());
        }
        if (policy.shareDenseDictionaryIds() && currentSourceDense) {
            DictionaryVector selected;
            if (shouldPropagateMappingIdentity()) {
                selected = currentDenseDictionaryMappings[outputIndex];
                if (selected == null) {
                    selected = DictionaryVector.wrap(currentSourcePositions, currentSourcePositions.length, source);
                    currentDenseDictionaryMappings[outputIndex] = selected;
                }
                selected = selected.sharedMappingView();
            }
            else {
                selected = DictionaryVector.wrap(currentSourcePositions, currentSourcePositions.length, source);
            }
            return allocator.adopt(allocationContext, selected);
        }
        return copySelectedVector(source);
    }

    private void clearDenseDictionaryMappings()
    {
        for (int index = 0; index < currentDenseDictionaryMappings.length; index++) {
            currentDenseDictionaryMappings[index] = null;
        }
    }

    private boolean shouldPropagateMappingIdentity()
    {
        return policy.propagateDenseDictionaryMappingIdentity() &&
                groupingSetInputs.length >= policy.denseDictionaryMappingMinGroupingSets();
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
        // Encoded outputs can borrow their value vector from the source batch. Transfer only buffers owned by this
        // operator so taking a dictionary/RLE wrapper does not steal the borrowed child from its upstream owner.
        return new Output(
                exposedStreams,
                streams::get,
                (stream, vector) -> allocator.transferOwned(allocationContext, vector),
                (stream, vector) -> allocator.release(allocationContext, vector));
    }

    private BooleanVector booleanVector(int size, boolean value)
    {
        BooleanVector vector = allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new);
        if (value) {
            vector.markAllTrue();
        }
        return vector;
    }

    private Vector allFalseVector(int size)
    {
        if (!policy.compactAllFalseStreams()) {
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

    private static int[] representativeSourceInputs(int[][] groupingSetInputs)
    {
        int[] result = new int[groupingSetInputs[0].length];
        java.util.Arrays.fill(result, -1);
        for (int outputIndex = 0; outputIndex < result.length; outputIndex++) {
            for (int[] groupingSet : groupingSetInputs) {
                if (groupingSet[outputIndex] >= 0) {
                    result[outputIndex] = groupingSet[outputIndex];
                    break;
                }
            }
            if (result[outputIndex] < 0) {
                throw new IllegalArgumentException("GroupId output has no source mapping: " + outputIndex);
            }
        }
        return result;
    }

    private static Schema outputSchema(
            Schema sourceSchema,
            int[][] groupingSetInputs,
            boolean[] outputCanBeNullExtended,
            Field groupIdField)
    {
        List<Field> fields = new ArrayList<>(outputCanBeNullExtended.length + 1);
        for (int outputIndex = 0; outputIndex < outputCanBeNullExtended.length; outputIndex++) {
            fields.add(outputField(sourceSchema, groupingSetInputs, outputIndex, outputCanBeNullExtended[outputIndex]));
        }
        fields.add(requireNonNull(groupIdField, "groupIdField is null"));
        return new Schema(fields);
    }

    private static Field outputField(Schema sourceSchema, int[][] groupingSetInputs, int outputIndex, boolean nullExtended)
    {
        Field field = null;
        boolean nullable = nullExtended;
        boolean ambiguous = false;
        for (int[] groupingSet : groupingSetInputs) {
            int sourceIndex = groupingSet[outputIndex];
            if (sourceIndex < 0) {
                continue;
            }
            if (sourceIndex >= sourceSchema.size()) {
                ambiguous = true;
                continue;
            }

            Field mappedField = sourceSchema.field(sourceIndex);
            nullable |= mappedField.nullable();
            if (field == null) {
                field = mappedField;
            }
            else if (!field.name().equals(mappedField.name()) ||
                    !field.type().identity().equals(mappedField.type().identity())) {
                ambiguous = true;
            }
        }

        if (ambiguous) {
            return unspecifiedField(nullable);
        }
        if (field == null) {
            if (outputIndex >= sourceSchema.size()) {
                return unspecifiedField(true);
            }
            field = sourceSchema.field(outputIndex);
            nullable |= field.nullable();
        }
        if (field.nullable() == nullable) {
            return field;
        }
        return new Field(field.name(), field.type(), nullable);
    }

    private static Field unspecifiedField(boolean nullable)
    {
        return new Field(Schema.unspecified(1).field(0).type(), nullable);
    }
}
