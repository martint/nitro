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
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public class SemiJoinOperator
        implements Operator
{
    private final Operator outer;
    private final Operator inner;
    private final int outerJoinColumn;
    private final int innerJoinColumn;
    private final Allocator allocator;
    private final Allocator.Context allocationContext;
    private final boolean includeMatches;
    private final boolean outputMatches;
    private final MembershipSet membership;
    private final PositionScratch selectionScratch;
    private final SemiJoinOperatorPolicy policy;

    private boolean loaded;
    private BatchState currentBatchState;
    private it.unimi.dsi.fastutil.longs.LongOpenHashSet dynamicFilterValues;
    private boolean dynamicFilterAbandoned;
    private SmallBinarySet smallBinaryMembership;
    private boolean smallBinaryMembershipAbandoned;

    public SemiJoinOperator(Allocator allocator, Operator outer, int outerJoinColumn, Operator inner, int innerJoinColumn)
    {
        this(allocator, outer, outerJoinColumn, inner, innerJoinColumn, true);
    }

    public SemiJoinOperator(Allocator allocator, Operator outer, int outerJoinColumn, Operator inner, int innerJoinColumn, boolean includeMatches)
    {
        this(allocator, outer, outerJoinColumn, inner, innerJoinColumn, includeMatches, false);
    }

    public SemiJoinOperator(Allocator allocator, Operator outer, int outerJoinColumn, Operator inner, int innerJoinColumn, boolean includeMatches, boolean outputMatches)
    {
        this(
                allocator,
                outer,
                outerJoinColumn,
                inner,
                innerJoinColumn,
                includeMatches,
                outputMatches,
                allocator.engineResources().operatorResources());
    }

    public SemiJoinOperator(
            Allocator allocator,
            Operator outer,
            int outerJoinColumn,
            Operator inner,
            int innerJoinColumn,
            boolean includeMatches,
            boolean outputMatches,
            OperatorResources operatorResources)
    {
        this.outer = outer;
        this.inner = inner;
        this.outerJoinColumn = outerJoinColumn;
        this.innerJoinColumn = innerJoinColumn;
        this.allocator = allocator;
        this.allocationContext = new Allocator.Context(
                "SemiJoinOperator",
                operatorResources.grouping().semiJoinBufferPool());
        this.selectionScratch = new PositionScratch(allocator.primitiveArrays());
        this.policy = operatorResources.semiJoinPolicy();
        this.includeMatches = includeMatches;
        this.outputMatches = outputMatches;
        this.membership = new MembershipSet(
                allocator,
                allocationContext,
                operatorResources,
                joinType(outer.outputSchema(), outerJoinColumn, inner.outputSchema(), innerJoinColumn));
    }

    private static Optional<TypeBinding> joinType(
            Schema outerSchema,
            int outerJoinColumn,
            Schema innerSchema,
            int innerJoinColumn)
    {
        Optional<TypeBinding> outerType = typeAt(outerSchema, outerJoinColumn);
        Optional<TypeBinding> innerType = typeAt(innerSchema, innerJoinColumn);
        if (outerType.filter(TypeBinding::isSpecified).isPresent() &&
                innerType.filter(TypeBinding::isSpecified).isPresent() &&
                !outerType.orElseThrow().identity().equals(innerType.orElseThrow().identity())) {
            throw new IllegalArgumentException("Semi-join key types do not match");
        }
        return innerType.filter(TypeBinding::isSpecified)
                .or(() -> outerType.filter(TypeBinding::isSpecified))
                .or(() -> innerType)
                .or(() -> outerType);
    }

    private static Optional<TypeBinding> typeAt(Schema schema, int column)
    {
        if (column < 0 || column >= schema.size()) {
            return Optional.empty();
        }
        return Optional.of(schema.field(column).type());
    }

    @Override
    public int outputCount()
    {
        return outer.outputCount() + (outputMatches ? 1 : 0);
    }

    @Override
    public Schema outputSchema()
    {
        Schema outerSchema = outer.outputSchema();
        if (!outputMatches) {
            return outerSchema;
        }
        List<Field> fields = new ArrayList<>(outerSchema.fields());
        fields.add(new Field(Schema.unspecified(1).field(0).type(), false));
        return new Schema(fields);
    }

    @Override
    public boolean hasNext()
    {
        loadInnerIfNecessary();
        return outer.hasNext();
    }

    @Override
    public Batch next()
    {
        loadInnerIfNecessary();

        Batch sourceBatch = outer.next();
        BatchState batchState = new BatchState(sourceBatch, sourceBatch.borrowMask());
        currentBatchState = batchState;

        Mask batchMask = sourceBatch.borrowMask();
        if (!outputMatches) {
            batchMask = selectRows(sourceBatch);
            outer.constrain(batchMask);
            sourceBatch.constrain(batchMask);
            batchState.constrain(batchMask);
        }

        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outer.outputCount(); outputIndex++) {
            Output sourceOutput = sourceBatch.output(outputIndex);
            outputs[outputIndex] = new Output(
                    sourceOutput.streams(),
                    sourceOutput::borrow,
                    (stream, vector) -> sourceOutput.take(stream),
                    (_, _) -> {},
                    sourceOutput::copySinglePosition);
        }
        if (outputMatches) {
            outputs[outer.outputCount()] = new Output(
                    Set.of(Stream.VALUES),
                    stream -> {
                        if (stream != Stream.VALUES) {
                            throw new IllegalArgumentException("Unsupported stream: " + stream);
                        }
                        return batchState.borrowMatchValues(this);
                    },
                    (stream, mask) -> {
                        if (stream != Stream.VALUES) {
                            throw new IllegalArgumentException("Unsupported stream: " + stream);
                        }
                        return batchState.borrowMatchValues(this, mask);
                    },
                    (stream, mask, selectTrue, resultAllocator, resultAllocationContext) -> {
                        if (stream != Stream.VALUES) {
                            throw new IllegalArgumentException("Unsupported stream: " + stream);
                        }
                        return batchState.borrowMatchMask(this, mask, selectTrue, resultAllocator, resultAllocationContext);
                    },
                    (stream, vector) -> vector,
                    (stream, vector) -> allocator.release(allocationContext, vector),
                    null,
                    (existing, sourcePosition, outputPosition, size) -> {
                        BooleanVector matchValues = batchState.borrowMatchValues(this);
                        BooleanVector outputValues = VectorAccess.writableBooleanVector(
                                allocator,
                                allocationContext,
                                existing == null ? null : existing.getOrNull(Stream.VALUES),
                                size);
                        outputValues.values()[outputPosition] = matchValues.values()[sourcePosition];
                        return Streams.ofValues(outputValues);
                    });
        }
        return new Batch(
                batchMask,
                batchState::constrain,
                Function.identity(),
                _ -> {},
                () -> {
                    if (currentBatchState == batchState) {
                        currentBatchState = null;
                    }
                    sourceBatch.close();
                },
                outputs);
    }

    @Override
    public void constrain(Mask mask)
    {
        outer.constrain(mask);
        if (currentBatchState != null) {
            currentBatchState.constrain(mask);
        }
    }

    @Override
    public void pushDynamicFilter(DynamicFilter filter)
    {
        // The semi-join appends only an optional match column after the outer columns, so every outer column index is
        // unchanged. Forward a filter on an outer column down to the outer source (e.g. so a downstream join's
        // dynamic filter reaches the probe-side scan through the semi-join).
        if (filter.column() < outer.outputCount()) {
            outer.pushDynamicFilter(filter);
        }
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        return outer.supportsRetainedBatches();
    }

    @Override
    public boolean supportsConstrainedReborrow()
    {
        // The output adds a semi-join match column relative to the outer input, so it cannot be
        // re-borrowed by source position after a downstream constrain.
        return false;
    }

    @Override
    public void close()
    {
        if (currentBatchState != null) {
            currentBatchState.sourceBatch().close();
            currentBatchState = null;
        }
        outer.close();
        if (!loaded) {
            inner.close();
        }
        membership.releaseBuffers();
        selectionScratch.release();
        if (smallBinaryMembership != null) {
            smallBinaryMembership.releaseBuffers();
            smallBinaryMembership = null;
        }
        allocator.release(allocationContext);
    }

    private void loadInnerIfNecessary()
    {
        if (loaded) {
            return;
        }
        loaded = true;
        while (inner.hasNext()) {
            Batch batch = inner.next();
            try {
                Mask mask = batch.borrowMask();
                if (mask.none()) {
                    continue;
                }
                Output output = batch.output(innerJoinColumn);
                Vector values = output.borrow(Stream.VALUES);
                Vector nulls = output.borrowOrNull(Stream.NULLS);
                collectDynamicFilterValues(values, nulls, mask);
                collectSmallBinaryMembership(values, nulls, mask);
                membership.addBatch(values, nulls, mask);
            }
            finally {
                batch.close();
            }
        }
        inner.close();
        pushDynamicFilterIfReady();
    }

    private void collectDynamicFilterValues(Vector values, Vector nulls, Mask mask)
    {
        if (!policy.dynamicFilterEnabled() || !includeMatches || outputMatches || dynamicFilterAbandoned) {
            return;
        }
        if (nulls != null && !VectorAccess.isAllFalseNulls(nulls)) {
            dynamicFilterAbandoned = true;
            dynamicFilterValues = null;
            return;
        }
        if (dynamicFilterValues == null) {
            dynamicFilterValues = new it.unimi.dsi.fastutil.longs.LongOpenHashSet();
        }
        if (values instanceof I64Vector i64) {
            for (int position : mask) {
                addDynamicFilterValue(i64.values()[position]);
                if (dynamicFilterAbandoned) {
                    return;
                }
            }
            return;
        }
        if (values instanceof DictionaryVector dictionary && dictionary.values() instanceof I64Vector i64) {
            int[] ids = dictionary.ids();
            long[] dictionaryValues = i64.values();
            for (int position : mask) {
                addDynamicFilterValue(dictionaryValues[ids[position]]);
                if (dynamicFilterAbandoned) {
                    return;
                }
            }
            return;
        }
        dynamicFilterAbandoned = true;
        dynamicFilterValues = null;
    }

    private void collectSmallBinaryMembership(Vector values, Vector nulls, Mask mask)
    {
        if (policy.smallBinarySetMaxValues() <= 0 || smallBinaryMembershipAbandoned || mask.none()) {
            return;
        }
        if (!SmallBinarySet.supports(values)) {
            smallBinaryMembershipAbandoned = true;
            if (smallBinaryMembership != null) {
                smallBinaryMembership.releaseBuffers();
            }
            smallBinaryMembership = null;
            return;
        }
        if (smallBinaryMembership == null) {
            smallBinaryMembership = new SmallBinarySet(
                    policy.smallBinarySetMaxValues(),
                    policy.cacheDictionaryMatches(),
                    allocator.primitiveArrays());
        }
        if (!smallBinaryMembership.addValues(values, nulls, mask)) {
            smallBinaryMembershipAbandoned = true;
            smallBinaryMembership.releaseBuffers();
            smallBinaryMembership = null;
        }
    }

    private void addDynamicFilterValue(long value)
    {
        dynamicFilterValues.add(value);
        if (dynamicFilterValues.size() > policy.dynamicFilterMaxValues()) {
            dynamicFilterAbandoned = true;
            dynamicFilterValues = null;
        }
    }

    private void pushDynamicFilterIfReady()
    {
        if (dynamicFilterValues != null && !dynamicFilterValues.isEmpty()) {
            outer.pushDynamicFilter(DynamicFilter.fromValues(outerJoinColumn, dynamicFilterValues));
        }
    }

    private Mask selectRows(Batch sourceBatch)
    {
        return selectRows(sourceBatch, sourceBatch.borrowMask(), includeMatches, allocator, allocationContext);
    }

    private Mask selectRows(Batch sourceBatch, Mask sourceMask, boolean includeMatches, Allocator resultAllocator, Allocator.Context resultAllocationContext)
    {
        if (sourceMask.none()) {
            return resultAllocator.allocateEmptyMask(resultAllocationContext, sourceMask.size());
        }

        Output output = sourceBatch.output(outerJoinColumn);
        Vector values = output.borrow(Stream.VALUES);
        Vector nulls = output.borrowOrNull(Stream.NULLS);

        SmallBinarySet binaryMembership = smallBinaryMembership;
        if (binaryMembership != null && binaryMembership.supportsProbe(values)) {
            return binaryMembership.selectRows(resultAllocator, resultAllocationContext, values, nulls, sourceMask, includeMatches, selectionScratch);
        }

        int[] positions = selectionScratch.ensure(sourceMask.count());
        int selectedCount = membership.selectPositions(values, nulls, sourceMask, includeMatches, positions);

        if (selectedCount == sourceMask.count()) {
            return resultAllocator.copyMask(resultAllocationContext, sourceMask);
        }
        return resultAllocator.allocateSparseMask(resultAllocationContext, positions, selectedCount, sourceMask.size());
    }

    private BooleanVector computeMatchValues(BatchState batchState, BooleanVector matchValues, Mask requestedMask)
    {
        if (requestedMask.none()) {
            return matchValues;
        }

        Output output = batchState.sourceBatch().output(outerJoinColumn);
        Vector values = output.borrow(Stream.VALUES);
        Vector nulls = output.borrowOrNull(Stream.NULLS);
        SmallBinarySet binaryMembership = smallBinaryMembership;
        if (binaryMembership != null && binaryMembership.supportsProbe(values)) {
            binaryMembership.writeMatches(values, nulls, requestedMask, matchValues.values());
            return matchValues;
        }

        membership.writeMatches(values, nulls, requestedMask, matchValues.values());
        return matchValues;
    }

    private static final class PositionScratch
    {
        private final PrimitiveArrayPool arrayPool;
        private int[] positions = new int[0];

        private PositionScratch(PrimitiveArrayPool arrayPool)
        {
            this.arrayPool = arrayPool;
        }

        private int[] ensure(int count)
        {
            if (positions.length < count) {
                int[] previous = positions;
                positions = arrayPool.isRetainable((long) count * Integer.BYTES) ? arrayPool.borrowInts(count) : new int[count];
                if (arrayPool.isRetainable((long) previous.length * Integer.BYTES)) {
                    arrayPool.release(previous);
                }
            }
            return positions;
        }

        private int[] selectedPrefix(Mask mask, int selectedCount)
        {
            int[] result = ensure(mask.count());
            if (mask.all()) {
                for (int index = 0; index < selectedCount; index++) {
                    result[index] = index;
                }
                return result;
            }
            int index = 0;
            for (int position : mask) {
                if (index == selectedCount) {
                    break;
                }
                result[index++] = position;
            }
            return result;
        }

        private void release()
        {
            if (arrayPool.isRetainable((long) positions.length * Integer.BYTES)) {
                arrayPool.release(positions);
            }
            positions = new int[0];
        }
    }

    private static final class SmallBinarySet
    {
        private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

        private final int maxValues;
        private final boolean cacheDictionaryMatches;
        private final PrimitiveArrayPool arrayPool;
        private byte[][] values = new byte[8][];
        private int[] lengths = new int[8];
        private int[] hashes = new int[8];
        private long[] firstWords = new long[8];
        private long[] secondWords = new long[8];
        private int size;

        private boolean[] dictionaryMatches = new boolean[0];
        private int[] dictionaryGenerations = new int[0];
        private int dictionaryGeneration;
        private Vector cachedDictionaryValues;
        private byte[] cachedDictionaryMatchStates = new byte[0];

        private SmallBinarySet(int maxValues, boolean cacheDictionaryMatches, PrimitiveArrayPool arrayPool)
        {
            this.maxValues = maxValues;
            this.cacheDictionaryMatches = cacheDictionaryMatches;
            this.arrayPool = arrayPool;
        }

        private static boolean supports(Vector values)
        {
            return switch (values) {
                case BinaryVector _ -> true;
                case DictionaryVector dictionary -> supports(dictionary.values());
                case RleVector rle -> supports(rle.values());
                default -> false;
            };
        }

        private boolean supportsProbe(Vector values)
        {
            return supports(values);
        }

        private boolean addValues(Vector values, Vector nulls, Mask mask)
        {
            VectorAccess.BooleanValues nullValues = VectorAccess.booleanValues(nulls);
            for (int position : mask) {
                if (!nullValues.value(position) && !addValue(values, position)) {
                    return false;
                }
            }
            return true;
        }

        private boolean addValue(Vector values, int position)
        {
            return switch (values) {
                case BinaryVector binary -> add(binary.data(), binary.startOffset(position), binary.length(position));
                case DictionaryVector dictionary -> addValue(dictionary.values(), dictionary.ids()[position]);
                case RleVector rle -> addValue(rle.values(), rle.runIndex(position));
                default -> false;
            };
        }

        private boolean add(byte[] data, int offset, int length)
        {
            if (contains(data, offset, length)) {
                return true;
            }
            if (size >= maxValues) {
                return false;
            }
            ensureCapacity(size + 1);
            byte[] copy = Arrays.copyOfRange(data, offset, offset + length);
            values[size] = copy;
            lengths[size] = length;
            hashes[size] = OperatorVectorSupport.binaryHash(copy, 0, length);
            firstWords[size] = length <= Long.BYTES ? pack(copy, 0, length) : pack(copy, 0, Long.BYTES);
            secondWords[size] = length <= Long.BYTES ? 0 : pack(copy, Long.BYTES, Math.min(Long.BYTES, length - Long.BYTES));
            size++;
            return true;
        }

        private void ensureCapacity(int capacity)
        {
            if (values.length >= capacity) {
                return;
            }
            int newSize = values.length * 2;
            while (newSize < capacity) {
                newSize *= 2;
            }
            values = Arrays.copyOf(values, newSize);
            lengths = Arrays.copyOf(lengths, newSize);
            hashes = Arrays.copyOf(hashes, newSize);
            firstWords = Arrays.copyOf(firstWords, newSize);
            secondWords = Arrays.copyOf(secondWords, newSize);
        }

        private void writeMatches(Vector values, Vector nulls, Mask mask, boolean[] output)
        {
            if (VectorAccess.isAllFalseNulls(nulls)) {
                writeMatchesNoNulls(values, mask, output);
                return;
            }
            VectorAccess.BooleanValues nullValues = VectorAccess.booleanValues(nulls);
            if (values instanceof DictionaryVector dictionary) {
                writeDictionaryMatches(dictionary, nullValues, mask, output);
                return;
            }
            if (mask.all()) {
                for (int position = 0; position < mask.size(); position++) {
                    output[position] = !nullValues.value(position) && containsValue(values, position);
                }
                return;
            }
            for (int position : mask) {
                output[position] = !nullValues.value(position) && containsValue(values, position);
            }
        }

        private void writeMatchesNoNulls(Vector values, Mask mask, boolean[] output)
        {
            if (values instanceof DictionaryVector dictionary) {
                writeDictionaryMatchesNoNulls(dictionary, mask, output);
                return;
            }
            if (mask.all()) {
                for (int position = 0; position < mask.size(); position++) {
                    output[position] = containsValue(values, position);
                }
                return;
            }
            for (int position : mask) {
                output[position] = containsValue(values, position);
            }
        }

        private void writeDictionaryMatches(DictionaryVector dictionary, VectorAccess.BooleanValues nullValues, Mask mask, boolean[] output)
        {
            int generation = nextDictionaryGeneration();
            int dictionarySize = dictionary.values().length();
            ensureDictionaryScratch(dictionarySize);
            int[] ids = dictionary.ids();
            Vector dictionaryValues = dictionary.values();
            if (mask.all()) {
                for (int position = 0; position < mask.size(); position++) {
                    output[position] = !nullValues.value(position) && dictionaryMatch(dictionaryValues, ids[position], generation);
                }
                return;
            }
            for (int position : mask) {
                output[position] = !nullValues.value(position) && dictionaryMatch(dictionaryValues, ids[position], generation);
            }
        }

        private void writeDictionaryMatchesNoNulls(DictionaryVector dictionary, Mask mask, boolean[] output)
        {
            int generation = nextDictionaryGeneration();
            int dictionarySize = dictionary.values().length();
            ensureDictionaryScratch(dictionarySize);
            int[] ids = dictionary.ids();
            Vector dictionaryValues = dictionary.values();
            if (mask.all()) {
                for (int position = 0; position < mask.size(); position++) {
                    output[position] = dictionaryMatch(dictionaryValues, ids[position], generation);
                }
                return;
            }
            for (int position : mask) {
                output[position] = dictionaryMatch(dictionaryValues, ids[position], generation);
            }
        }

        private Mask selectRows(Allocator allocator, Allocator.Context allocationContext, Vector values, Vector nulls, Mask mask, boolean includeMatches, PositionScratch selectionScratch)
        {
            if (VectorAccess.isAllFalseNulls(nulls)) {
                return selectRowsNoNulls(allocator, allocationContext, values, mask, includeMatches, selectionScratch);
            }
            VectorAccess.BooleanValues nullValues = VectorAccess.booleanValues(nulls);
            int[] positions = null;
            boolean allSelected = true;
            int selectedCount = 0;
            int maskCount = mask.count();
            if (values instanceof DictionaryVector dictionary) {
                int generation = nextDictionaryGeneration();
                int dictionarySize = dictionary.values().length();
                ensureDictionaryScratch(dictionarySize);
                int[] ids = dictionary.ids();
                Vector dictionaryValues = dictionary.values();
                if (mask.all()) {
                    for (int position = 0; position < maskCount; position++) {
                        boolean match = !nullValues.value(position) && dictionaryMatch(dictionaryValues, ids[position], generation);
                        if (match == includeMatches) {
                            if (!allSelected) {
                                positions = selectionScratch.ensure(maskCount);
                                positions[selectedCount] = position;
                            }
                            selectedCount++;
                        }
                        else if (allSelected) {
                            allSelected = false;
                            if (selectedCount > 0) {
                                positions = selectionScratch.selectedPrefix(mask, selectedCount);
                            }
                        }
                    }
                }
                else {
                    int[] maskPositions = mask.selectedPositions();
                    for (int index = 0; index < maskCount; index++) {
                        int position = maskPositions[index];
                        boolean match = !nullValues.value(position) && dictionaryMatch(dictionaryValues, ids[position], generation);
                        if (match == includeMatches) {
                            if (!allSelected) {
                                positions = selectionScratch.ensure(maskCount);
                                positions[selectedCount] = position;
                            }
                            selectedCount++;
                        }
                        else if (allSelected) {
                            allSelected = false;
                            if (selectedCount > 0) {
                                positions = selectionScratch.selectedPrefix(mask, selectedCount);
                            }
                        }
                    }
                }
            }
            else {
                if (mask.all()) {
                    for (int position = 0; position < maskCount; position++) {
                        boolean match = !nullValues.value(position) && containsValue(values, position);
                        if (match == includeMatches) {
                            if (!allSelected) {
                                positions = selectionScratch.ensure(maskCount);
                                positions[selectedCount] = position;
                            }
                            selectedCount++;
                        }
                        else if (allSelected) {
                            allSelected = false;
                            if (selectedCount > 0) {
                                positions = selectionScratch.selectedPrefix(mask, selectedCount);
                            }
                        }
                    }
                }
                else {
                    int[] maskPositions = mask.selectedPositions();
                    for (int index = 0; index < maskCount; index++) {
                        int position = maskPositions[index];
                        boolean match = !nullValues.value(position) && containsValue(values, position);
                        if (match == includeMatches) {
                            if (!allSelected) {
                                positions = selectionScratch.ensure(maskCount);
                                positions[selectedCount] = position;
                            }
                            selectedCount++;
                        }
                        else if (allSelected) {
                            allSelected = false;
                            if (selectedCount > 0) {
                                positions = selectionScratch.selectedPrefix(mask, selectedCount);
                            }
                        }
                    }
                }
            }
            if (allSelected || selectedCount == maskCount) {
                return allocator.copyMask(allocationContext, mask);
            }
            return allocator.allocateSparseMask(allocationContext, positions, selectedCount, mask.size());
        }

        private Mask selectRowsNoNulls(Allocator allocator, Allocator.Context allocationContext, Vector values, Mask mask, boolean includeMatches, PositionScratch selectionScratch)
        {
            int[] positions = null;
            boolean allSelected = true;
            int selectedCount = 0;
            int maskCount = mask.count();
            if (values instanceof DictionaryVector dictionary) {
                int generation = nextDictionaryGeneration();
                int dictionarySize = dictionary.values().length();
                ensureDictionaryScratch(dictionarySize);
                int[] ids = dictionary.ids();
                Vector dictionaryValues = dictionary.values();
                if (mask.all()) {
                    for (int position = 0; position < maskCount; position++) {
                        if (dictionaryMatch(dictionaryValues, ids[position], generation) == includeMatches) {
                            if (!allSelected) {
                                positions = selectionScratch.ensure(maskCount);
                                positions[selectedCount] = position;
                            }
                            selectedCount++;
                        }
                        else if (allSelected) {
                            allSelected = false;
                            if (selectedCount > 0) {
                                positions = selectionScratch.selectedPrefix(mask, selectedCount);
                            }
                        }
                    }
                }
                else {
                    int[] maskPositions = mask.selectedPositions();
                    for (int index = 0; index < maskCount; index++) {
                        int position = maskPositions[index];
                        if (dictionaryMatch(dictionaryValues, ids[position], generation) == includeMatches) {
                            if (!allSelected) {
                                positions = selectionScratch.ensure(maskCount);
                                positions[selectedCount] = position;
                            }
                            selectedCount++;
                        }
                        else if (allSelected) {
                            allSelected = false;
                            if (selectedCount > 0) {
                                positions = selectionScratch.selectedPrefix(mask, selectedCount);
                            }
                        }
                    }
                }
            }
            else {
                if (mask.all()) {
                    for (int position = 0; position < maskCount; position++) {
                        if (containsValue(values, position) == includeMatches) {
                            if (!allSelected) {
                                positions = selectionScratch.ensure(maskCount);
                                positions[selectedCount] = position;
                            }
                            selectedCount++;
                        }
                        else if (allSelected) {
                            allSelected = false;
                            if (selectedCount > 0) {
                                positions = selectionScratch.selectedPrefix(mask, selectedCount);
                            }
                        }
                    }
                }
                else {
                    int[] maskPositions = mask.selectedPositions();
                    for (int index = 0; index < maskCount; index++) {
                        int position = maskPositions[index];
                        if (containsValue(values, position) == includeMatches) {
                            if (!allSelected) {
                                positions = selectionScratch.ensure(maskCount);
                                positions[selectedCount] = position;
                            }
                            selectedCount++;
                        }
                        else if (allSelected) {
                            allSelected = false;
                            if (selectedCount > 0) {
                                positions = selectionScratch.selectedPrefix(mask, selectedCount);
                            }
                        }
                    }
                }
            }
            if (allSelected || selectedCount == maskCount) {
                return allocator.copyMask(allocationContext, mask);
            }
            return allocator.allocateSparseMask(allocationContext, positions, selectedCount, mask.size());
        }

        private boolean dictionaryMatch(Vector dictionaryValues, int dictionaryId, int generation)
        {
            if (cacheDictionaryMatches) {
                return cachedDictionaryMatch(dictionaryValues, dictionaryId);
            }
            if (dictionaryGenerations[dictionaryId] != generation) {
                dictionaryMatches[dictionaryId] = containsValue(dictionaryValues, dictionaryId);
                dictionaryGenerations[dictionaryId] = generation;
            }
            return dictionaryMatches[dictionaryId];
        }

        private boolean cachedDictionaryMatch(Vector dictionaryValues, int dictionaryId)
        {
            if (cachedDictionaryValues != dictionaryValues) {
                cachedDictionaryValues = dictionaryValues;
                byte[] previous = cachedDictionaryMatchStates;
                cachedDictionaryMatchStates = borrowBytes(dictionaryValues.length());
                Arrays.fill(cachedDictionaryMatchStates, (byte) 0);
                release(previous);
            }
            byte state = cachedDictionaryMatchStates[dictionaryId];
            if (state != 0) {
                return state > 1;
            }
            boolean match = containsValue(dictionaryValues, dictionaryId);
            cachedDictionaryMatchStates[dictionaryId] = (byte) (match ? 2 : 1);
            return match;
        }

        private int nextDictionaryGeneration()
        {
            if (dictionaryGeneration == Integer.MAX_VALUE) {
                Arrays.fill(dictionaryGenerations, 0);
                dictionaryGeneration = 0;
            }
            return ++dictionaryGeneration;
        }

        private void ensureDictionaryScratch(int size)
        {
            if (dictionaryMatches.length >= size) {
                return;
            }
            boolean[] previousMatches = dictionaryMatches;
            int[] previousGenerations = dictionaryGenerations;
            dictionaryMatches = borrowBooleans(size);
            dictionaryGenerations = borrowInts(size);
            Arrays.fill(dictionaryGenerations, 0);
            dictionaryGeneration = 0;
            release(previousMatches);
            release(previousGenerations);
        }

        private byte[] borrowBytes(int size)
        {
            return arrayPool.isRetainable(size) ? arrayPool.borrowBytes(size) : new byte[size];
        }

        private boolean[] borrowBooleans(int size)
        {
            return arrayPool.isRetainable(size) ? arrayPool.borrowBooleans(size) : new boolean[size];
        }

        private int[] borrowInts(int size)
        {
            return arrayPool.isRetainable((long) size * Integer.BYTES) ? arrayPool.borrowInts(size) : new int[size];
        }

        private void release(byte[] buffer)
        {
            if (arrayPool.isRetainable(buffer.length)) {
                arrayPool.release(buffer);
            }
        }

        private void release(boolean[] buffer)
        {
            if (arrayPool.isRetainable(buffer.length)) {
                arrayPool.release(buffer);
            }
        }

        private void release(int[] buffer)
        {
            if (arrayPool.isRetainable((long) buffer.length * Integer.BYTES)) {
                arrayPool.release(buffer);
            }
        }

        private void releaseBuffers()
        {
            release(dictionaryMatches);
            dictionaryMatches = new boolean[0];
            release(dictionaryGenerations);
            dictionaryGenerations = new int[0];
            release(cachedDictionaryMatchStates);
            cachedDictionaryMatchStates = new byte[0];
            cachedDictionaryValues = null;
        }

        private boolean containsValue(Vector values, int position)
        {
            return switch (values) {
                case BinaryVector binary -> contains(binary, position);
                case DictionaryVector dictionary -> containsValue(dictionary.values(), dictionary.ids()[position]);
                case RleVector rle -> containsValue(rle.values(), rle.runIndex(position));
                default -> false;
            };
        }

        private boolean contains(BinaryVector vector, int position)
        {
            return contains(vector.data(), vector.startOffset(position), vector.length(position));
        }

        private boolean contains(byte[] data, int offset, int length)
        {
            if (length <= 2 * Long.BYTES) {
                long firstWord = length <= Long.BYTES ? pack(data, offset, length) : pack(data, offset, Long.BYTES);
                long secondWord = length <= Long.BYTES ? 0 : pack(data, offset + Long.BYTES, Math.min(Long.BYTES, length - Long.BYTES));
                for (int index = 0; index < size; index++) {
                    if (lengths[index] == length && firstWords[index] == firstWord && secondWords[index] == secondWord) {
                        return true;
                    }
                }
                return false;
            }

            int hash = OperatorVectorSupport.binaryHash(data, offset, length);
            for (int index = 0; index < size; index++) {
                if (hashes[index] == hash
                        && lengths[index] == length
                        && OperatorVectorSupport.binaryEquals(values[index], 0, data, offset, length)) {
                    return true;
                }
            }
            return false;
        }

        private static long pack(byte[] data, int offset, int length)
        {
            if (length == Long.BYTES) {
                return (long) LONG_HANDLE.get(data, offset);
            }
            long value = 0;
            for (int index = 0; index < length; index++) {
                value |= (data[offset + index] & 0xFFL) << (index * Byte.SIZE);
            }
            return value;
        }
    }

    private static final class BatchState
    {
        private final Batch sourceBatch;
        private final Mask[] maskHolder;
        private BooleanVector matchValues;
        private boolean matchValuesComplete;

        private BatchState(Batch sourceBatch, Mask mask)
        {
            this.sourceBatch = sourceBatch;
            this.maskHolder = new Mask[] {mask};
        }

        private void constrain(Mask mask)
        {
            maskHolder[0] = mask;
            sourceBatch.constrain(mask);
        }

        private Batch sourceBatch()
        {
            return sourceBatch;
        }

        private Mask mask()
        {
            return maskHolder[0];
        }

        private BooleanVector borrowMatchValues(SemiJoinOperator operator)
        {
            return borrowMatchValues(operator, null);
        }

        private BooleanVector borrowMatchValues(SemiJoinOperator operator, Mask requestedMask)
        {
            Mask effectiveMask = requestedMask == null ? mask() : requestedMask;
            if (matchValues == null) {
                matchValues = operator.allocator.allocate(operator.allocationContext, BooleanVector.class, mask().size(), BooleanVector::new);
            }
            if (matchValuesComplete) {
                return matchValues;
            }
            if (effectiveMask.none()) {
                return matchValues;
            }
            boolean computesCurrentMask = effectiveMask.containsAll(mask());
            BooleanVector result = operator.computeMatchValues(this, matchValues, effectiveMask);
            if (computesCurrentMask) {
                matchValuesComplete = true;
            }
            return result;
        }

        private Mask borrowMatchMask(SemiJoinOperator operator, Mask requestedMask, boolean selectTrue, Allocator resultAllocator, Allocator.Context resultAllocationContext)
        {
            return operator.selectRows(sourceBatch, requestedMask, selectTrue, resultAllocator, resultAllocationContext);
        }
    }
}
