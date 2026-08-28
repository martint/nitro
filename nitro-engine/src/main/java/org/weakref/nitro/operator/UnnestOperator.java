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
import org.weakref.nitro.data.ErrorVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RepeatedVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.checkIndex;
import static java.util.Objects.requireNonNull;

/**
 * Expands aligned repeated structural columns while retaining their physical child encodings.
 *
 * <p>Multiple inputs are zipped to the longest input for each row. Shorter inputs are null padded. Outer expansion
 * emits one null-padded row when every input is empty or null. Output is bounded so a single large collection cannot
 * monopolize a driver or require an unbounded metadata allocation.
 */
public final class UnnestOperator
        implements Operator
{
    private record Projection(Streams streams, List<Vector> inheritedNulls)
    {
        private Projection
        {
            streams = requireNonNull(streams, "streams is null");
            inheritedNulls = List.copyOf(requireNonNull(inheritedNulls, "inheritedNulls is null"));
        }
    }

    /** Publishes sibling streams over one immutable logical-row mapping. */
    private static final class SharedMapping
    {
        private DictionaryVector anchor;

        private DictionaryVector wrap(int[] ids, int count, Vector values)
        {
            if (anchor == null) {
                anchor = DictionaryVector.wrapNested(ids, count, values);
                return anchor;
            }
            return anchor.sharedMappingWithValues(values);
        }
    }

    public record OutputMapping(int repeatedOutput, List<Integer> fieldPath, Field field)
    {
        public OutputMapping
        {
            checkArgument(repeatedOutput >= 0, "repeatedOutput is negative");
            fieldPath = List.copyOf(requireNonNull(fieldPath, "fieldPath is null"));
            for (int fieldIndex : fieldPath) {
                checkArgument(fieldIndex >= 0, "fieldPath contains a negative index");
            }
            field = requireNonNull(field, "field is null");
        }

        public static OutputMapping direct(int repeatedOutput, Field field)
        {
            return new OutputMapping(repeatedOutput, List.of(), field);
        }
    }

    public record Mapping(int inputColumn, List<OutputMapping> outputs)
    {
        public Mapping
        {
            checkArgument(inputColumn >= 0, "inputColumn is negative");
            outputs = List.copyOf(requireNonNull(outputs, "outputs is null"));
            checkArgument(!outputs.isEmpty(), "outputs is empty");
        }

        public static Mapping direct(int inputColumn, List<Field> outputFields)
        {
            requireNonNull(outputFields, "outputFields is null");
            List<OutputMapping> outputs = new ArrayList<>(outputFields.size());
            for (int output = 0; output < outputFields.size(); output++) {
                outputs.add(OutputMapping.direct(output, outputFields.get(output)));
            }
            return new Mapping(inputColumn, outputs);
        }
    }

    private final Allocator.Context allocationContext = new Allocator.Context("UnnestOperator", UnnestOperator.class);
    private final Allocator allocator;
    private final Operator source;
    private final int[] replicateColumns;
    private final List<Mapping> mappings;
    private final Optional<Field> ordinalityField;
    private final boolean outer;
    private final int maxRowsPerBatch;
    private final Schema outputSchema;
    private final int[] replicatePositions;
    private final int[][] nestedPositions;
    private final boolean[][] padding;
    private final boolean[] mappingHasPadding;
    private final int[] repeatedStarts;
    private final int[] repeatedLengths;
    private final long[] ordinality;
    private final boolean[] ordinalityNulls;

    private InputState input;
    private boolean outputOpen;
    private boolean closed;

    public UnnestOperator(
            Allocator allocator,
            Operator source,
            int[] replicateColumns,
            List<Mapping> mappings,
            Optional<Field> ordinalityField,
            boolean outer,
            int maxRowsPerBatch)
    {
        this(
                allocator,
                source,
                replicateColumns,
                mappings,
                ordinalityField,
                outer,
                new UnnestOperatorPolicy(maxRowsPerBatch));
    }

    public UnnestOperator(
            Allocator allocator,
            Operator source,
            int[] replicateColumns,
            List<Mapping> mappings,
            Optional<Field> ordinalityField,
            boolean outer,
            UnnestOperatorPolicy policy)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.source = requireNonNull(source, "source is null");
        this.replicateColumns = requireNonNull(replicateColumns, "replicateColumns is null").clone();
        this.mappings = List.copyOf(requireNonNull(mappings, "mappings is null"));
        this.ordinalityField = requireNonNull(ordinalityField, "ordinalityField is null");
        this.outer = outer;
        checkArgument(!this.mappings.isEmpty(), "mappings is empty");
        policy = requireNonNull(policy, "policy is null");
        this.maxRowsPerBatch = policy.maxRowsPerBatch();

        Schema sourceSchema = source.outputSchema();
        List<Field> outputFields = new ArrayList<>();
        for (int replicateColumn : this.replicateColumns) {
            outputFields.add(sourceSchema.field(checkIndex(replicateColumn, sourceSchema.size())));
        }
        for (Mapping mapping : this.mappings) {
            checkIndex(mapping.inputColumn(), sourceSchema.size());
            mapping.outputs().stream().map(OutputMapping::field).forEach(outputFields::add);
        }
        ordinalityField.ifPresent(outputFields::add);
        outputSchema = new Schema(outputFields);

        replicatePositions = allocator.primitiveArrays().borrowInts(this.maxRowsPerBatch);
        nestedPositions = new int[this.mappings.size()][];
        padding = new boolean[this.mappings.size()][];
        for (int mapping = 0; mapping < this.mappings.size(); mapping++) {
            nestedPositions[mapping] = allocator.primitiveArrays().borrowInts(this.maxRowsPerBatch);
            padding[mapping] = allocator.primitiveArrays().borrowBooleans(this.maxRowsPerBatch);
        }
        mappingHasPadding = allocator.primitiveArrays().borrowBooleans(this.mappings.size());
        repeatedStarts = allocator.primitiveArrays().borrowInts(this.mappings.size());
        repeatedLengths = allocator.primitiveArrays().borrowInts(this.mappings.size());
        ordinality = allocator.primitiveArrays().borrowLongs(this.maxRowsPerBatch);
        ordinalityNulls = outer && ordinalityField.isPresent()
                ? allocator.primitiveArrays().borrowBooleans(this.maxRowsPerBatch)
                : null;
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
        if (input != null && input.hasOutput()) {
            return true;
        }
        checkArgument(!outputOpen, "Cannot advance UNNEST while its output batch is open");
        closeExhaustedInput();
        while (source.hasNext()) {
            InputState candidate = new InputState(source.next());
            if (candidate.hasOutput()) {
                input = candidate;
                return true;
            }
            candidate.close();
        }
        return false;
    }

    @Override
    public Batch next()
    {
        if (!hasNext()) {
            throw new IllegalStateException("UNNEST has no more output");
        }
        checkArgument(!outputOpen, "Previous UNNEST output batch is still open");

        int directOutputCount = input.directOutputCount();
        if (directOutputCount >= 0) {
            return directOutput(directOutputCount);
        }

        int count = input.fill(maxRowsPerBatch);
        Mask mask = allocator.allocateAllMask(allocationContext, count);
        List<Vector> owned = new ArrayList<>();
        Output[] outputs = new Output[outputCount()];
        int output = 0;

        SharedMapping replicateMapping = new SharedMapping();
        for (int replicateColumn : replicateColumns) {
            outputs[output] = mappedOutput(
                    input.batch.output(replicateColumn),
                    replicatePositions,
                    count,
                    isIdentityMapping(replicatePositions, count),
                    false,
                    null,
                    null,
                    replicateMapping,
                    owned);
            output++;
        }

        for (int mapping = 0; mapping < mappings.size(); mapping++) {
            SharedMapping sharedMapping = new SharedMapping();
            boolean hasPadding = mappingHasPadding[mapping];
            boolean identityMapping = !hasPadding && isIdentityMapping(nestedPositions[mapping], count);
            VectorAccess.RepeatedValues repeated = input.repeated[mapping];
            for (OutputMapping outputMapping : mappings.get(mapping).outputs()) {
                outputs[output] = mappedOutput(
                        project(repeated.output(outputMapping.repeatedOutput()), outputMapping.fieldPath()),
                        nestedPositions[mapping],
                        count,
                        identityMapping,
                        hasPadding,
                        padding[mapping],
                        outputMapping.field(),
                        sharedMapping,
                        owned);
                output++;
            }
        }

        if (ordinalityField.isPresent()) {
            I64Vector ordinality = allocator.allocate(allocationContext, I64Vector.class, count, I64Vector::new);
            System.arraycopy(this.ordinality, 0, ordinality.values(), 0, count);
            owned.add(ordinality);
            if (input.hasOrdinalityNulls()) {
                BooleanVector nulls = allocator.allocate(allocationContext, BooleanVector.class, count, BooleanVector::new);
                System.arraycopy(ordinalityNulls, 0, nulls.values(), 0, count);
                owned.add(nulls);
                outputs[output++] = borrowedOutput(Streams.ofValuesAndNulls(ordinality, nulls));
            }
            else {
                outputs[output++] = borrowedOutput(Streams.ofValues(ordinality));
            }
        }
        if (output != outputs.length) {
            throw new IllegalStateException("UNNEST output shape changed after planning");
        }
        boolean last = !input.hasOutput();
        outputOpen = true;
        return new Batch(
                mask,
                _ -> {},
                value -> allocator.transfer(allocationContext, value),
                value -> allocator.release(allocationContext, value),
                () -> {
                    for (Vector vector : owned) {
                        allocator.release(allocationContext, vector);
                    }
                    outputOpen = false;
                    if (last) {
                        closeExhaustedInput();
                    }
                },
                outputs);
    }

    private Batch directOutput(int count)
    {
        Mask mask = allocator.allocateAllMask(allocationContext, count);
        Output[] outputs = new Output[outputCount()];
        int output = 0;
        for (int mappingIndex = 0; mappingIndex < mappings.size(); mappingIndex++) {
            Mapping mapping = mappings.get(mappingIndex);
            VectorAccess.RepeatedValues repeated = input.repeated[mappingIndex];
            for (OutputMapping outputMapping : mapping.outputs()) {
                Projection projection = project(repeated.output(outputMapping.repeatedOutput()), outputMapping.fieldPath());
                if (!projection.inheritedNulls().isEmpty()) {
                    throw new IllegalStateException("Direct UNNEST output unexpectedly inherited structural nulls");
                }
                outputs[output++] = borrowedOutput(projection.streams());
            }
        }
        input.consumeDirectOutput();
        outputOpen = true;
        return new Batch(
                mask,
                _ -> {},
                value -> allocator.transfer(allocationContext, value),
                value -> allocator.release(allocationContext, value),
                () -> {
                    outputOpen = false;
                    closeExhaustedInput();
                },
                outputs);
    }

    @Override
    public void constrain(Mask mask)
    {
        // Expanded rows do not have a one-to-one input mask to propagate. Output vectors remain lazy dictionary views.
    }

    @Override
    public boolean supportsOpenBatchHasNext()
    {
        return true;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        try {
            if (input != null) {
                input.close();
                input = null;
            }
            source.close();
            allocator.release(allocationContext);
        }
        finally {
            allocator.primitiveArrays().release(replicatePositions);
            for (int mapping = 0; mapping < mappings.size(); mapping++) {
                allocator.primitiveArrays().release(nestedPositions[mapping]);
                allocator.primitiveArrays().release(padding[mapping]);
            }
            allocator.primitiveArrays().release(mappingHasPadding);
            allocator.primitiveArrays().release(repeatedStarts);
            allocator.primitiveArrays().release(repeatedLengths);
            allocator.primitiveArrays().release(ordinality);
            if (ordinalityNulls != null) {
                allocator.primitiveArrays().release(ordinalityNulls);
            }
        }
    }

    private Output mappedOutput(
            Output sourceOutput,
            int[] ids,
            int count,
            boolean identityMapping,
            boolean hasPadding,
            boolean[] padding,
            Field field,
            SharedMapping sharedMapping,
            List<Vector> owned)
    {
        Streams sourceStreams = Streams.of(
                sourceOutput.borrow(Stream.VALUES),
                sourceOutput.borrowOrNull(Stream.NULLS),
                sourceOutput.borrowOrNull(Stream.ERRORS));
        return mappedOutput(new Projection(sourceStreams, List.of()), ids, count, identityMapping, hasPadding, padding, field, sharedMapping, owned);
    }

    private static Projection project(Streams source, List<Integer> fieldPath)
    {
        Streams result = source;
        List<Vector> inheritedNulls = new ArrayList<>();
        for (int field : fieldPath) {
            // Struct-level nulls are semantically inherited by every child. Preserve them separately so mappedOutput
            // can combine them with the leaf's own null stream without materializing the structural value.
            Vector parentNulls = result.getOrNull(Stream.NULLS);
            if (parentNulls != null && !VectorAccess.isAllFalseNulls(parentNulls)) {
                inheritedNulls.add(parentNulls);
            }
            result = VectorAccess.structField(result.values(), field);
        }
        return new Projection(result, inheritedNulls);
    }

    private Output mappedOutput(
            Projection projection,
            int[] ids,
            int count,
            boolean identityMapping,
            boolean hasPadding,
            boolean[] padding,
            Field field,
            SharedMapping sharedMapping,
            List<Vector> owned)
    {
        Streams source = projection.streams();
        Streams.Builder streams = Streams.builder();
        boolean mappedNulls = false;
        for (Stream stream : source.streams()) {
            Vector sourceVector = source.get(stream);
            Vector mapped;
            if (sourceVector.length() == 0) {
                if (stream != Stream.VALUES || !hasPadding) {
                    continue;
                }
                mapped = requireNonNull(field, "field is null")
                        .type()
                        .vectorFactory()
                        .orElseThrow(() -> new IllegalArgumentException("UNNEST output type cannot construct null values"))
                        .nullValues(allocator.vectorAllocator(allocationContext), count);
                owned.add(mapped);
            }
            else if ((hasPadding || !projection.inheritedNulls().isEmpty()) && stream == Stream.NULLS) {
                BooleanVector nulls = allocator.allocate(allocationContext, BooleanVector.class, count, BooleanVector::new);
                VectorAccess.BooleanValues sourceNulls = VectorAccess.booleanValues(sourceVector);
                List<VectorAccess.BooleanValues> inheritedNulls = projection.inheritedNulls().stream()
                        .map(VectorAccess::booleanValues)
                        .toList();
                for (int position = 0; position < count; position++) {
                    int sourcePosition = ids[position];
                    boolean isNull = hasPadding && padding[position] || sourceNulls.value(sourcePosition);
                    for (VectorAccess.BooleanValues inherited : inheritedNulls) {
                        isNull |= inherited.value(sourcePosition);
                    }
                    nulls.values()[position] = isNull;
                }
                owned.add(nulls);
                mapped = nulls;
            }
            else if (hasPadding && stream == Stream.ERRORS) {
                ErrorVector errors = allocator.allocate(allocationContext, ErrorVector.class, count, ErrorVector::new);
                for (int position = 0; position < count; position++) {
                    if (!padding[position]) {
                        sourceVector.copySinglePositionInto(allocator, allocationContext, errors, ids[position], position, count);
                    }
                }
                owned.add(errors);
                mapped = errors;
            }
            else if (identityMapping && sourceVector.length() == count) {
                mapped = sourceVector;
            }
            else {
                mapped = sharedMapping.wrap(ids, count, sourceVector);
            }
            streams.put(stream, mapped);
            mappedNulls |= stream == Stream.NULLS;
        }
        if ((hasPadding || !projection.inheritedNulls().isEmpty()) && !mappedNulls) {
            BooleanVector nulls = allocator.allocate(allocationContext, BooleanVector.class, count, BooleanVector::new);
            List<VectorAccess.BooleanValues> inheritedNulls = projection.inheritedNulls().stream()
                    .map(VectorAccess::booleanValues)
                    .toList();
            for (int position = 0; position < count; position++) {
                int sourcePosition = ids[position];
                boolean isNull = hasPadding && padding[position];
                if (!isNull) {
                    for (VectorAccess.BooleanValues inherited : inheritedNulls) {
                        isNull |= inherited.value(sourcePosition);
                    }
                }
                nulls.values()[position] = isNull;
            }
            owned.add(nulls);
            streams.put(Stream.NULLS, nulls);
        }
        return borrowedOutput(streams.build());
    }

    private static boolean isIdentityMapping(int[] ids, int count)
    {
        for (int position = 0; position < count; position++) {
            if (ids[position] != position) {
                return false;
            }
        }
        return true;
    }

    private Output borrowedOutput(Streams streams)
    {
        return new Output(
                streams.streams(),
                streams::get,
                (_, vector) -> {
                    Vector copy = vector.copy(allocator, allocationContext);
                    return allocator.transfer(allocationContext, copy);
                },
                (_, _) -> {});
    }

    private void closeExhaustedInput()
    {
        if (input != null && !input.hasOutput()) {
            input.close();
            input = null;
        }
    }

    private final class InputState
            implements AutoCloseable
    {
        private final Batch batch;
        private final Mask mask;
        private final VectorAccess.RepeatedValues[] repeated;
        private final VectorAccess.BooleanValues[] collectionNulls;
        private final boolean rowShapeReusable;
        private int selectedIndex;
        private int elementIndex;
        private int rowLength = -1;
        private int cachedRowStart = -1;
        private int cachedRowEnd = -1;
        private int cachedRowLength;
        private boolean cachedOuterPadding;
        private boolean outerPadding;
        private boolean hasOrdinalityNulls;
        private boolean closed;

        private InputState(Batch batch)
        {
            this.batch = requireNonNull(batch, "batch is null");
            mask = batch.borrowMask();
            repeated = new VectorAccess.RepeatedValues[mappings.size()];
            collectionNulls = new VectorAccess.BooleanValues[mappings.size()];
            boolean rowShapeReusable = true;
            for (int mapping = 0; mapping < mappings.size(); mapping++) {
                Output input = batch.output(mappings.get(mapping).inputColumn());
                repeated[mapping] = VectorAccess.repeatedValues(input.borrow(Stream.VALUES));
                for (OutputMapping output : mappings.get(mapping).outputs()) {
                    if (output.repeatedOutput() >= repeated[mapping].outputCount()) {
                        throw new IllegalArgumentException("Repeated vector output does not match planned UNNEST output");
                    }
                }
                Vector nulls = input.borrowOrNull(Stream.NULLS);
                boolean nullFree = VectorAccess.isAllFalseNulls(nulls);
                collectionNulls[mapping] = VectorAccess.booleanValues(nullFree ? null : nulls);
                rowShapeReusable &= nullFree;
            }
            this.rowShapeReusable = rowShapeReusable;
            skipRowsWithoutOutput();
        }

        private boolean hasOutput()
        {
            return selectedIndex < mask.selectedCount();
        }

        private int directOutputCount()
        {
            if (selectedIndex != 0 || elementIndex != 0 ||
                    replicateColumns.length != 0 || mappings.size() != 1 || ordinalityField.isPresent() || outer || !mask.all()) {
                return -1;
            }
            int count = -1;
            for (int mapping = 0; mapping < mappings.size(); mapping++) {
                Output input = batch.output(mappings.get(mapping).inputColumn());
                if (!(input.borrow(Stream.VALUES) instanceof RepeatedVector) ||
                        !VectorAccess.isAllFalseNulls(input.borrowOrNull(Stream.NULLS)) ||
                        !VectorAccess.isAllFalseNulls(input.borrowOrNull(Stream.ERRORS))) {
                    return -1;
                }
                int mappingCount = mask.size() == 0 ? 0 : repeated[mapping].endOffset(mask.size() - 1);
                if ((mask.size() > 0 && repeated[mapping].startOffset(0) != 0) || mappingCount > maxRowsPerBatch) {
                    return -1;
                }
                if (count >= 0 && count != mappingCount) {
                    return -1;
                }
                count = mappingCount;
                for (OutputMapping output : mappings.get(mapping).outputs()) {
                    Projection projection = project(repeated[mapping].output(output.repeatedOutput()), output.fieldPath());
                    if (!projection.inheritedNulls().isEmpty() || projection.streams().values().length() != mappingCount) {
                        return -1;
                    }
                }
            }
            return count;
        }

        private void consumeDirectOutput()
        {
            selectedIndex = mask.selectedCount();
        }

        private int fill(int limit)
        {
            for (boolean[] values : padding) {
                Arrays.fill(values, 0, limit, false);
            }
            Arrays.fill(mappingHasPadding, false);
            hasOrdinalityNulls = false;

            int output = 0;
            while (output < limit && hasOutput()) {
                int inputPosition = mask.position(selectedIndex);
                if (rowLength < 0) {
                    rowLength = prepareRow(inputPosition);
                }
                while (output < limit && elementIndex < rowLength) {
                    replicatePositions[output] = inputPosition;
                    ordinality[output] = elementIndex + 1L;
                    if (ordinalityNulls != null) {
                        ordinalityNulls[output] = outerPadding;
                        hasOrdinalityNulls |= outerPadding;
                    }
                    for (int mapping = 0; mapping < mappings.size(); mapping++) {
                        int length = repeatedLengths[mapping];
                        if (elementIndex < length) {
                            nestedPositions[mapping][output] = repeatedStarts[mapping] + elementIndex;
                        }
                        else {
                            nestedPositions[mapping][output] = 0;
                            padding[mapping][output] = true;
                            mappingHasPadding[mapping] = true;
                        }
                    }
                    output++;
                    elementIndex++;
                }
                if (elementIndex == rowLength) {
                    selectedIndex++;
                    elementIndex = 0;
                    rowLength = -1;
                    skipRowsWithoutOutput();
                }
            }
            return output;
        }

        private boolean hasOrdinalityNulls()
        {
            return hasOrdinalityNulls;
        }

        private int prepareRow(int inputPosition)
        {
            if (rowShapeReusable && inputPosition >= cachedRowStart && inputPosition < cachedRowEnd) {
                outerPadding = cachedOuterPadding;
                return cachedRowLength;
            }

            int length = 0;
            int rowEnd = Integer.MAX_VALUE;
            for (int mapping = 0; mapping < mappings.size(); mapping++) {
                if (collectionNulls[mapping].value(inputPosition)) {
                    repeatedStarts[mapping] = 0;
                    repeatedLengths[mapping] = 0;
                }
                else {
                    repeatedStarts[mapping] = repeated[mapping].startOffset(inputPosition);
                    repeatedLengths[mapping] = repeated[mapping].length(inputPosition);
                }
                length = Math.max(length, repeatedLengths[mapping]);
                if (rowShapeReusable) {
                    rowEnd = Math.min(rowEnd, repeated[mapping].valueRunEnd(inputPosition));
                }
            }
            outerPadding = outer && length == 0;
            int result = outer ? Math.max(1, length) : length;
            if (rowShapeReusable) {
                cachedRowStart = inputPosition;
                cachedRowEnd = rowEnd;
                cachedRowLength = result;
                cachedOuterPadding = outerPadding;
            }
            return result;
        }

        private void skipRowsWithoutOutput()
        {
            if (outer) {
                return;
            }
            while (selectedIndex < mask.selectedCount()) {
                rowLength = prepareRow(mask.position(selectedIndex));
                if (rowLength != 0) {
                    return;
                }
                selectedIndex++;
                rowLength = -1;
            }
        }

        @Override
        public void close()
        {
            if (!closed) {
                closed = true;
                batch.close();
            }
        }
    }
}
