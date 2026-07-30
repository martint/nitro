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
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class FullJoinOperator
        implements Operator
{
    private static final int NO_MATCH = -1;
    private static final int JOINED_ROW_CHUNK_SHIFT = 16;
    private static final int JOINED_ROW_CHUNK_SIZE = 1 << JOINED_ROW_CHUNK_SHIFT;
    private static final int JOINED_ROW_CHUNK_MASK = JOINED_ROW_CHUNK_SIZE - 1;
    private final Allocator allocator;
    private final PrimitiveArrayPool arrayPool;
    private final Allocator.Context allocationContext = new Allocator.Context("FullJoinOperator");
    private final Operator outer;
    private final Operator inner;
    private final int[] outerJoinColumns;
    private final int[] innerJoinColumns;
    private final boolean sortedInputs;
    private final FullJoinOperatorPolicy policy;
    private final Schema outputSchema;
    private final TypeBinding[] keyTypes;
    private final StructuralComparisonKernel[] comparisonKernels;
    private final StructuralKeyKernel[] keyKernels;

    private Streams[] materialized;
    private Mask outputMask;
    private boolean loaded;
    private boolean done;

    public FullJoinOperator(
            Allocator allocator,
            Operator outer,
            int[] outerJoinColumns,
            Operator inner,
            int[] innerJoinColumns,
            FullJoinOperatorPolicy policy)
    {
        this(
                allocator,
                outer,
                outerJoinColumns,
                inner,
                innerJoinColumns,
                false,
                policy,
                new StructuralTypeKernelFactory());
    }

    public FullJoinOperator(
            Allocator allocator,
            Operator outer,
            int[] outerJoinColumns,
            Operator inner,
            int[] innerJoinColumns,
            OperatorResources resources)
    {
        this(
                allocator,
                outer,
                outerJoinColumns,
                inner,
                innerJoinColumns,
                false,
                requireNonNull(resources, "resources is null").fullJoinPolicy(),
                resources.codeGeneration().structuralTypes());
    }

    /**
     * Creates a full join for inputs already ordered ascending by their respective join columns.
     * The explicit contract lets blocking/window pipelines preserve and exploit physical order without
     * embedding query- or type-specific logic in the join.
     */
    public static FullJoinOperator sorted(
            Allocator allocator,
            Operator outer,
            int[] outerJoinColumns,
            Operator inner,
            int[] innerJoinColumns,
            FullJoinOperatorPolicy policy)
    {
        return new FullJoinOperator(
                allocator,
                outer,
                outerJoinColumns,
                inner,
                innerJoinColumns,
                true,
                policy,
                new StructuralTypeKernelFactory());
    }

    public static FullJoinOperator sorted(
            Allocator allocator,
            Operator outer,
            int[] outerJoinColumns,
            Operator inner,
            int[] innerJoinColumns,
            OperatorResources resources)
    {
        return new FullJoinOperator(
                allocator,
                outer,
                outerJoinColumns,
                inner,
                innerJoinColumns,
                true,
                requireNonNull(resources, "resources is null").fullJoinPolicy(),
                resources.codeGeneration().structuralTypes());
    }

    private FullJoinOperator(
            Allocator allocator,
            Operator outer,
            int[] outerJoinColumns,
            Operator inner,
            int[] innerJoinColumns,
            boolean sortedInputs,
            FullJoinOperatorPolicy policy,
            StructuralTypeKernelFactory structuralTypes)
    {
        if (outerJoinColumns.length == 0) {
            throw new IllegalArgumentException("FullJoinOperator requires at least one join key");
        }
        if (outerJoinColumns.length != innerJoinColumns.length) {
            throw new IllegalArgumentException("Join key counts must match");
        }
        this.allocator = allocator;
        this.arrayPool = allocator.primitiveArrays();
        this.outer = outer;
        this.inner = inner;
        this.outerJoinColumns = outerJoinColumns.clone();
        this.innerJoinColumns = innerJoinColumns.clone();
        this.sortedInputs = sortedInputs;
        this.policy = requireNonNull(policy, "policy is null");
        this.outputSchema = outputSchema(outer.outputSchema(), inner.outputSchema());
        this.keyTypes = joinKeyTypes(
                outer.outputSchema(), this.outerJoinColumns, inner.outputSchema(), this.innerJoinColumns);
        this.comparisonKernels = new StructuralComparisonKernel[keyTypes.length];
        this.keyKernels = new StructuralKeyKernel[keyTypes.length];
        for (int keyIndex = 0; keyIndex < keyTypes.length; keyIndex++) {
            if (sortedInputs) {
                comparisonKernels[keyIndex] = structuralTypes.comparison(keyTypes[keyIndex]);
            }
            else {
                keyKernels[keyIndex] = structuralTypes.key(keyTypes[keyIndex]);
            }
        }
    }

    private static TypeBinding[] joinKeyTypes(
            Schema outerSchema,
            int[] outerJoinColumns,
            Schema innerSchema,
            int[] innerJoinColumns)
    {
        TypeBinding[] types = new TypeBinding[outerJoinColumns.length];
        for (int keyIndex = 0; keyIndex < outerJoinColumns.length; keyIndex++) {
            int index = keyIndex;
            Optional<TypeBinding> outerType = typeAt(outerSchema, outerJoinColumns[keyIndex]);
            Optional<TypeBinding> innerType = typeAt(innerSchema, innerJoinColumns[keyIndex]);
            if (outerType.filter(TypeBinding::isSpecified).isPresent() &&
                    innerType.filter(TypeBinding::isSpecified).isPresent() &&
                    !outerType.orElseThrow().identity().equals(innerType.orElseThrow().identity())) {
                throw new IllegalArgumentException("Full-join key types do not match at index " + keyIndex);
            }
            types[keyIndex] = innerType.filter(TypeBinding::isSpecified)
                    .or(() -> outerType.filter(TypeBinding::isSpecified))
                    .or(() -> innerType)
                    .or(() -> outerType)
                    .orElseThrow(() -> new IllegalArgumentException(
                            "Full-join key column is out of bounds at index " + index));
        }
        return types;
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
        return outer.outputCount() + inner.outputCount();
    }

    @Override
    public Schema outputSchema()
    {
        return outputSchema;
    }

    private static Schema outputSchema(Schema outerSchema, Schema innerSchema)
    {
        List<Field> fields = new ArrayList<>(outerSchema.size() + innerSchema.size());
        outerSchema.fields().forEach(field -> fields.add(nullable(field)));
        innerSchema.fields().forEach(field -> fields.add(nullable(field)));
        return new Schema(fields);
    }

    private static Field nullable(Field field)
    {
        return field.nullable() ? field : new Field(field.name(), field.type(), true);
    }

    @Override
    public boolean hasNext()
    {
        if (!loaded) {
            load();
        }
        return !done;
    }

    @Override
    public Batch next()
    {
        if (!loaded) {
            load();
        }
        done = true;
        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            int index = outputIndex;
            outputs[outputIndex] = new Output(
                    materialized[index].streams(),
                    materialized[index]::get,
                    (stream, vector) -> allocator.transfer(allocationContext, vector));
        }
        return new Batch(outputMask, takenMask -> allocator.transfer(allocationContext, takenMask), outputs);
    }

    @Override
    public void constrain(Mask mask)
    {
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        return true;
    }

    @Override
    public boolean supportsConstrainedReborrow()
    {
        // Output is reshaped relative to either input and constrain() is a no-op, so a downstream
        // constrain + re-borrow cannot be satisfied by source position.
        return false;
    }

    @Override
    public void close()
    {
        outer.close();
        inner.close();
        allocator.release(allocationContext);
    }

    private void load()
    {
        loaded = true;

        MaterializedInput outerInput = materialize(outer);
        MaterializedInput innerInput = materialize(inner);
        validateKeyVectors(outerInput.pages(), outerJoinColumns, "outer");
        validateKeyVectors(innerInput.pages(), innerJoinColumns, "inner");

        if (sortedInputs) {
            loadSorted(outerInput, innerInput);
            return;
        }

        int innerRowCount = countRows(innerInput.pages());
        long[] innerRowReferences = arrayPool.borrowLongs(innerRowCount);
        int[] innerHashNext = arrayPool.borrowInts(innerRowCount);
        boolean[] matchedInnerRows = arrayPool.borrowBooleans(innerRowCount);
        int[] innerHashHeads = arrayPool.borrowInts(hashTableSize(innerRowCount));
        Arrays.fill(innerHashHeads, NO_MATCH);
        Arrays.fill(matchedInnerRows, false);
        try {
            indexInnerRows(
                    innerInput.pages(),
                    innerRowReferences,
                    innerHashHeads,
                    innerHashNext);

            try (JoinedRows joinedRows = new JoinedRows(arrayPool)) {
                for (int outerPageIndex = 0; outerPageIndex < outerInput.pages().size(); outerPageIndex++) {
                    TableOperator.Page page = outerInput.pages().get(outerPageIndex);
                    for (int position = 0; position < page.rows(); position++) {
                        long outerRowReference = packRowReference(outerPageIndex, position);
                        if (keyHasNull(page.columns(), position, outerJoinColumns)) {
                            joinedRows.add(outerRowReference, NO_MATCH);
                            continue;
                        }
                        int innerOrdinal = innerHashHeads[
                                hashKey(page.columns(), outerJoinColumns, position) &
                                        (innerHashHeads.length - 1)];
                        boolean matched = false;
                        while (innerOrdinal != NO_MATCH) {
                            if (equalKeys(
                                    outerInput.pages(),
                                    outerRowReference,
                                    outerJoinColumns,
                                    innerInput.pages(),
                                    innerRowReferences[innerOrdinal],
                                    innerJoinColumns)) {
                                matched = true;
                                matchedInnerRows[innerOrdinal] = true;
                                joinedRows.add(outerRowReference, innerOrdinal);
                            }
                            innerOrdinal = innerHashNext[innerOrdinal];
                        }
                        if (!matched) {
                            joinedRows.add(outerRowReference, NO_MATCH);
                        }
                    }
                }

                for (int innerOrdinal = 0; innerOrdinal < matchedInnerRows.length; innerOrdinal++) {
                    if (!matchedInnerRows[innerOrdinal]) {
                        joinedRows.add(NO_MATCH, innerOrdinal);
                    }
                }

                materializeJoinedRows(outerInput, innerInput, joinedRows, innerRowReferences);
            }
        }
        finally {
            arrayPool.release(innerRowReferences);
            arrayPool.release(innerHashNext);
            arrayPool.release(matchedInnerRows);
            arrayPool.release(innerHashHeads);
        }
    }

    private void loadSorted(MaterializedInput outerInput, MaterializedInput innerInput)
    {
        int outerRowCount = countRows(outerInput.pages());
        int innerRowCount = countRows(innerInput.pages());
        long[] outerRowReferences = arrayPool.borrowLongs(outerRowCount);
        long[] innerRowReferences = arrayPool.borrowLongs(innerRowCount);
        try {
            fillRowReferences(outerInput.pages(), outerRowReferences);
            fillRowReferences(innerInput.pages(), innerRowReferences);
            try (JoinedRows joinedRows = new JoinedRows(arrayPool)) {
                int outerOrdinal = 0;
                int innerOrdinal = 0;
                while (outerOrdinal < outerRowCount && innerOrdinal < innerRowCount) {
                    long outerReference = outerRowReferences[outerOrdinal];
                    long innerReference = innerRowReferences[innerOrdinal];
                    int comparison = compareKeys(outerInput.pages(), outerReference, outerJoinColumns, innerInput.pages(), innerReference, innerJoinColumns);
                    if (comparison < 0 || (comparison == 0 && (keyHasNull(outerInput.pages(), outerReference, outerJoinColumns) || keyHasNull(innerInput.pages(), innerReference, innerJoinColumns)))) {
                        joinedRows.add(outerReference, NO_MATCH);
                        outerOrdinal++;
                        continue;
                    }
                    if (comparison > 0) {
                        joinedRows.add(NO_MATCH, innerOrdinal);
                        innerOrdinal++;
                        continue;
                    }

                    int outerRunEnd = equalRunEnd(outerInput.pages(), outerRowReferences, outerOrdinal, outerJoinColumns);
                    int innerRunEnd = equalRunEnd(innerInput.pages(), innerRowReferences, innerOrdinal, innerJoinColumns);
                    for (int outerIndex = outerOrdinal; outerIndex < outerRunEnd; outerIndex++) {
                        for (int innerIndex = innerOrdinal; innerIndex < innerRunEnd; innerIndex++) {
                            joinedRows.add(outerRowReferences[outerIndex], innerIndex);
                        }
                    }
                    outerOrdinal = outerRunEnd;
                    innerOrdinal = innerRunEnd;
                }
                while (outerOrdinal < outerRowCount) {
                    joinedRows.add(outerRowReferences[outerOrdinal++], NO_MATCH);
                }
                while (innerOrdinal < innerRowCount) {
                    joinedRows.add(NO_MATCH, innerOrdinal++);
                }
                materializeJoinedRows(outerInput, innerInput, joinedRows, innerRowReferences);
            }
        }
        finally {
            arrayPool.release(outerRowReferences);
            arrayPool.release(innerRowReferences);
        }
    }

    private void materializeJoinedRows(MaterializedInput outerInput, MaterializedInput innerInput, JoinedRows joinedRows, long[] innerRowReferences)
    {
        int rowCount = joinedRows.size();
        materialized = new Streams[outputCount()];
        for (int outputIndex = 0; outputIndex < outer.outputCount(); outputIndex++) {
            materialized[outputIndex] = materializeOutputColumn(
                    outputSchema.field(outputIndex).type(),
                    outerInput.schema()[outputIndex],
                    outerInput.pages(),
                    joinedRows,
                    rowCount,
                    true,
                    outputIndex,
                    innerRowReferences);
        }
        for (int outputIndex = 0; outputIndex < inner.outputCount(); outputIndex++) {
            materialized[outer.outputCount() + outputIndex] = materializeOutputColumn(
                    outputSchema.field(outer.outputCount() + outputIndex).type(),
                    innerInput.schema()[outputIndex],
                    innerInput.pages(),
                    joinedRows,
                    rowCount,
                    false,
                    outputIndex,
                    innerRowReferences);
        }
        outputMask = allocator.allocateRangeMask(allocationContext, 0, rowCount);
    }

    private static void fillRowReferences(List<TableOperator.Page> pages, long[] references)
    {
        int ordinal = 0;
        for (int pageIndex = 0; pageIndex < pages.size(); pageIndex++) {
            TableOperator.Page page = pages.get(pageIndex);
            for (int position = 0; position < page.rows(); position++) {
                references[ordinal++] = packRowReference(pageIndex, position);
            }
        }
    }

    private int equalRunEnd(List<TableOperator.Page> pages, long[] references, int start, int[] joinColumns)
    {
        int end = start + 1;
        while (end < references.length && equalKeys(pages, references[start], joinColumns, pages, references[end], joinColumns)) {
            end++;
        }
        return end;
    }

    private int compareKeys(List<TableOperator.Page> leftPages, long leftReference, int[] leftColumns, List<TableOperator.Page> rightPages, long rightReference, int[] rightColumns)
    {
        Streams[] left = columns(leftPages, leftReference);
        Streams[] right = columns(rightPages, rightReference);
        int leftPosition = unpackPosition(leftReference);
        int rightPosition = unpackPosition(rightReference);
        for (int keyIndex = 0; keyIndex < leftColumns.length; keyIndex++) {
            Streams leftKey = left[leftColumns[keyIndex]];
            Streams rightKey = right[rightColumns[keyIndex]];
            int comparison = comparisonKernels[keyIndex].compare(
                    leftKey.values(), leftKey.getOrNull(Stream.NULLS), leftPosition,
                    rightKey.values(), rightKey.getOrNull(Stream.NULLS), rightPosition);
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    private boolean equalKeys(List<TableOperator.Page> leftPages, long leftReference, int[] leftColumns, List<TableOperator.Page> rightPages, long rightReference, int[] rightColumns)
    {
        Streams[] left = columns(leftPages, leftReference);
        Streams[] right = columns(rightPages, rightReference);
        int leftPosition = unpackPosition(leftReference);
        int rightPosition = unpackPosition(rightReference);
        for (int keyIndex = 0; keyIndex < leftColumns.length; keyIndex++) {
            Streams leftKey = left[leftColumns[keyIndex]];
            Streams rightKey = right[rightColumns[keyIndex]];
            StructuralIdentityKernel identity = sortedInputs
                    ? comparisonKernels[keyIndex]
                    : keyKernels[keyIndex];
            if (!identity.identical(
                    leftKey.values(), leftKey.getOrNull(Stream.NULLS), leftPosition,
                    rightKey.values(), rightKey.getOrNull(Stream.NULLS), rightPosition)) {
                return false;
            }
        }
        return true;
    }

    private static boolean keyHasNull(List<TableOperator.Page> pages, long reference, int[] joinColumns)
    {
        Streams[] columns = columns(pages, reference);
        return keyHasNull(columns, unpackPosition(reference), joinColumns);
    }

    private static boolean keyHasNull(Streams[] columns, int position, int[] joinColumns)
    {
        for (int joinColumn : joinColumns) {
            if (OperatorVectorSupport.isNull(columns[joinColumn].getOrNull(Stream.NULLS), position)) {
                return true;
            }
        }
        return false;
    }

    private static Streams[] columns(List<TableOperator.Page> pages, long reference)
    {
        return pages.get(unpackPageIndex(reference)).columns();
    }

    private void validateKeyVectors(List<TableOperator.Page> pages, int[] joinColumns, String side)
    {
        for (TableOperator.Page page : pages) {
            for (int keyIndex = 0; keyIndex < joinColumns.length; keyIndex++) {
                TypeBinding type = keyTypes[keyIndex];
                Vector values = page.columns()[joinColumns[keyIndex]].values();
                if (type.isSpecified() && !type.supportsVector(values)) {
                    throw new IllegalArgumentException(
                            "Full-join " + side + " key vector at index " + keyIndex +
                                    " is incompatible with plan-time type " + type.identity());
                }
            }
        }
    }

    private void indexInnerRows(
            List<TableOperator.Page> pages,
            long[] innerRowReferences,
            int[] hashHeads,
            int[] hashNext)
    {
        int innerOrdinal = 0;
        for (int pageIndex = 0; pageIndex < pages.size(); pageIndex++) {
            TableOperator.Page page = pages.get(pageIndex);
            for (int position = 0; position < page.rows(); position++) {
                innerRowReferences[innerOrdinal++] = packRowReference(pageIndex, position);
            }
        }

        // Insert in reverse row order so each bucket chain preserves the source's
        // ascending row order, including duplicate-key multiplicity.
        for (innerOrdinal = innerRowReferences.length - 1; innerOrdinal >= 0; innerOrdinal--) {
            long reference = innerRowReferences[innerOrdinal];
            Streams[] columns = columns(pages, reference);
            int position = unpackPosition(reference);
            if (keyHasNull(columns, position, innerJoinColumns)) {
                hashNext[innerOrdinal] = NO_MATCH;
                continue;
            }
            int bucket = hashKey(columns, innerJoinColumns, position) & (hashHeads.length - 1);
            hashNext[innerOrdinal] = hashHeads[bucket];
            hashHeads[bucket] = innerOrdinal;
        }
    }

    private int hashKey(Streams[] columns, int[] joinColumns, int position)
    {
        int hash = 1;
        for (int keyIndex = 0; keyIndex < joinColumns.length; keyIndex++) {
            Streams key = columns[joinColumns[keyIndex]];
            hash = 31 * hash + Long.hashCode(
                    keyKernels[keyIndex].hash(key.values(), key.getOrNull(Stream.NULLS), position));
        }
        return hash ^ (hash >>> 16);
    }

    private static int hashTableSize(int rowCount)
    {
        int target = Math.max(1, rowCount);
        if (target == 1) {
            return 1;
        }
        if (target >= (1 << 30)) {
            return 1 << 30;
        }
        return Integer.highestOneBit(target - 1) << 1;
    }

    private Streams materializeOutputColumn(TypeBinding type, Streams schema, List<TableOperator.Page> pages, JoinedRows joinedRows, int rowCount, boolean useOuter, int outputIndex, long[] innerRowReferences)
    {
        Vector values = null;
        BooleanVector nulls = allocator.allocate(allocationContext, BooleanVector.class, rowCount, BooleanVector::new);
        BooleanVector errors = schema.has(Stream.ERRORS) ? allocator.allocate(allocationContext, BooleanVector.class, rowCount, BooleanVector::new) : null;

        for (int outputPosition = 0; outputPosition < rowCount; outputPosition++) {
            long rowReference = useOuter ? joinedRows.outerRowReference(outputPosition) : innerReferencesOrNoMatch(joinedRows.innerRowOrdinal(outputPosition), innerRowReferences);
            if (rowReference == NO_MATCH) {
                nulls.values()[outputPosition] = true;
                continue;
            }

            TableOperator.Page page = pages.get(unpackPageIndex(rowReference));
            int sourcePosition = unpackPosition(rowReference);
            Streams source = page.columns()[outputIndex];
            values = source.values().copySinglePositionInto(allocator, allocationContext, values, sourcePosition, outputPosition, rowCount);
            if (source.has(Stream.NULLS)) {
                nulls.values()[outputPosition] = OperatorVectorSupport.isNull(source.get(Stream.NULLS), sourcePosition);
            }
            if (errors != null && source.has(Stream.ERRORS)) {
                errors.values()[outputPosition] = OperatorVectorSupport.isNull(source.get(Stream.ERRORS), sourcePosition);
            }
        }

        if (values == null) {
            values = nullValues(type, schema.values(), rowCount);
        }
        Streams.Builder builder = Streams.builder()
                .put(Stream.VALUES, values)
                .put(Stream.NULLS, nulls);
        if (errors != null) {
            builder.put(Stream.ERRORS, errors);
        }
        return builder.build();
    }

    private long innerReferencesOrNoMatch(int innerRowOrdinal, long[] innerRowReferences)
    {
        if (innerRowOrdinal == NO_MATCH) {
            return NO_MATCH;
        }
        return innerRowReferences[innerRowOrdinal];
    }

    private MaterializedInput materialize(Operator source)
    {
        List<TableOperator.Page> pages = new ArrayList<>();
        Streams[] schema = new Streams[source.outputCount()];
        while (source.hasNext()) {
            try (Batch batch = source.next()) {
                Mask mask = batch.borrowMask();
                for (int outputIndex = 0; outputIndex < source.outputCount(); outputIndex++) {
                    if (schema[outputIndex] == null) {
                        schema[outputIndex] = emptyStreamsLike(batch.output(outputIndex));
                    }
                }
                if (mask.none()) {
                    continue;
                }
                Streams[] columns = new Streams[source.outputCount()];
                if (policy.retainInputBatches() && mask.all() && source.supportsRetainedBatches()) {
                    for (int outputIndex = 0; outputIndex < columns.length; outputIndex++) {
                        columns[outputIndex] = takeStreams(batch.output(outputIndex));
                    }
                }
                else {
                    for (int outputIndex = 0; outputIndex < columns.length; outputIndex++) {
                        columns[outputIndex] = allocator.copyStreams(allocationContext, borrowedStreams(batch.output(outputIndex)), mask);
                    }
                }
                pages.add(new TableOperator.Page(mask.count(), columns, Mask.all(mask.count())));
            }
        }
        return new MaterializedInput(schema, pages);
    }

    private Streams emptyStreamsLike(Output output)
    {
        Streams.Builder builder = Streams.builder();
        for (Stream stream : output.streams()) {
            builder.put(stream, output.borrow(stream).emptyLike(allocator, allocationContext));
        }
        return builder.build();
    }

    private static Streams borrowedStreams(Output output)
    {
        Streams.Builder builder = Streams.builder();
        for (Stream stream : output.streams()) {
            builder.put(stream, output.borrow(stream));
        }
        return builder.build();
    }

    private Streams takeStreams(Output output)
    {
        Streams.Builder builder = Streams.builder();
        for (Stream stream : output.streams()) {
            builder.put(stream, allocator.transfer(allocationContext, output.take(stream)));
        }
        return builder.build();
    }

    private Vector nullValues(TypeBinding type, Vector schemaValues, int size)
    {
        if (type.isSpecified() && type.vectorFactory().isPresent()) {
            Vector values = type.vectorFactory().orElseThrow()
                    .nullValues(allocator.vectorAllocator(allocationContext), size);
            if (values.length() != size) {
                throw new IllegalArgumentException("Type vector factory returned length %s for requested length %s"
                        .formatted(values.length(), size));
            }
            if (!type.supportsVector(values)) {
                throw new IllegalArgumentException("Type %s does not support factory result %s"
                        .formatted(type.identity(), values.getClass().getName()));
            }
            return values;
        }
        return legacyNullValuesLike(schemaValues, size);
    }

    private Vector legacyNullValuesLike(Vector schemaValues, int size)
    {
        return switch (schemaValues) {
            case I64Vector _ -> allocator.allocate(allocationContext, I64Vector.class, size, I64Vector::new);
            case I32Vector _ -> allocator.allocate(allocationContext, I32Vector.class, size, I32Vector::new);
            case F64Vector _ -> allocator.allocate(allocationContext, F64Vector.class, size, F64Vector::new);
            case BooleanVector _ -> allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new);
            case BinaryVector _ -> BinaryVector.allocate(allocator, allocationContext, size, 0);
            default -> throw new IllegalArgumentException("Unsupported FullJoinOperator output type: " + schemaValues.getClass().getSimpleName());
        };
    }

    private static int countRows(List<TableOperator.Page> pages)
    {
        int rows = 0;
        for (TableOperator.Page page : pages) {
            rows += page.rows();
        }
        return rows;
    }

    private static long packRowReference(int pageIndex, int position)
    {
        return (((long) pageIndex) << 32) | (position & 0xFFFF_FFFFL);
    }

    private static int unpackPageIndex(long rowReference)
    {
        return (int) (rowReference >>> 32);
    }

    private static int unpackPosition(long rowReference)
    {
        return (int) rowReference;
    }

    private record MaterializedInput(Streams[] schema, List<TableOperator.Page> pages) {}

    private static final class JoinedRows
            implements AutoCloseable
    {
        private final PrimitiveArrayPool arrayPool;
        private final List<long[]> outerRowReferences = new ArrayList<>();
        private final List<int[]> innerRowOrdinals = new ArrayList<>();
        private int size;

        private JoinedRows(PrimitiveArrayPool arrayPool)
        {
            this.arrayPool = arrayPool;
        }

        public void add(long outerRowReference, int innerRowOrdinal)
        {
            int chunkOffset = size & JOINED_ROW_CHUNK_MASK;
            if (chunkOffset == 0) {
                outerRowReferences.add(arrayPool.borrowLongs(JOINED_ROW_CHUNK_SIZE));
                innerRowOrdinals.add(arrayPool.borrowInts(JOINED_ROW_CHUNK_SIZE));
            }
            int chunkIndex = size >>> JOINED_ROW_CHUNK_SHIFT;
            outerRowReferences.get(chunkIndex)[chunkOffset] = outerRowReference;
            innerRowOrdinals.get(chunkIndex)[chunkOffset] = innerRowOrdinal;
            size++;
        }

        public int size()
        {
            return size;
        }

        public long outerRowReference(int position)
        {
            return outerRowReferences.get(position >>> JOINED_ROW_CHUNK_SHIFT)[position & JOINED_ROW_CHUNK_MASK];
        }

        public int innerRowOrdinal(int position)
        {
            return innerRowOrdinals.get(position >>> JOINED_ROW_CHUNK_SHIFT)[position & JOINED_ROW_CHUNK_MASK];
        }

        @Override
        public void close()
        {
            outerRowReferences.forEach(arrayPool::release);
            innerRowOrdinals.forEach(arrayPool::release);
            outerRowReferences.clear();
            innerRowOrdinals.clear();
            size = 0;
        }
    }
}
