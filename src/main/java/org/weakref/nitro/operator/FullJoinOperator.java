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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class FullJoinOperator
        implements Operator
{
    private static final int NO_MATCH = -1;

    private final Allocator allocator;
    private final Allocator.Context allocationContext = new Allocator.Context("FullJoinOperator");
    private final Operator outer;
    private final Operator inner;
    private final int[] outerJoinColumns;
    private final int[] innerJoinColumns;

    private Streams[] materialized;
    private Mask outputMask;
    private boolean loaded;
    private boolean done;

    public FullJoinOperator(Allocator allocator, Operator outer, int[] outerJoinColumns, Operator inner, int[] innerJoinColumns)
    {
        if (outerJoinColumns.length == 0) {
            throw new IllegalArgumentException("FullJoinOperator requires at least one join key");
        }
        if (outerJoinColumns.length != innerJoinColumns.length) {
            throw new IllegalArgumentException("Join key counts must match");
        }
        this.allocator = allocator;
        this.outer = outer;
        this.inner = inner;
        this.outerJoinColumns = outerJoinColumns.clone();
        this.innerJoinColumns = innerJoinColumns.clone();
    }

    @Override
    public int outputCount()
    {
        return outer.outputCount() + inner.outputCount();
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

        Map<OperatorKeySemantics.Key, IntArrayList> innerMatches = new HashMap<>();
        int innerRowCount = countRows(innerInput.pages());
        long[] innerRowReferences = new long[innerRowCount];
        boolean[] matchedInnerRows = new boolean[innerRowCount];
        indexInnerRows(innerInput.pages(), innerMatches, innerRowReferences);

        List<JoinedRow> joinedRows = new ArrayList<>();
        for (int outerPageIndex = 0; outerPageIndex < outerInput.pages().size(); outerPageIndex++) {
            TableOperator.Page page = outerInput.pages().get(outerPageIndex);
            OperatorKeySemantics.Key[] reusableKeys = new OperatorKeySemantics.Key[outerJoinColumns.length];
            OperatorKeySemantics.CompositeProbeKey reusableCompositeKey = OperatorKeySemantics.reusableCompositeProbeKey(outerJoinColumns.length);
            for (int position = 0; position < page.rows(); position++) {
                OperatorKeySemantics.Key key = probeKey(page.columns(), outerJoinColumns, position, reusableKeys, reusableCompositeKey);
                if (key == null) {
                    joinedRows.add(new JoinedRow(packRowReference(outerPageIndex, position), NO_MATCH));
                    continue;
                }
                IntArrayList matches = innerMatches.get(key);
                if (matches == null || matches.isEmpty()) {
                    joinedRows.add(new JoinedRow(packRowReference(outerPageIndex, position), NO_MATCH));
                    continue;
                }
                for (int index = 0; index < matches.size(); index++) {
                    int innerOrdinal = matches.getInt(index);
                    matchedInnerRows[innerOrdinal] = true;
                    joinedRows.add(new JoinedRow(packRowReference(outerPageIndex, position), innerOrdinal));
                }
            }
        }

        for (int innerOrdinal = 0; innerOrdinal < matchedInnerRows.length; innerOrdinal++) {
            if (!matchedInnerRows[innerOrdinal]) {
                joinedRows.add(new JoinedRow(NO_MATCH, innerOrdinal));
            }
        }

        int rowCount = joinedRows.size();
        materialized = new Streams[outputCount()];
        for (int outputIndex = 0; outputIndex < outer.outputCount(); outputIndex++) {
            materialized[outputIndex] = materializeOutputColumn(outerInput.schema()[outputIndex], outerInput.pages(), joinedRows, rowCount, true, outputIndex, innerRowReferences);
        }
        for (int outputIndex = 0; outputIndex < inner.outputCount(); outputIndex++) {
            materialized[outer.outputCount() + outputIndex] = materializeOutputColumn(innerInput.schema()[outputIndex], innerInput.pages(), joinedRows, rowCount, false, outputIndex, innerRowReferences);
        }
        outputMask = allocator.allocateRangeMask(allocationContext, 0, rowCount);
    }

    private void indexInnerRows(List<TableOperator.Page> pages, Map<OperatorKeySemantics.Key, IntArrayList> innerMatches, long[] innerRowReferences)
    {
        int innerOrdinal = 0;
        for (int pageIndex = 0; pageIndex < pages.size(); pageIndex++) {
            TableOperator.Page page = pages.get(pageIndex);
            OperatorKeySemantics.Key[] reusableKeys = new OperatorKeySemantics.Key[innerJoinColumns.length];
            OperatorKeySemantics.CompositeProbeKey reusableCompositeKey = OperatorKeySemantics.reusableCompositeProbeKey(innerJoinColumns.length);
            for (int position = 0; position < page.rows(); position++) {
                innerRowReferences[innerOrdinal] = packRowReference(pageIndex, position);
                OperatorKeySemantics.Key key = probeKey(page.columns(), innerJoinColumns, position, reusableKeys, reusableCompositeKey);
                if (key != null) {
                    innerMatches.computeIfAbsent(OperatorKeySemantics.ownedKey(key), ignored -> new IntArrayList())
                            .add(innerOrdinal);
                }
                innerOrdinal++;
            }
        }
    }

    private Streams materializeOutputColumn(Streams schema, List<TableOperator.Page> pages, List<JoinedRow> joinedRows, int rowCount, boolean useOuter, int outputIndex, long[] innerRowReferences)
    {
        Vector values = valuesLike(schema.values(), rowCount);
        BooleanVector nulls = allocator.allocate(allocationContext, BooleanVector.class, rowCount, BooleanVector::new);
        BooleanVector errors = schema.has(Stream.ERRORS) ? allocator.allocate(allocationContext, BooleanVector.class, rowCount, BooleanVector::new) : null;

        for (int outputPosition = 0; outputPosition < joinedRows.size(); outputPosition++) {
            JoinedRow row = joinedRows.get(outputPosition);
            long rowReference = useOuter ? row.outerRowReference() : innerReferencesOrNoMatch(row, innerRowReferences);
            if (rowReference == NO_MATCH) {
                nulls.values()[outputPosition] = true;
                continue;
            }

            TableOperator.Page page = pages.get(unpackPageIndex(rowReference));
            int sourcePosition = unpackPosition(rowReference);
            Streams source = page.columns()[outputIndex];
            values = source.values().copySinglePositionInto(allocator, allocationContext, values, sourcePosition, outputPosition, rowCount);
            if (source.has(Stream.NULLS)) {
                nulls.values()[outputPosition] = ((BooleanVector) source.get(Stream.NULLS)).values()[sourcePosition];
            }
            if (errors != null && source.has(Stream.ERRORS)) {
                errors.values()[outputPosition] = ((BooleanVector) source.get(Stream.ERRORS)).values()[sourcePosition];
            }
        }

        Streams.Builder builder = Streams.builder()
                .put(Stream.VALUES, values)
                .put(Stream.NULLS, nulls);
        if (errors != null) {
            builder.put(Stream.ERRORS, errors);
        }
        return builder.build();
    }

    private long innerReferencesOrNoMatch(JoinedRow row, long[] innerRowReferences)
    {
        if (row.innerRowOrdinal() == NO_MATCH) {
            return NO_MATCH;
        }
        return innerRowReferences[row.innerRowOrdinal()];
    }

    private OperatorKeySemantics.Key probeKey(Streams[] columns, int[] joinColumns, int position, OperatorKeySemantics.Key[] reusableKeys, OperatorKeySemantics.CompositeProbeKey reusableCompositeKey)
    {
        for (int keyIndex = 0; keyIndex < joinColumns.length; keyIndex++) {
            Streams column = columns[joinColumns[keyIndex]];
            Vector values = column.values();
            reusableKeys[keyIndex] = reusableKeys[keyIndex] == null ? OperatorKeySemantics.reusableProbeKey(values) : reusableKeys[keyIndex];
            reusableKeys[keyIndex] = OperatorKeySemantics.probeKey(values, (BooleanVector) column.getOrNull(Stream.NULLS), position, reusableKeys[keyIndex]);
            if (reusableKeys[keyIndex] == null) {
                return null;
            }
        }
        return OperatorKeySemantics.probeCompositeKey(reusableKeys, reusableCompositeKey);
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
                for (int outputIndex = 0; outputIndex < columns.length; outputIndex++) {
                    columns[outputIndex] = allocator.copyStreams(allocationContext, borrowedStreams(batch.output(outputIndex)), mask);
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

    private Vector valuesLike(Vector schemaValues, int size)
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

    private record JoinedRow(long outerRowReference, int innerRowOrdinal) {}
}
