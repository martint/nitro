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
package org.weakref.nitro.parquet;

import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.RowGroup;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.ArrayVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.StructVector;

import java.util.List;

import static java.util.Objects.requireNonNull;

/** Reconstructs a standard three-level Parquet LIST from one physical-leaf event stream. */
final class NestedArrayReader
        implements AutoCloseable
{
    private final ParquetSchema.Group list;
    private final ParquetSchema.Group repeatedValues;
    private final ElementReader elements;

    private boolean positioned;
    private boolean exhausted;

    NestedArrayReader(ParquetSchema.Group list, RleReaderPolicy rlePolicy, PrimitiveArrayPool arrayPool)
    {
        this(list, rlePolicy, ParquetMaterializationPolicy.defaults(), arrayPool, null, null, null);
    }

    NestedArrayReader(
            ParquetSchema.Group list,
            RleReaderPolicy rlePolicy,
            ParquetMaterializationPolicy materializationPolicy,
            PrimitiveArrayPool arrayPool)
    {
        this(list, rlePolicy, materializationPolicy, arrayPool, null, null, null);
    }

    NestedArrayReader(
            ParquetSchema.Group list,
            RleReaderPolicy rlePolicy,
            PrimitiveArrayPool arrayPool,
            TypeBinding outputType,
            ParquetValueBinding.Group logicalBinding)
    {
        this(list, rlePolicy, ParquetMaterializationPolicy.defaults(), arrayPool, null, outputType, logicalBinding);
    }

    NestedArrayReader(
            ParquetSchema.Group list,
            RleReaderPolicy rlePolicy,
            ParquetMaterializationPolicy materializationPolicy,
            PrimitiveArrayPool arrayPool,
            TypeBinding outputType,
            ParquetValueBinding.Group logicalBinding)
    {
        this(list, rlePolicy, materializationPolicy, arrayPool, null, outputType, logicalBinding);
    }

    NestedArrayReader(ParquetSchema.Group list, RleReaderPolicy rlePolicy, NestedLeafCursor elementCursor)
    {
        this(list, rlePolicy, ParquetMaterializationPolicy.defaults(), null, new NestedLeafCursor[] {elementCursor}, null, null);
    }

    NestedArrayReader(ParquetSchema.Group list, RleReaderPolicy rlePolicy, NestedLeafCursor[] elementCursors)
    {
        this(list, rlePolicy, ParquetMaterializationPolicy.defaults(), null, elementCursors, null, null);
    }

    NestedArrayReader(
            ParquetSchema.Group list,
            RleReaderPolicy rlePolicy,
            NestedLeafCursor[] elementCursors,
            TypeBinding outputType,
            ParquetValueBinding.Group logicalBinding)
    {
        this(list, rlePolicy, ParquetMaterializationPolicy.defaults(), null, elementCursors, outputType, logicalBinding);
    }

    private NestedArrayReader(
            ParquetSchema.Group list,
            RleReaderPolicy rlePolicy,
            ParquetMaterializationPolicy materializationPolicy,
            PrimitiveArrayPool arrayPool,
            NestedLeafCursor[] elementCursors,
            TypeBinding outputType,
            ParquetValueBinding.Group logicalBinding)
    {
        this.list = requireNonNull(list, "list is null");
        if (list.isList()) {
            if (list.children().size() != 1 || !(list.children().getFirst() instanceof ParquetSchema.Group repeatedGroup)) {
                throw unsupported("LIST must contain one repeated element group");
            }
            this.repeatedValues = repeatedGroup;
        }
        else if (list.repetition() == FieldRepetitionType.REPEATED) {
            this.repeatedValues = list;
        }
        else {
            throw unsupported("field is neither annotated as LIST nor a legacy repeated group");
        }
        if (repeatedValues.repetition() != FieldRepetitionType.REPEATED || repeatedValues.children().size() != 1) {
            throw unsupported("LIST element group must be repeated and contain exactly one field");
        }
        ParquetSchema.Node element = repeatedValues.children().getFirst();
        if (element.repetition() == FieldRepetitionType.REPEATED) {
            throw unsupported("LIST element cannot be repeated");
        }
        if ((outputType == null) != (logicalBinding == null)) {
            throw new IllegalArgumentException("LIST output type and logical binding must be supplied together");
        }
        NestedLogicalBindings bindings = logicalBinding == null
                ? null
                : NestedLogicalBindings.require(list.name(), outputType, logicalBinding, 1);
        this.elements = switch (element) {
            case ParquetSchema.Primitive primitive -> new PrimitiveElementReader(
                    primitive,
                    rlePolicy,
                    materializationPolicy,
                    arrayPool,
                    elementCursors,
                    bindings == null ? null : bindings.childPrimitive(0, primitive));
            case ParquetSchema.Group group when !group.isList() && !group.isMap() -> new StructElementReader(
                    group,
                    rlePolicy,
                    materializationPolicy,
                    arrayPool,
                    elementCursors,
                    bindings == null ? null : bindings.childGroup(0, group));
            case ParquetSchema.Group group -> throw unsupported(
                    "nested " + (group.isList() ? "LIST" : "MAP") + " elements are not implemented yet");
        };
    }

    void addRowGroup(ParquetFile file, RowGroup rowGroup)
    {
        requireNonNull(file, "file is null");
        requireNonNull(rowGroup, "rowGroup is null");
        elements.addRowGroup(file, rowGroup);
    }

    Streams read(Allocator allocator, Allocator.Context context, int rowCount, Mask mask)
    {
        requireNonNull(allocator, "allocator is null");
        requireNonNull(context, "context is null");
        requireNonNull(mask, "mask is null");
        if (rowCount < 0 || mask.size() != rowCount) {
            throw new IllegalArgumentException("Nested LIST row count and mask length differ: " + rowCount + " != " + mask.size());
        }

        elements.reset(allocator);
        ArrayVector arrays = allocator.allocateArray(context, rowCount);
        BooleanVector listNulls = list.repetition() == FieldRepetitionType.OPTIONAL
                ? allocator.allocate(context, BooleanVector.class, rowCount, BooleanVector::new)
                : null;

        if (rowCount == 0) {
            arrays.setElements(elements.materialize(allocator, context));
            return listNulls == null ? Streams.ofValues(arrays) : Streams.ofValuesAndNulls(arrays, listNulls);
        }

        if (!positioned && elements.eventSource() instanceof NestedLeafEventSource eventSource) {
            readEventWindows(eventSource, arrays, listNulls, rowCount, mask);
            arrays.setElements(elements.materialize(allocator, context));
            return listNulls == null ? Streams.ofValues(arrays) : Streams.ofValuesAndNulls(arrays, listNulls);
        }

        int selectedIndex = 0;
        int nextSelected = mask.all() ? 0 : (mask.count() == 0 ? rowCount : mask.position(0));
        ensurePositioned();
        for (int row = 0; row < rowCount; row++) {
            if (exhausted) {
                throw new IllegalArgumentException("Nested LIST event stream ended before row " + row);
            }
            if (elements.repetitionLevel() != 0) {
                throw new IllegalArgumentException("Nested LIST row starts with nonzero repetition level");
            }

            boolean selected = mask.all() || row == nextSelected;
            boolean listIsNull = elements.definitionLevel() < list.maximumDefinitionLevel();
            if (selected && listNulls != null) {
                listNulls.values()[row] = listIsNull;
            }

            do {
                boolean hasElement = elements.definitionLevel() >= repeatedValues.maximumDefinitionLevel();
                if (selected && hasElement) {
                    elements.appendCurrent();
                }
                advance();
            }
            while (!exhausted && elements.repetitionLevel() != 0);

            arrays.offsets()[row + 1] = elements.size();
            if (selected && !mask.all()) {
                selectedIndex++;
                nextSelected = selectedIndex < mask.count() ? mask.position(selectedIndex) : rowCount;
            }
        }

        arrays.setElements(elements.materialize(allocator, context));
        return listNulls == null ? Streams.ofValues(arrays) : Streams.ofValuesAndNulls(arrays, listNulls);
    }

    private void readEventWindows(
            NestedLeafEventSource reader,
            ArrayVector arrays,
            BooleanVector listNulls,
            int rowCount,
            Mask mask)
    {
        if (mask.all()) {
            readAllEventWindows(reader, arrays, listNulls, rowCount);
            return;
        }

        int row = -1;
        int selectedIndex = 0;
        int nextSelected = mask.count() == 0 ? rowCount : mask.position(0);
        boolean selected = false;

        while (true) {
            NestedEventWindow window = reader.eventWindow();
            if (window == null) {
                if (row == rowCount - 1) {
                    arrays.offsets()[rowCount] = elements.size();
                    return;
                }
                throw new IllegalArgumentException("Nested LIST event stream ended before row " + (row + 1));
            }

            int consumed = 0;
            int elementRunStart = -1;
            int elementRunCount = 0;
            while (consumed < window.length()) {
                int repetitionLevel = window.repetitionLevel(consumed);
                if (repetitionLevel == 0) {
                    if (elementRunCount != 0) {
                        elements.appendWindow(window, elementRunStart, elementRunCount);
                        elementRunCount = 0;
                    }
                    if (row >= 0) {
                        arrays.offsets()[row + 1] = elements.size();
                        if (row + 1 == rowCount) {
                            reader.advanceEvents(consumed);
                            return;
                        }
                    }
                    row++;
                    selected = row == nextSelected;
                    if (selected) {
                        selectedIndex++;
                        nextSelected = selectedIndex < mask.count() ? mask.position(selectedIndex) : rowCount;
                    }
                    if (selected && listNulls != null) {
                        listNulls.values()[row] = window.definitionLevel(consumed) < list.maximumDefinitionLevel();
                    }
                }
                else if (row < 0) {
                    throw new IllegalArgumentException("Nested LIST row starts with nonzero repetition level");
                }

                boolean hasElement = window.definitionLevel(consumed) >= repeatedValues.maximumDefinitionLevel();
                if (selected && hasElement) {
                    if (elementRunCount == 0) {
                        elementRunStart = consumed;
                    }
                    elementRunCount++;
                }
                else if (elementRunCount != 0) {
                    elements.appendWindow(window, elementRunStart, elementRunCount);
                    elementRunCount = 0;
                }
                consumed++;
            }
            if (elementRunCount != 0) {
                elements.appendWindow(window, elementRunStart, elementRunCount);
            }
            reader.advanceEvents(consumed);
        }
    }

    private void readAllEventWindows(
            NestedLeafEventSource reader,
            ArrayVector arrays,
            BooleanVector listNulls,
            int rowCount)
    {
        int row = -1;
        int outputElementCount = 0;
        int elementDefinitionLevel = repeatedValues.maximumDefinitionLevel();
        int listDefinitionLevel = list.maximumDefinitionLevel();

        while (true) {
            NestedEventWindow window = reader.eventWindow();
            if (window == null) {
                if (row == rowCount - 1) {
                    arrays.offsets()[rowCount] = outputElementCount;
                    return;
                }
                throw new IllegalArgumentException("Nested LIST event stream ended before row " + (row + 1));
            }

            int windowLength = window.length();
            if (window.allDefinitionLevelsAtLeast(windowLength, elementDefinitionLevel)) {
                int consumed = 0;
                while (consumed < windowLength) {
                    if (window.repetitionLevel(consumed) == 0) {
                        if (row >= 0) {
                            arrays.offsets()[row + 1] = outputElementCount + consumed;
                            if (row + 1 == rowCount) {
                                elements.appendWindow(window, 0, consumed);
                                reader.advanceEvents(consumed);
                                return;
                            }
                        }
                        row++;
                    }
                    else if (row < 0) {
                        throw new IllegalArgumentException("Nested LIST row starts with nonzero repetition level");
                    }
                    consumed++;
                }
                elements.appendWindow(window, 0, consumed);
                outputElementCount += consumed;
                reader.advanceEvents(consumed);
                continue;
            }

            int consumed = 0;
            int elementRunStart = -1;
            int elementRunCount = 0;
            while (consumed < windowLength) {
                int repetitionLevel = window.repetitionLevel(consumed);
                if (repetitionLevel == 0) {
                    if (row >= 0) {
                        arrays.offsets()[row + 1] = outputElementCount;
                        if (row + 1 == rowCount) {
                            if (elementRunCount != 0) {
                                elements.appendWindow(window, elementRunStart, elementRunCount);
                            }
                            reader.advanceEvents(consumed);
                            return;
                        }
                    }
                    row++;
                    if (listNulls != null) {
                        listNulls.values()[row] = window.definitionLevel(consumed) < listDefinitionLevel;
                    }
                }
                else if (row < 0) {
                    throw new IllegalArgumentException("Nested LIST row starts with nonzero repetition level");
                }

                if (window.definitionLevel(consumed) >= elementDefinitionLevel) {
                    if (elementRunCount == 0) {
                        elementRunStart = consumed;
                    }
                    elementRunCount++;
                    outputElementCount++;
                }
                else if (elementRunCount != 0) {
                    elements.appendWindow(window, elementRunStart, elementRunCount);
                    elementRunCount = 0;
                }
                consumed++;
            }
            if (elementRunCount != 0) {
                elements.appendWindow(window, elementRunStart, elementRunCount);
            }
            reader.advanceEvents(consumed);
        }
    }

    void skip(long rowCount)
    {
        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount is negative");
        }
        ensurePositioned();
        for (long row = 0; row < rowCount; row++) {
            if (exhausted) {
                throw new IllegalArgumentException("Nested LIST event stream ended before skipped row " + row);
            }
            if (elements.repetitionLevel() != 0) {
                throw new IllegalArgumentException("Nested LIST row starts with nonzero repetition level");
            }
            do {
                advance();
            }
            while (!exhausted && elements.repetitionLevel() != 0);
        }
    }

    long consumedPageBytes()
    {
        return elements.consumedPageBytes();
    }

    private void ensurePositioned()
    {
        if (!positioned) {
            positioned = true;
            advance();
        }
    }

    private void advance()
    {
        exhausted = !elements.next();
    }

    private UnsupportedParquetFeatureException unsupported(String reason)
    {
        return new UnsupportedParquetFeatureException(
                "Unsupported native Parquet LIST layout at '" + list.name() + "': " + reason);
    }

    @Override
    public void close()
    {
        elements.close();
    }

    private interface ElementReader
            extends AutoCloseable
    {
        void addRowGroup(ParquetFile file, RowGroup rowGroup);

        void reset(Allocator allocator);

        boolean next();

        int repetitionLevel();

        int definitionLevel();

        void appendCurrent();

        default NestedLeafEventSource eventSource()
        {
            return null;
        }

        default void appendWindow(NestedEventWindow window, int offset, int count)
        {
            throw new UnsupportedOperationException("element reader does not support event windows");
        }

        int size();

        Streams materialize(Allocator allocator, Allocator.Context context);

        long consumedPageBytes();

        @Override
        void close();
    }

    private static final class PrimitiveElementReader
            implements ElementReader
    {
        private final ParquetSchema.Primitive element;
        private final NestedLeafCursor reader;
        private final NestedValueAccumulator values;

        private PrimitiveElementReader(
                ParquetSchema.Primitive element,
                RleReaderPolicy rlePolicy,
                ParquetMaterializationPolicy materializationPolicy,
                PrimitiveArrayPool arrayPool,
                NestedLeafCursor[] cursors,
                NestedLogicalBindings.Primitive logicalBinding)
        {
            this.element = requireNonNull(element, "element is null");
            if (cursors != null && cursors.length != 1) {
                throw new IllegalArgumentException("Primitive LIST element requires exactly one cursor");
            }
            this.reader = cursors == null
                    ? new NestedLeafReader(element, rlePolicy, requireNonNull(arrayPool, "arrayPool is null"))
                    : requireNonNull(cursors[0], "cursor is null");
            this.values = logicalBinding == null
                    ? NestedValueAccumulators.create(
                            element,
                            element.repetition() == FieldRepetitionType.OPTIONAL,
                            materializationPolicy)
                    : NestedValueAccumulators.create(
                            element,
                            element.repetition() == FieldRepetitionType.OPTIONAL,
                            logicalBinding.type(),
                            logicalBinding.value(),
                            materializationPolicy);
        }

        @Override
        public void addRowGroup(ParquetFile file, RowGroup rowGroup)
        {
            if (!(reader instanceof NestedLeafReader physicalReader)) {
                throw new IllegalStateException("Cannot add Parquet row groups to an injected nested cursor");
            }
            physicalReader.addChunk(file, file.columnChunk(rowGroup, element).meta_data);
        }

        @Override
        public void reset(Allocator allocator)
        {
            values.reset(allocator);
        }

        @Override
        public boolean next()
        {
            return reader.next();
        }

        @Override
        public int repetitionLevel()
        {
            return reader.repetitionLevel();
        }

        @Override
        public int definitionLevel()
        {
            return reader.definitionLevel();
        }

        @Override
        public void appendCurrent()
        {
            if (reader.hasValue()) {
                values.append(reader.valueDecoder(), reader.valueOrdinal(), reader.dictionaryId());
            }
            else {
                values.appendNull();
            }
        }

        @Override
        public NestedLeafEventSource eventSource()
        {
            return reader instanceof NestedLeafEventSource source ? source : null;
        }

        @Override
        public void appendWindow(NestedEventWindow window, int offset, int count)
        {
            window.appendTo(values, offset, count);
        }

        @Override
        public int size()
        {
            return values.size();
        }

        @Override
        public Streams materialize(Allocator allocator, Allocator.Context context)
        {
            return values.materialize(allocator, context);
        }

        @Override
        public long consumedPageBytes()
        {
            return reader instanceof NestedLeafReader physicalReader ? physicalReader.consumedPageBytes() : 0;
        }

        @Override
        public void close()
        {
            try (reader; values) {
                // Closing releases the physical leaf and allocator-owned accumulation buffers.
            }
        }
    }

    private static final class StructElementReader
            implements ElementReader
    {
        private final ParquetSchema.Group element;
        private final List<ParquetSchema.Primitive> fields;
        private final NestedLeafCursor[] readers;
        private final NestedValueAccumulator[] values;
        private final NullAccumulator nulls = new NullAccumulator();

        private StructElementReader(
                ParquetSchema.Group element,
                RleReaderPolicy rlePolicy,
                ParquetMaterializationPolicy materializationPolicy,
                PrimitiveArrayPool arrayPool,
                NestedLeafCursor[] cursors,
                NestedLogicalBindings.Group logicalBinding)
        {
            this.element = requireNonNull(element, "element is null");
            if (element.children().stream().anyMatch(child -> !(child instanceof ParquetSchema.Primitive))) {
                throw new UnsupportedParquetFeatureException(
                        "Native Parquet LIST struct element at '" + element.name() + "' contains a nested field");
            }
            this.fields = element.children().stream().map(ParquetSchema.Primitive.class::cast).toList();
            if (fields.isEmpty()) {
                throw new UnsupportedParquetFeatureException(
                        "Native Parquet LIST struct element at '" + element.name() + "' has no fields");
            }
            if (cursors != null && cursors.length != fields.size()) {
                throw new IllegalArgumentException("LIST struct element cursor count does not match field count");
            }
            NestedLogicalBindings bindings = logicalBinding == null
                    ? null
                    : NestedLogicalBindings.require(
                            element.name(), logicalBinding.type(), logicalBinding.value(), fields.size());
            this.readers = new NestedLeafCursor[fields.size()];
            this.values = new NestedValueAccumulator[fields.size()];
            for (int field = 0; field < fields.size(); field++) {
                ParquetSchema.Primitive leaf = fields.get(field);
                readers[field] = cursors == null
                        ? new NestedLeafReader(leaf, rlePolicy, requireNonNull(arrayPool, "arrayPool is null"))
                        : requireNonNull(cursors[field], "cursor is null");
                if (bindings == null) {
                    values[field] = NestedValueAccumulators.create(leaf, true, materializationPolicy);
                }
                else {
                    NestedLogicalBindings.Primitive child = bindings.childPrimitive(field, leaf);
                    values[field] = NestedValueAccumulators.create(
                            leaf, true, child.type(), child.value(), materializationPolicy);
                }
            }
        }

        @Override
        public void addRowGroup(ParquetFile file, RowGroup rowGroup)
        {
            for (int field = 0; field < fields.size(); field++) {
                if (!(readers[field] instanceof NestedLeafReader physicalReader)) {
                    throw new IllegalStateException("Cannot add Parquet row groups to injected nested cursors");
                }
                physicalReader.addChunk(file, file.columnChunk(rowGroup, fields.get(field)).meta_data);
            }
        }

        @Override
        public void reset(Allocator allocator)
        {
            for (NestedValueAccumulator value : values) {
                value.reset(allocator);
            }
            nulls.reset(allocator);
        }

        @Override
        public boolean next()
        {
            boolean available = readers[0].next();
            for (int field = 1; field < readers.length; field++) {
                if (readers[field].next() != available) {
                    throw new IllegalArgumentException("LIST struct leaf event streams have different lengths");
                }
            }
            if (available) {
                int repetitionLevel = readers[0].repetitionLevel();
                boolean elementIsNull = readers[0].definitionLevel() < element.maximumDefinitionLevel();
                for (int field = 1; field < readers.length; field++) {
                    if (readers[field].repetitionLevel() != repetitionLevel) {
                        throw new IllegalArgumentException("LIST struct leaves disagree about repetition boundaries");
                    }
                    if ((readers[field].definitionLevel() < element.maximumDefinitionLevel()) != elementIsNull) {
                        throw new IllegalArgumentException("LIST struct leaves disagree about element presence");
                    }
                }
            }
            return available;
        }

        @Override
        public int repetitionLevel()
        {
            return readers[0].repetitionLevel();
        }

        @Override
        public int definitionLevel()
        {
            return readers[0].definitionLevel();
        }

        @Override
        public void appendCurrent()
        {
            int repetitionLevel = readers[0].repetitionLevel();
            boolean elementIsNull = readers[0].definitionLevel() < element.maximumDefinitionLevel();
            nulls.append(elementIsNull);
            for (int field = 0; field < readers.length; field++) {
                NestedLeafCursor reader = readers[field];
                if (reader.repetitionLevel() != repetitionLevel) {
                    throw new IllegalArgumentException("LIST struct leaves disagree about repetition boundaries");
                }
                if ((reader.definitionLevel() < element.maximumDefinitionLevel()) != elementIsNull) {
                    throw new IllegalArgumentException("LIST struct leaves disagree about element presence");
                }
                if (reader.hasValue()) {
                    values[field].append(reader.valueDecoder(), reader.valueOrdinal(), reader.dictionaryId());
                }
                else {
                    values[field].appendNull();
                }
            }
        }

        @Override
        public int size()
        {
            return values[0].size();
        }

        @Override
        public Streams materialize(Allocator allocator, Allocator.Context context)
        {
            StructVector structs = allocator.allocate(context, StructVector.class, size(), StructVector::new);
            for (int field = 0; field < fields.size(); field++) {
                structs.setField(fields.get(field).name(), values[field].materialize(allocator, context));
            }
            if (element.repetition() == FieldRepetitionType.REQUIRED) {
                return Streams.ofValues(structs);
            }
            return Streams.builder()
                    .put(Stream.VALUES, structs)
                    .put(Stream.NULLS, nulls.materialize(allocator, context))
                    .build();
        }

        @Override
        public long consumedPageBytes()
        {
            long bytes = 0;
            for (NestedLeafCursor reader : readers) {
                if (reader instanceof NestedLeafReader physicalReader) {
                    bytes = Math.addExact(bytes, physicalReader.consumedPageBytes());
                }
            }
            return bytes;
        }

        @Override
        public void close()
        {
            RuntimeException failure = null;
            for (NestedLeafCursor reader : readers) {
                try {
                    reader.close();
                }
                catch (RuntimeException e) {
                    failure = merge(failure, e);
                }
            }
            for (NestedValueAccumulator value : values) {
                try {
                    value.close();
                }
                catch (RuntimeException e) {
                    failure = merge(failure, e);
                }
            }
            try {
                nulls.close();
            }
            catch (RuntimeException e) {
                failure = merge(failure, e);
            }
            if (failure != null) {
                throw failure;
            }
        }

        private static RuntimeException merge(RuntimeException failure, RuntimeException next)
        {
            if (failure == null) {
                return next;
            }
            failure.addSuppressed(next);
            return failure;
        }
    }

    private static final class NullAccumulator
            implements AutoCloseable
    {
        private static final boolean[] EMPTY = new boolean[0];

        private PrimitiveArrayPool arrayPool;
        private boolean[] values = EMPTY;
        private int size;

        void reset(Allocator allocator)
        {
            PrimitiveArrayPool requestedPool = requireNonNull(allocator, "allocator is null").primitiveArrays();
            if (arrayPool != null && arrayPool != requestedPool) {
                throw new IllegalArgumentException("Nested null accumulator cannot change allocator ownership");
            }
            arrayPool = requestedPool;
            size = 0;
        }

        void append(boolean value)
        {
            ensureCapacity(size + 1);
            values[size++] = value;
        }

        BooleanVector materialize(Allocator allocator, Allocator.Context context)
        {
            BooleanVector result = allocator.allocate(context, BooleanVector.class, size, BooleanVector::new);
            System.arraycopy(values, 0, result.values(), 0, size);
            return result;
        }

        private void ensureCapacity(int required)
        {
            if (values.length >= required) {
                return;
            }
            int capacity = Math.max(required, Math.max(16, values.length * 2));
            boolean[] replacement = arrayPool.borrowBooleans(capacity);
            System.arraycopy(values, 0, replacement, 0, size);
            arrayPool.release(values);
            values = replacement;
        }

        @Override
        public void close()
        {
            if (arrayPool != null) {
                arrayPool.release(values);
                values = EMPTY;
                arrayPool = null;
                size = 0;
            }
        }
    }
}
