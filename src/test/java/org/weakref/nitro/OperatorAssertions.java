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
package org.weakref.nitro;

import org.assertj.core.api.AssertProvider;
import org.assertj.core.api.Descriptable;
import org.assertj.core.description.Description;
import org.weakref.nitro.data.ArrayVector;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.MapVector;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.data.StructVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Output;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class OperatorAssertions
{
    private OperatorAssertions() {}

    public static AssertProvider<OperatorAssert> operator(Operator operator)
    {
        return () -> new OperatorAssert(operator);
    }

    public static class OperatorAssert
            implements Descriptable<OperatorAssert>
    {
        private final Operator operator;
        private Description description;

        public OperatorAssert(Operator operator)
        {
            this.operator = operator;
        }

        @Override
        public OperatorAssert describedAs(Description description)
        {
            this.description = description;
            return this;
        }

        public void matchesExactly(List<Row> expected)
        {
            List<Row> actual = toRows(operator);
            assertThat(actual)
                    .describedAs(description)
                    .containsExactlyElementsOf(expected);
        }

        public void matches(List<Row> expected)
        {
            List<Row> actual = toRows(operator);
            assertThat(actual)
                    .describedAs(description)
                    .containsExactlyInAnyOrderElementsOf(expected);
        }

        public static List<Row> toRows(Operator operator)
        {
            List<Row> result = new ArrayList<>();
            while (operator.hasNext()) {
                Batch batch = operator.next();
                var mask = batch.borrowMask();
                if (mask.none()) {
                    continue;
                }

                List<Output> columns = new ArrayList<>();
                for (int i = 0; i < operator.outputCount(); i++) {
                    columns.add(batch.output(i));
                }

                for (int position : mask) {
                    Object[] row = new Object[columns.size()];
                    for (int i = 0; i < columns.size(); i++) {
                        Output output = columns.get(i);
                        Vector values = output.borrow(Stream.VALUES);
                        BooleanVector nulls = (BooleanVector) output.borrowOrNull(Stream.NULLS);
                        row[i] = decodeValue(values, nulls, position);
                    }

                    result.add(new Row(row));
                }
            }
            operator.close();
            return result;
        }

        private static Object decodeValue(Vector values, BooleanVector nulls, int position)
        {
            if (nulls != null && nulls.values()[position]) {
                return null;
            }
            return decodeNonNullValue(values, position);
        }

        private static Object decodeNonNullValue(Vector values, int position)
        {
            return switch (values) {
                case I32Vector vector -> vector.values()[position];
                case I64Vector vector -> vector.values()[position];
                case BooleanVector vector -> vector.values()[position] ? 1L : 0L;
                case F64Vector vector -> vector.values()[position];
                case BinaryVector vector -> vector.hasTrait(BinaryVector.Trait.UTF8_STRING) ? vector.utf8Value(position) : vector.copyBytes(position);
                case ArrayVector vector -> decodeArray(vector, position);
                case MapVector vector -> decodeMap(vector, position);
                case StructVector vector -> decodeStruct(vector, position);
                case DictionaryVector vector -> decodeNonNullValue(vector.values(), vector.ids()[position]);
                case RleVector vector -> decodeRleValue(vector, position);
                default -> throw new UnsupportedOperationException(values.getClass().getSimpleName());
            };
        }

        private static List<Object> decodeArray(ArrayVector values, int position)
        {
            List<Object> elements = new ArrayList<>(values.length(position));
            BooleanVector nulls = values.elementNulls();
            for (int index = values.startOffset(position); index < values.endOffset(position); index++) {
                elements.add(decodeValue(values.elementValues(), nulls, index));
            }
            return elements;
        }

        private static LinkedHashMap<Object, Object> decodeMap(MapVector values, int position)
        {
            LinkedHashMap<Object, Object> entries = new LinkedHashMap<>();
            BooleanVector valueNulls = (BooleanVector) values.valueStreamOrNull(Stream.NULLS);
            for (int index = values.startOffset(position); index < values.endOffset(position); index++) {
                Object key = decodeNonNullValue(values.keyValues(), index);
                Object value = decodeValue(values.valueValues(), valueNulls, index);
                entries.put(key, value);
            }
            return entries;
        }

        private static LinkedHashMap<String, Object> decodeStruct(StructVector values, int position)
        {
            LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
            for (var entry : values.fields().entrySet()) {
                fields.put(entry.getKey(), decodeStreamValue(entry.getValue(), position));
            }
            return fields;
        }

        private static Object decodeStreamValue(Streams streams, int position)
        {
            return decodeValue(streams.values(), (BooleanVector) streams.getOrNull(Stream.NULLS), position);
        }

        private static Object decodeRleValue(RleVector values, int position)
        {
            int count = 0;
            for (int index = 0; index < values.counts().length; index++) {
                count += values.counts()[index];
                if (position < count) {
                    return decodeNonNullValue(values.values(), index);
                }
            }
            throw new IndexOutOfBoundsException("Position " + position + " is out of bounds for RLE vector of length " + values.length());
        }
    }
}
