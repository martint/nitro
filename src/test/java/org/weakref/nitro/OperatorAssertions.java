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
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Output;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.ArrayList;
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
                    Long[] row = new Long[columns.size()];
                    for (int i = 0; i < columns.size(); i++) {
                        Output output = columns.get(i);
                        Vector values = output.borrow(Stream.VALUES);
                        BooleanVector nulls = (BooleanVector) output.borrowOrNull(Stream.NULLS);
                        row[i] = switch (values) {
                            case I64Vector v -> nulls != null && nulls.values()[position] ? null : v.values()[position];
                            case BooleanVector v -> v.values()[position] ? 1L : 0L;
                            case DictionaryVector v -> decodeDictionaryValue(v, nulls, position);
                            default -> throw new UnsupportedOperationException(values.getClass().getSimpleName());
                        };
                    }

                    result.add(new Row(row));
                }
            }
            operator.close();
            return result;
        }

        private static Long decodeDictionaryValue(DictionaryVector values, BooleanVector nulls, int position)
        {
            if (nulls != null && nulls.values()[position]) {
                return null;
            }

            return decodeValue(values.values(), values.ids()[position]);
        }

        private static Long decodeValue(Vector values, int position)
        {
            return switch (values) {
                case I64Vector vector -> vector.values()[position];
                case BooleanVector vector -> vector.values()[position] ? 1L : 0L;
                case DictionaryVector vector -> decodeValue(vector.values(), vector.ids()[position]);
                case RleVector vector -> decodeRleValue(vector, position);
                default -> throw new UnsupportedOperationException(values.getClass().getSimpleName());
            };
        }

        private static Long decodeRleValue(RleVector values, int position)
        {
            int count = 0;
            for (int index = 0; index < values.counts().length; index++) {
                count += values.counts()[index];
                if (position < count) {
                    return decodeValue(values.values(), index);
                }
            }
            throw new IndexOutOfBoundsException("Position " + position + " is out of bounds for RLE vector of length " + values.length());
        }
    }
}
