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
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.I64VectorWithNulls;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.Operator;
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
                Batch batch = operator.nextBatch();
                var mask = batch.borrowMask();
                if (mask.none()) {
                    continue;
                }

                List<Vector> columns = new ArrayList<>();
                for (int i = 0; i < operator.outputCount(); i++) {
                    columns.add(batch.output(i).borrow(Stream.VALUES));
                }

                for (int position : mask) {
                    Long[] row = new Long[columns.size()];
                    for (int i = 0; i < columns.size(); i++) {
                        row[i] = switch (columns.get(i)) {
                            case I64VectorWithNulls v -> v.nulls()[position] ? null : v.values()[position];
                            case I64Vector v -> v.values()[position];
                            case BooleanVector v -> v.values()[position] ? 1L : 0L;
                            default -> throw new UnsupportedOperationException(columns.get(i).getClass().getSimpleName());
                        };
                    }

                    result.add(new Row(row));
                }
            }
            operator.close();
            return result;
        }
    }
}
