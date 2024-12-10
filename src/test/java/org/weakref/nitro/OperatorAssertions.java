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
import org.weakref.nitro.data.LongVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Operator;

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
                Mask mask = operator.next();

                List<Vector> columns = new ArrayList<>();
                for (int i = 0; i < operator.columnCount(); i++) {
                    columns.add(operator.column(i));
                }

                for (int position : mask) {
                    Long[] row = new Long[columns.size()];
                    for (int i = 0; i < columns.size(); i++) {
                        LongVector column = (LongVector) columns.get(i);
                        row[i] = column.nulls()[position] ? null : column.values()[position];
                    }

                    result.add(new Row(row));
                }
            }
            return result;
        }
    }
}
