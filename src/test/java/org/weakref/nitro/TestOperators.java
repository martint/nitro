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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.weakref.nitro.operator.AggregationOperator;
import org.weakref.nitro.operator.FilterOperator;
import org.weakref.nitro.operator.GeneratorOperator;
import org.weakref.nitro.operator.GroupOperator;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.LimitOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.TopNOperator;
import org.weakref.nitro.operator.ValuesOperator;
import org.weakref.nitro.operator.aggregation.CountAll;
import org.weakref.nitro.operator.aggregation.First;
import org.weakref.nitro.operator.aggregation.Max;
import org.weakref.nitro.operator.aggregation.Min;
import org.weakref.nitro.operator.aggregation.Sum;
import org.weakref.nitro.operator.generator.SequenceGenerator;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.OperatorAssertions.operator;
import static org.weakref.nitro.data.Row.row;

public class TestOperators
{
    @Test
    void testComplex()
    {
        /*
          SELECT min(v), max(v), sum(v), count(*)
          FROM (
            SELECT y * 2 AS v
            FROM TABLE(sequences([0, 100], 50) t(x, y)
            WHERE x < 20 OR x > 40;
            LIMIT 5)
         */
        assertThat(operator(
                new AggregationOperator(
                        List.of(
                                new Min(0),
                                new Max(0),
                                new Sum(0),
                                new CountAll()),
                        new LimitOperator(
                                5,
                                new ProjectOperator(
                                        List.of(1),
                                        List.of(v -> v * 2),
                                        new FilterOperator(
                                                0,
                                                value -> value < 20 || value > 40,
                                                new GeneratorOperator(
                                                        50,
                                                        10,
                                                        List.of(
                                                                new SequenceGenerator(0),
                                                                new SequenceGenerator(100)))))))))
                .matches(List.of(row(200L, 208L, 1020L, 5L)));
    }

    @Test
    void testFilterOverLimit()
    {
        assertThat(operator(
                new FilterOperator(
                        0,
                        value -> value < 10 || value > 40,
                        new LimitOperator(
                                15,
                                new GeneratorOperator(
                                        50,
                                        10,
                                        List.of(
                                                new SequenceGenerator(0),
                                                new SequenceGenerator(100)))))))
                .matches(List.of(
                        row(0L, 100L),
                        row(1L, 101L),
                        row(2L, 102L),
                        row(3L, 103L),
                        row(4L, 104L),
                        row(5L, 105L),
                        row(6L, 106L),
                        row(7L, 107L),
                        row(8L, 108L),
                        row(9L, 109L)));

        assertThat(operator(
                new FilterOperator(
                        0,
                        value -> value % 2 == 0,
                        new LimitOperator(
                                15,
                                new GeneratorOperator(
                                        50,
                                        10,
                                        List.of(
                                                new SequenceGenerator(0),
                                                new SequenceGenerator(100)))))))
                .matches(List.of(
                        row(0L, 100L),
                        row(2L, 102L),
                        row(4L, 104L),
                        row(6L, 106L),
                        row(8L, 108L),
                        row(10L, 110L),
                        row(12L, 112L),
                        row(14L, 114L)));
    }

    @Test
    void testAggregationOverLimit()
    {
        assertThat(operator(
                new AggregationOperator(
                        List.of(new CountAll()),
                        new LimitOperator(
                                15,
                                new GeneratorOperator(50, 10, List.of(new SequenceGenerator(0)))))))
                .matches(List.of(row(15L)));
    }

    @Test
    void testFilterOverFilter()
    {
        assertThat(operator(
                new FilterOperator(
                        0,
                        value -> value % 2 == 0,
                        new FilterOperator(
                                0,
                                value -> value % 3 == 0,
                                new GeneratorOperator(50, 10, List.of(new SequenceGenerator(0)))))))
                .matches(List.of(
                        row(0L),
                        row(6L),
                        row(12L),
                        row(18L),
                        row(24L),
                        row(30L),
                        row(36L),
                        row(42L),
                        row(48L)));
    }

    @Test
    void testGroup()
    {
        assertThat(operator(
                new GroupOperator(
                        0,
                        new ProjectOperator(
                                List.of(0),
                                List.of(v -> v / 3),
                                new GeneratorOperator(10, 10, List.of(new SequenceGenerator(100)))))))
                .matches(List.of(
                        row(0L, 33L),
                        row(0L, 33L),
                        row(1L, 34L),
                        row(1L, 34L),
                        row(1L, 34L),
                        row(2L, 35L),
                        row(2L, 35L),
                        row(2L, 35L),
                        row(3L, 36L),
                        row(3L, 36L)));
    }

    @Test
    void testAggregation()
    {
        assertThat(operator(
                new AggregationOperator(
                        List.of(new CountAll()),
                        new GeneratorOperator(50, 10, List.of(new SequenceGenerator(0))))))
                .matches(List.of(row(50L)));
    }

    @Test
    void testMinAggregation()
    {
        assertThat(operator(
                new AggregationOperator(
                        List.of(new Min(0)),
                        new GeneratorOperator(50, 10, List.of(new SequenceGenerator(100))))))
                .matches(List.of(row(100L)));
    }

    @Test
    void testLimit()
    {
        assertThat(operator(
                new LimitOperator(
                        5,
                        new GeneratorOperator(50, 10, List.of(new SequenceGenerator(0))))))
                .describedAs("Within first batch")
                .matches(List.of(
                        row(0L),
                        row(1L),
                        row(2L),
                        row(3L),
                        row(4L)));

        assertThat(operator(
                new LimitOperator(
                        15,
                        new GeneratorOperator(50, 10, List.of(new SequenceGenerator(0))))))
                .describedAs("Middle of second batch")
                .matches(List.of(
                        row(0L),
                        row(1L),
                        row(2L),
                        row(3L),
                        row(4L),
                        row(5L),
                        row(6L),
                        row(7L),
                        row(8L),
                        row(9L),
                        row(10L),
                        row(11L),
                        row(12L),
                        row(13L),
                        row(14L)));

        assertThat(operator(
                new LimitOperator(
                        15,
                        new GeneratorOperator(12, 10, List.of(new SequenceGenerator(0))))))
                .describedAs("Beyond end of underlying sequence")
                .matches(List.of(
                        row(0L),
                        row(1L),
                        row(2L),
                        row(3L),
                        row(4L),
                        row(5L),
                        row(6L),
                        row(7L),
                        row(8L),
                        row(9L),
                        row(10L),
                        row(11L)));
    }

    @Test
    void topN()
    {
        assertThat(operator(
                new TopNOperator(
                        5,
                        0,
                        new GeneratorOperator(
                                50,
                                10,
                                List.of(
                                        new SequenceGenerator(0),
                                        new SequenceGenerator(100))))))
                .matches(List.of(
                        row(49L, 149L),
                        row(48L, 148L),
                        row(47L, 147L),
                        row(46L, 146L),
                        row(45L, 145L)));
    }

    @Test
    void testGroupedAggregation()
    {
        assertThat(operator(
                new GroupedAggregationOperator(
                        0,
                        List.of(
                                new First(1), // key
                                new Min(2),
                                new Max(2),
                                new Sum(2),
                                new CountAll()),
                        new GroupOperator(
                                0,
                                new ProjectOperator(
                                        List.of(0, 0, 1),
                                        List.of(
                                                v -> v % 10 + 13,
                                                v -> v),
                                        new GeneratorOperator(
                                                50,
                                                10,
                                                List.of(new SequenceGenerator(100))))))))
                .matches(List.of(
                        row(13L, 100L, 140L, 600L, 5L),
                        row(14L, 101L, 141L, 605L, 5L),
                        row(15L, 102L, 142L, 610L, 5L),
                        row(16L, 103L, 143L, 615L, 5L),
                        row(17L, 104L, 144L, 620L, 5L),
                        row(18L, 105L, 145L, 625L, 5L),
                        row(19L, 106L, 146L, 630L, 5L),
                        row(20L, 107L, 147L, 635L, 5L),
                        row(21L, 108L, 148L, 640L, 5L),
                        row(22L, 109L, 149L, 645L, 5L)));
    }

    @Test
    @Disabled
    void x()
    {
        for (int i = 0; i < 1000; i++) {
            Operator operator = new AggregationOperator(
                    List.of(
                            new Min(0),
                            new Max(0),
                            new Sum(0),
                            new CountAll()),
                    new GeneratorOperator(
                            100_000_000L,
                            10000,
                            List.of(new SequenceGenerator(100))));

            long start = System.nanoTime();
            while (operator.hasNext()) {
                operator.next();
                for (int c = 0; c < operator.columnCount(); c++) {
                    operator.column(c);
                }
            }
            System.out.println((System.nanoTime() - start) / 1_000_000.0);
        }
    }

    @Test
    void testValues()
    {
        assertThat(operator(new ValuesOperator(
                3,
                List.of(
                        row(1L, 10L, 100L),
                        row(2L, 20L, 200L),
                        row(null, 30L, 300L),
                        row(4L, null, 400L),
                        row(5L, 50L, null)))))
                .matches(List.of(
                        row(1L, 10L, 100L),
                        row(2L, 20L, 200L),
                        row(null, 30L, 300L),
                        row(4L, null, 400L),
                        row(5L, 50L, null)));
    }
}
