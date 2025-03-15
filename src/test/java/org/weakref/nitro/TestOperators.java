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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.function.Function;
import org.weakref.nitro.operator.AggregationOperator;
import org.weakref.nitro.operator.ConstantTableOperator;
import org.weakref.nitro.operator.FilterOperator;
import org.weakref.nitro.operator.GeneratorOperator;
import org.weakref.nitro.operator.GroupOperator;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.LimitOperator;
import org.weakref.nitro.operator.NestedLoopJoinOperator;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.TopNOperator;
import org.weakref.nitro.operator.aggregation.CountAll;
import org.weakref.nitro.operator.aggregation.First;
import org.weakref.nitro.operator.aggregation.Max;
import org.weakref.nitro.operator.aggregation.Min;
import org.weakref.nitro.operator.aggregation.Sum;
import org.weakref.nitro.operator.filter.I64Predicate;
import org.weakref.nitro.operator.generator.SequenceGenerator;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.OperatorAssertions.operator;
import static org.weakref.nitro.data.Row.row;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
public class TestOperators
{
    private final Allocator allocator = new Allocator();

    @AfterAll
    void tearDown()
    {
        System.out.println(allocator);
    }

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
                        allocator,
                        List.of(
                                new Min(0),
                                new Max(0),
                                new Sum(0),
                                new CountAll()),
                        new LimitOperator(
                                5,
                                new ProjectOperator(
                                        allocator,
                                        new ProjectOperator.Execution(
                                                List.of(new ProjectOperator.Invocation(
                                                        multiply(2),
                                                        List.of(-2))),
                                                List.of(0)),
                                        new FilterOperator(
                                                0,
                                                new I64Predicate(value -> value < 20 || value > 40),
                                                new GeneratorOperator(
                                                        allocator,
                                                        50,
                                                        10,
                                                        List.of(
                                                                new SequenceGenerator(0),
                                                                new SequenceGenerator(100)))))))))
                .matchesExactly(List.of(row(200L, 208L, 1020L, 5L)));
    }

    @Test
    void testFilterOverLimit()
    {
        assertThat(operator(
                new FilterOperator(
                        0,
                        new I64Predicate(value -> value < 10 || value > 40),
                        new LimitOperator(
                                15,
                                new GeneratorOperator(
                                        allocator,
                                        50,
                                        10,
                                        List.of(
                                                new SequenceGenerator(0),
                                                new SequenceGenerator(100)))))))
                .matchesExactly(List.of(
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
                        new I64Predicate(value -> value % 2 == 0),
                        new LimitOperator(
                                15,
                                new GeneratorOperator(
                                        allocator,
                                        50,
                                        10,
                                        List.of(
                                                new SequenceGenerator(0),
                                                new SequenceGenerator(100)))))))
                .matchesExactly(List.of(
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
                        allocator,
                        List.of(new CountAll()),
                        new LimitOperator(
                                15,
                                new GeneratorOperator(
                                        allocator,
                                        50,
                                        10,
                                        List.of(new SequenceGenerator(0)))))))
                .matchesExactly(List.of(row(15L)));
    }

    @Test
    void testFilterOverFilter()
    {
        assertThat(operator(
                new FilterOperator(
                        0,
                        new I64Predicate(value -> value % 2 == 0),
                        new FilterOperator(
                                0,
                                new I64Predicate(value -> value % 3 == 0),
                                new GeneratorOperator(
                                        allocator,
                                        50,
                                        10,
                                        List.of(new SequenceGenerator(0)))))))
                .matchesExactly(List.of(
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
                        allocator,
                        0,
                        new ProjectOperator(
                                allocator,
                                new ProjectOperator.Execution(
                                        List.of(new ProjectOperator.Invocation(
                                                divide(3),
                                                List.of(-1))),
                                        List.of(0)),
                                new GeneratorOperator(
                                        allocator,
                                        10,
                                        10,
                                        List.of(new SequenceGenerator(100)))))))
                .matchesExactly(List.of(
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
    void testLimit()
    {
        assertThat(operator(
                new LimitOperator(
                        5,
                        new GeneratorOperator(
                                allocator,
                                50,
                                10,
                                List.of(new SequenceGenerator(0))))))
                .describedAs("Within first batch")
                .matchesExactly(List.of(
                        row(0L),
                        row(1L),
                        row(2L),
                        row(3L),
                        row(4L)));

        assertThat(operator(
                new LimitOperator(
                        15,
                        new GeneratorOperator(
                                allocator,
                                50,
                                10,
                                List.of(new SequenceGenerator(0))))))
                .describedAs("Middle of second batch")
                .matchesExactly(List.of(
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
                        new GeneratorOperator(
                                allocator,
                                12,
                                10,
                                List.of(new SequenceGenerator(0))))))
                .describedAs("Beyond end of underlying sequence")
                .matchesExactly(List.of(
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
    void testTopN()
    {
        assertThat(operator(
                new TopNOperator(
                        allocator,
                        5,
                        0,
                        new GeneratorOperator(
                                allocator,
                                50,
                                10,
                                List.of(
                                        new SequenceGenerator(0),
                                        new SequenceGenerator(100))))))
                .matchesExactly(List.of(
                        row(49L, 149L),
                        row(48L, 148L),
                        row(47L, 147L),
                        row(46L, 146L),
                        row(45L, 145L)));
    }

    @Test
    void testAggregation()
    {
        assertThat(operator(
                new AggregationOperator(
                        allocator,
                        List.of(
                                new First(0),
                                new Min(0),
                                new Max(0),
                                new Sum(0),
                                new CountAll()),
                        new GeneratorOperator(
                                allocator,
                                50,
                                10,
                                List.of(new SequenceGenerator(100))))))
                .matchesExactly(List.of(row(100L, 100L, 149L, 6225L, 50L)));

        assertThat(operator(
                new AggregationOperator(
                        allocator,
                        List.of(
                                new First(0),
                                new Min(0),
                                new Max(0),
                                new Sum(0),
                                new CountAll()),
                        new FilterOperator(
                                0,
                                new I64Predicate(value -> value % 2 == 0),
                                new GeneratorOperator(
                                        allocator,
                                        50,
                                        10,
                                        List.of(new SequenceGenerator(100)))))))
                .matchesExactly(List.of(row(100L, 100L, 148L, 3100L, 25L)));
    }

    @Test
    void testGroupedAggregation()
    {
        assertThat(operator(
                new GroupedAggregationOperator(
                        allocator,
                        0,
                        List.of(
                                new First(1), // key
                                new Min(2),
                                new Max(2),
                                new Sum(2),
                                new CountAll()),
                        new GroupOperator(
                                allocator,
                                0,
                                new ProjectOperator(
                                        allocator,
                                        new ProjectOperator.Execution(
                                                List.of(new ProjectOperator.Invocation(
                                                        (output, inputs, mask) -> {
                                                            I64Vector in = (I64Vector) inputs[0];
                                                            I64Vector out = (I64Vector) output;
                                                            for (int i = 0; i <= mask.maxPosition(); i++) {
                                                                out.nulls()[i] = in.nulls()[i];
                                                                out.values()[i] = in.values()[i] % 10 + 13;
                                                            }
                                                        },
                                                        List.of(-1))),
                                                List.of(0, -1)),
                                        new GeneratorOperator(
                                                allocator,
                                                50,
                                                10,
                                                List.of(new SequenceGenerator(100))))))))
                .matchesExactly(List.of(
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
    void testConstantTable()
    {
        assertThat(operator(new ConstantTableOperator(
                allocator,
                3,
                List.of(
                        row(1L, 10L, 100L),
                        row(2L, 20L, 200L),
                        row(null, 30L, 300L),
                        row(4L, null, 400L),
                        row(5L, 50L, null)))))
                .matchesExactly(List.of(
                        row(1L, 10L, 100L),
                        row(2L, 20L, 200L),
                        row(null, 30L, 300L),
                        row(4L, null, 400L),
                        row(5L, 50L, null)));
    }

    @Test
    void testNestedLoop()
    {
        assertThat(operator(
                new NestedLoopJoinOperator(
                        allocator,
                        new ConstantTableOperator(
                                allocator,
                                1,
                                List.of(
                                        row(1L),
                                        row(2L),
                                        row(3L))),
                        new ConstantTableOperator(
                                allocator,
                                1,
                                List.of(
                                        row(10L),
                                        row(20L),
                                        row(30L))))))
                .matches(List.of(
                        row(1L, 10L),
                        row(1L, 20L),
                        row(1L, 30L),
                        row(2L, 10L),
                        row(2L, 20L),
                        row(2L, 30L),
                        row(3L, 10L),
                        row(3L, 20L),
                        row(3L, 30L)));
    }

    @Test
    void testNestedLoop1()
    {
        assertThat(operator(
                new NestedLoopJoinOperator(
                        allocator,
                        new ConstantTableOperator(
                                allocator,
                                1,
                                List.of(row(1L))),
                        new GeneratorOperator(
                                allocator,
                                10,
                                5,
                                List.of(new SequenceGenerator(0))))))
                .matches(List.of(
                        row(1L, 0L),
                        row(1L, 1L),
                        row(1L, 2L),
                        row(1L, 3L),
                        row(1L, 4L),
                        row(1L, 5L),
                        row(1L, 6L),
                        row(1L, 7L),
                        row(1L, 8L),
                        row(1L, 9L)));
    }

    @Test
    void testNestedLoop2()
    {
        assertThat(operator(
                new NestedLoopJoinOperator(
                        allocator,
                        new GeneratorOperator(allocator, 6, 2, List.of(new SequenceGenerator(0))),
                        new GeneratorOperator(allocator, 3, 1, List.of(new SequenceGenerator(10))))))
                .matches(List.of(
                        row(0L, 10L),
                        row(0L, 11L),
                        row(0L, 12L),
                        row(1L, 10L),
                        row(1L, 11L),
                        row(1L, 12L),
                        row(2L, 10L),
                        row(2L, 11L),
                        row(2L, 12L),
                        row(3L, 10L),
                        row(3L, 11L),
                        row(3L, 12L),
                        row(4L, 10L),
                        row(4L, 11L),
                        row(4L, 12L),
                        row(5L, 10L),
                        row(5L, 11L),
                        row(5L, 12L)));
    }

    @Test
    void testNestedLoop3()
    {
        assertThat(operator(
                new NestedLoopJoinOperator(
                        allocator,
                        new GeneratorOperator(allocator, 3, 1, List.of(new SequenceGenerator(0))),
                        new GeneratorOperator(allocator, 6, 2, List.of(new SequenceGenerator(10))))))
                .matches(List.of(
                        row(0L, 10L),
                        row(0L, 11L),
                        row(0L, 12L),
                        row(0L, 13L),
                        row(0L, 14L),
                        row(0L, 15L),
                        row(1L, 10L),
                        row(1L, 11L),
                        row(1L, 12L),
                        row(1L, 13L),
                        row(1L, 14L),
                        row(1L, 15L),
                        row(2L, 10L),
                        row(2L, 11L),
                        row(2L, 12L),
                        row(2L, 13L),
                        row(2L, 14L),
                        row(2L, 15L)));
    }

    @Test
    void testNestedLoopEmptyBuild()
    {
        assertThat(operator(
                new NestedLoopJoinOperator(
                        allocator,
                        new GeneratorOperator(allocator, 10, 2, List.of(new SequenceGenerator(0))),
                        new ConstantTableOperator(allocator, 1, List.of()))))
                .matches(List.of());
    }

    @Test
    void testNestedLoopEmptyProbe()
    {
        assertThat(operator(
                new NestedLoopJoinOperator(
                        allocator,
                        new ConstantTableOperator(allocator, 1, List.of()),
                        new GeneratorOperator(allocator, 10, 2, List.of(new SequenceGenerator(0))))))
                .matches(List.of());
    }

    @Test
    void testProject()
    {
        Function negate = (output, inputs, mask) -> {
            I64Vector in = (I64Vector) inputs[0];
            I64Vector out = (I64Vector) output;
            for (int i = 0; i <= mask.maxPosition(); i++) {
                out.values()[i] = -in.values()[i];
                out.nulls()[i] = in.nulls()[i];
            }
        };

        Function add = (output, inputs, mask) -> {
            I64Vector in1 = (I64Vector) inputs[0];
            I64Vector in2 = (I64Vector) inputs[1];
            I64Vector out = (I64Vector) output;
            for (int i = 0; i <= mask.maxPosition(); i++) {
                out.values()[i] = in1.values()[i] + in2.values()[i];
                out.nulls()[i] = in1.nulls()[i] || in2.nulls()[i];
            }
        };

        Function multiply = (output, inputs, mask) -> {
            I64Vector in1 = (I64Vector) inputs[0];
            I64Vector in2 = (I64Vector) inputs[1];
            I64Vector out = (I64Vector) output;
            for (int i = 0; i <= mask.maxPosition(); i++) {
                out.values()[i] = in1.values()[i] * in2.values()[i];
                out.nulls()[i] = in1.nulls()[i] || in2.nulls()[i];
            }
        };

        /*
           %0 = %input * %input
           %1 = %0 + %0
           %2 = -%0
         */

        assertThat(operator(
                new ProjectOperator(
                        allocator,
                        new ProjectOperator.Execution(
                                List.of(
                                        new ProjectOperator.Invocation(multiply, List.of(-1, -1)),
                                        new ProjectOperator.Invocation(add, List.of(0, 0)),
                                        new ProjectOperator.Invocation(negate, List.of(0))),
                                List.of(-1, 1, 2)),
                        new GeneratorOperator(
                                allocator,
                                10,
                                5,
                                List.of(new SequenceGenerator(0), new SequenceGenerator(100))))))
                .matchesExactly(List.of(
                        row(0L, 0L, 0L),
                        row(1L, 2L, -1L),
                        row(2L, 8L, -4L),
                        row(3L, 18L, -9L),
                        row(4L, 32L, -16L),
                        row(5L, 50L, -25L),
                        row(6L, 72L, -36L),
                        row(7L, 98L, -49L),
                        row(8L, 128L, -64L),
                        row(9L, 162L, -81L)));
    }

    private static Function multiply(long value)
    {
        return (output, inputs, mask) -> {
            I64Vector in = (I64Vector) inputs[0];
            I64Vector out = (I64Vector) output;
            for (int i = 0; i <= mask.maxPosition(); i++) {
                out.nulls()[i] = in.nulls()[i];
                out.values()[i] = in.values()[i] * value;
            }
        };
    }

    private static Function divide(long value)
    {
        return (output, inputs, mask) -> {
            I64Vector in = (I64Vector) inputs[0];
            I64Vector out = (I64Vector) output;
            for (int i = 0; i <= mask.maxPosition(); i++) {
                out.nulls()[i] = in.nulls()[i];
                out.values()[i] = in.values()[i] / value;
            }
        };
    }
}
