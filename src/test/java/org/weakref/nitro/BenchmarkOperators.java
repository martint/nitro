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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.AggregationOperator;
import org.weakref.nitro.operator.GeneratorOperator;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.NestedLoopJoinOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.aggregation.CountAll;
import org.weakref.nitro.operator.generator.SequenceGenerator;

import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.Throughput)
public class BenchmarkOperators
{
    private final Allocator allocator = new Allocator();

    @Benchmark
    @OperationsPerInvocation(1_000_000_000)
    public void aggregation()
    {
        Operator operator = new AggregationOperator(
                allocator,
                List.of(new CountAll()),
                new GeneratorOperator(
                        allocator,
                        1_000_000_000L,
                        List.of(new SequenceGenerator(0))));

        consume(operator);
    }

    @Benchmark
    @OperationsPerInvocation(100_000_000)
    public void groupBy()
    {
        Operator operator = new GroupedAggregationOperator(
                allocator,
                0,
                List.of(new CountAll()),
                new GeneratorOperator(
                        allocator,
                        100_000_000L,
                        List.of(new SequenceGenerator(0, 10))));

        consume(operator);
    }

    @Benchmark
    @OperationsPerInvocation(25_000 * 25_000)
    public void nestedLoopJoin()
    {
        Operator operator = new NestedLoopJoinOperator(
                allocator,
                new GeneratorOperator(
                        allocator,
                        25_000L,
                        List.of(new SequenceGenerator(100))),
                new GeneratorOperator(
                        allocator,
                        25_000L,
                        List.of(new SequenceGenerator(100))));

        consume(operator);
    }

    @Benchmark
    @OperationsPerInvocation(3_000 * 500_000)
    public void nestedLoopJoinSmallVsLarge()
    {
        Operator operator = new NestedLoopJoinOperator(
                allocator,
                new GeneratorOperator(
                        allocator,
                        3_000L,
                        List.of(new SequenceGenerator(100))),
                new GeneratorOperator(
                        allocator,
                        500_000L,
                        List.of(new SequenceGenerator(100))));

        consume(operator);
    }

    @Benchmark
    @OperationsPerInvocation(3_000 * 500_000)
    public void nestedLoopJoinLargeVsSmall()
    {
        Operator operator = new NestedLoopJoinOperator(
                allocator,
                new GeneratorOperator(
                        allocator,
                        500_000L,
                        List.of(new SequenceGenerator(100))),
                new GeneratorOperator(
                        allocator,
                        3_000,
                        List.of(new SequenceGenerator(100))));

        consume(operator);
    }

    private static void consume(Operator operator)
    {
        while (operator.hasNext()) {
            operator.next();
            for (int column = 0; column < operator.columnCount(); column++) {
                consume(operator.column(column));
            }
        }
    }

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    private static void consume(Vector vector)
    {
    }

    public static void main(String[] args)
            throws Exception
    {
        Benchmarks.benchmark(BenchmarkOperators.class)
                .run();
    }
}
