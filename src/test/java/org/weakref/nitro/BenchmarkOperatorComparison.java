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
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.AggregationOperator;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.HashJoinOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.TableOperator;
import org.weakref.nitro.operator.TableOperator.Page;
import org.weakref.nitro.operator.aggregation.Sum;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.Stream;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.LongUnaryOperator;

/**
 * Operator-isolation benchmarks intended to be run apples-to-apples against the equivalent Velox
 * operator microbenchmarks (velox/exec/benchmarks/NitroComparisonBenchmark.cpp). Each benchmark
 * drives a SINGLE operator over deterministic synthetic bigint columns whose row counts, key
 * distribution, and group cardinality match the Velox side exactly, so the per-operator gap between
 * the engines can be isolated, profiled, and tuned without the confounds of a full query (scan +
 * decode + multiple operators).
 *
 * <p>Data is primary synthetic input (built once in {@link #setup()}), not replayed intermediate
 * query results. The batch size matches Velox's vector size so both engines see the same chunking.
 */
@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 6, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkOperatorComparison
{
    // Batch (vector) size — matches the Velox side so chunking overhead is equal on both engines.
    // Swept to separate per-batch overhead from per-row operator cost.
    @org.openjdk.jmh.annotations.Param({"8192", "32768", "65536"})
    public int batch;

    private static final int PROBE_ROWS = 8_000_000;
    private static final int BUILD_ROWS_SMALL = 1_000_000;
    private static final int BUILD_ROWS_LARGE = 8_000_000;
    private static final int SMALL_PROBE_ROWS = 1_000_000;

    private static final int SCAN_ROWS = 16_000_000;
    private static final int GROUPS_LOW = 1_024;
    private static final int GROUPS_HIGH = 4_000_000;

    // Sparse keys (stride > 1) defeat Nitro's dense array-mode join so the hash probe path is exercised,
    // matching Velox which always hashes. The dense variant (stride 1) measures Nitro's array-mode best case.
    private static final long KEY_STRIDE = 64;

    private final PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();

    private List<Page> probeSparse;
    private List<Page> buildSparseSmall;
    private List<Page> probeDense;
    private List<Page> buildDenseSmall;
    private List<Page> buildSparseLarge;
    private List<Page> smallProbeSparse;
    private List<Page> scanLowCard;
    private List<Page> scanHighCard;
    private List<Page> scanValues;
    private List<Page> scanTwoKey;
    private List<Page> scanThreeKey;

    private Allocator allocator;

    @Setup
    public void setup()
    {
        // Build keys 0..n-1 (dense) or *stride (sparse), each unique, with a payload column = row index.
        // Probe keys cycle 0..distinct-1 (*stride) so every probe row matches exactly one build row.
        buildSparseSmall = twoColumnPages(BUILD_ROWS_SMALL, row -> row * KEY_STRIDE, row -> row);
        probeSparse = twoColumnPages(PROBE_ROWS, row -> (row % BUILD_ROWS_SMALL) * KEY_STRIDE, row -> row);

        buildDenseSmall = twoColumnPages(BUILD_ROWS_SMALL, row -> row, row -> row);
        probeDense = twoColumnPages(PROBE_ROWS, row -> row % BUILD_ROWS_SMALL, row -> row);

        buildSparseLarge = twoColumnPages(BUILD_ROWS_LARGE, row -> row * KEY_STRIDE, row -> row);
        smallProbeSparse = twoColumnPages(SMALL_PROBE_ROWS, row -> (row % BUILD_ROWS_LARGE) * KEY_STRIDE, row -> row);

        scanLowCard = twoColumnPages(SCAN_ROWS, row -> row % GROUPS_LOW, row -> row);
        scanHighCard = twoColumnPages(SCAN_ROWS, row -> row % GROUPS_HIGH, row -> row);
        scanValues = oneColumnPages(SCAN_ROWS, row -> row);
        // Multi-long grouping inputs: key columns + payload. 2-key ~64k groups, 3-key ~64k groups.
        scanTwoKey = multiColumnPages(SCAN_ROWS, new LongUnaryOperator[] {
                row -> row % 1024, row -> row % 64, row -> row});
        scanThreeKey = multiColumnPages(SCAN_ROWS, new LongUnaryOperator[] {
                row -> row % 256, row -> row % 16, row -> row % 16, row -> row});
    }

    @Setup(Level.Invocation)
    public void setupInvocation()
    {
        allocator = new Allocator();
    }

    // Every pipeline terminates in a global sum over all of the operator-under-test's output columns.
    // This (a) forces full value materialization of lazy (dictionary) join output, matching Velox's
    // copyResults, and (b) reduces the result to one row so the consumer cost is trivial and identical
    // on both engines — the same idiom Velox's own FilterProjectBenchmark uses. The added global sum is
    // small and symmetric; aggregationSum measures it in isolation for reference.

    // ---- Hash join: probe-heavy (small unique build, large probe) ----

    @Benchmark
    public void joinProbeSparse()
    {
        consume(sumAll(new HashJoinOperator(
                allocator,
                new TableOperator(2, probeSparse), 0,
                new TableOperator(2, buildSparseSmall), 0), 4));
    }

    @Benchmark
    public void joinProbeDense()
    {
        consume(sumAll(new HashJoinOperator(
                allocator,
                new TableOperator(2, probeDense), 0,
                new TableOperator(2, buildDenseSmall), 0), 4));
    }

    // ---- Hash join: build-heavy (large unique build, small probe) ----

    @Benchmark
    public void joinBuildSparse()
    {
        consume(sumAll(new HashJoinOperator(
                allocator,
                new TableOperator(2, smallProbeSparse), 0,
                new TableOperator(2, buildSparseLarge), 0), 4));
    }

    // ---- Grouped aggregation: sum(payload) GROUP BY key ----

    @Benchmark
    public void groupSumLowCardinality()
    {
        // groupByColumns = [key]; output is [key, sum], matching Velox's singleAggregation output and
        // forcing the key to be materialized on both engines.
        consume(sumAll(new GroupedAggregationOperator(
                allocator, List.of(0), List.of(new Sum(1)), new TableOperator(2, scanLowCard)), 2));
    }

    @Benchmark
    public void groupSumHighCardinality()
    {
        consume(sumAll(new GroupedAggregationOperator(
                allocator, List.of(0), List.of(new Sum(1)), new TableOperator(2, scanHighCard)), 2));
    }

    // ---- Multi-long grouped aggregation: sum(payload) GROUP BY (k0, k1[, k2]) ----

    @Benchmark
    public void groupSumTwoLongKeys()
    {
        consume(sumAll(new GroupedAggregationOperator(
                allocator, List.of(0, 1), List.of(new Sum(2)), new TableOperator(3, scanTwoKey)), 3));
    }

    @Benchmark
    public void groupSumThreeLongKeys()
    {
        consume(sumAll(new GroupedAggregationOperator(
                allocator, List.of(0, 1, 2), List.of(new Sum(3)), new TableOperator(4, scanThreeKey)), 4));
    }

    // ---- Global aggregation: sum(value) ----

    @Benchmark
    public void aggregationSum()
    {
        consume(new AggregationOperator(
                allocator, List.of(new Sum(0)), new TableOperator(1, scanValues)));
    }

    // ---- Projection: value + value ----

    @Benchmark
    public void project()
    {
        Variable projected = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        projected,
                        new Call("add", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(projected, Stream.VALUES)));
        consume(sumAll(new ProjectOperator(allocator, plan, primitiveRegistry, new TableOperator(1, scanValues)), 1));
    }

    // ---- helpers ----

    private Operator sumAll(Operator source, int columns)
    {
        List<org.weakref.nitro.operator.aggregation.Accumulator> sums = new ArrayList<>();
        for (int column = 0; column < columns; column++) {
            sums.add(new Sum(column));
        }
        return new AggregationOperator(allocator, sums, source);
    }

    private List<Page> twoColumnPages(int totalRows, LongUnaryOperator keyFn, LongUnaryOperator payloadFn)
    {
        List<Page> pages = new ArrayList<>();
        for (int start = 0; start < totalRows; start += batch) {
            int rows = Math.min(batch, totalRows - start);
            long[] keys = new long[rows];
            long[] payload = new long[rows];
            for (int index = 0; index < rows; index++) {
                long row = start + index;
                keys[index] = keyFn.applyAsLong(row);
                payload[index] = payloadFn.applyAsLong(row);
            }
            pages.add(Page.values(rows, new Vector[] {new I64Vector(keys), new I64Vector(payload)}, Mask.all(rows)));
        }
        return pages;
    }

    private List<Page> multiColumnPages(int totalRows, LongUnaryOperator[] columnFns)
    {
        int columns = columnFns.length;
        List<Page> pages = new ArrayList<>();
        for (int start = 0; start < totalRows; start += batch) {
            int rows = Math.min(batch, totalRows - start);
            Vector[] vectors = new Vector[columns];
            for (int column = 0; column < columns; column++) {
                long[] values = new long[rows];
                LongUnaryOperator fn = columnFns[column];
                for (int index = 0; index < rows; index++) {
                    values[index] = fn.applyAsLong(start + index);
                }
                vectors[column] = new I64Vector(values);
            }
            pages.add(Page.values(rows, vectors, Mask.all(rows)));
        }
        return pages;
    }

    private List<Page> oneColumnPages(int totalRows, LongUnaryOperator valueFn)
    {
        List<Page> pages = new ArrayList<>();
        for (int start = 0; start < totalRows; start += batch) {
            int rows = Math.min(batch, totalRows - start);
            long[] values = new long[rows];
            for (int index = 0; index < rows; index++) {
                values[index] = valueFn.applyAsLong(start + index);
            }
            pages.add(Page.values(rows, new Vector[] {new I64Vector(values)}, Mask.all(rows)));
        }
        return pages;
    }

    private static void consume(Operator operator)
    {
        try (operator) {
            while (operator.hasNext()) {
                try (var batch = operator.next()) {
                    Mask mask = batch.borrowMask();
                    if (mask.none()) {
                        continue;
                    }
                    for (int column = 0; column < operator.outputCount(); column++) {
                        consume(batch.output(column).borrow(Stream.VALUES));
                    }
                }
            }
        }
    }

    private static void consume(Vector vector)
    {
        if (vector.length() == Integer.MIN_VALUE) {
            throw new AssertionError();
        }
    }
}
