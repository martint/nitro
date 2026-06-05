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
package org.weakref.nitro.jit;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

public class TestJitPipeline
{
    @Test
    void compilesAndComputesGlobalAggregate()
    {
        // SELECT sum(a*b), count(*) WHERE a > 500
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(new Plan.Predicate(">", new Plan.Col(0), new Plan.Lit(500))),
                List.of(),
                List.of(
                        new Plan.Aggregate("sum", new Plan.Bin("*", new Plan.Col(0), new Plan.Col(1))),
                        new Plan.Aggregate("count", null)));

        int rows = 100_000;
        long[] a = new long[rows];
        long[] b = new long[rows];
        long expectedSum = 0;
        long expectedCount = 0;
        for (int i = 0; i < rows; i++) {
            a[i] = i % 1000;
            b[i] = (i % 7) + 1;
            if (a[i] > 500) {
                expectedSum += a[i] * b[i];
                expectedCount++;
            }
        }

        CompiledPipeline compiled = PipelineCompiler.compile(pipeline);
        CompiledPipeline.Result result = compiled.execute(new long[][][] {{a, b}}, new int[] {rows});

        System.out.println("=== generated source ===\n" + PipelineCompiler.render(pipeline));
        assertThat(result.rowCount()).isEqualTo(1);
        assertThat(result.columns()[0][0]).isEqualTo(expectedSum);
        assertThat(result.columns()[1][0]).isEqualTo(expectedCount);
    }

    @Test
    void compilesAndComputesGroupedAggregate()
    {
        // SELECT k, sum(v), count(*) WHERE v > 0 GROUP BY k
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(new Plan.Predicate(">", new Plan.Col(1), new Plan.Lit(0))),
                List.of(new Plan.Col(0)),
                List.of(
                        new Plan.Aggregate("sum", new Plan.Col(1)),
                        new Plan.Aggregate("count", null)));

        int rows = 200_000;
        long[] k = new long[rows];
        long[] v = new long[rows];
        Map<Long, long[]> reference = new HashMap<>();   // key -> {sum, count}
        for (int i = 0; i < rows; i++) {
            k[i] = i % 5000;        // 5000 distinct groups
            v[i] = (i % 11) - 2;    // includes negatives -> filtered
            if (v[i] > 0) {
                long[] acc = reference.computeIfAbsent(k[i], ignored -> new long[2]);
                acc[0] += v[i];
                acc[1]++;
            }
        }

        System.out.println("=== generated grouped source ===\n" + PipelineCompiler.render(pipeline));

        // Dense keys in [0, 5000): the sample speculates array mode and the bet holds (no deopt).
        verifyGroupedSumCount(pipeline, k, v, reference);

        // Deopt: the first SAMPLE_SIZE rows look narrow ([0, 50)), so the array is sized small, then later
        // rows reach up to 4999 and force a mid-stream migration into the hash table. Results must still match.
        long[] kDeopt = new long[rows];
        long[] vDeopt = new long[rows];
        Map<Long, long[]> deoptReference = new HashMap<>();
        for (int i = 0; i < rows; i++) {
            kDeopt[i] = i < 4096 ? i % 50 : i % 5000;
            vDeopt[i] = (i % 11) - 2;
            if (vDeopt[i] > 0) {
                long[] acc = deoptReference.computeIfAbsent(kDeopt[i], ignored -> new long[2]);
                acc[0] += vDeopt[i];
                acc[1]++;
            }
        }
        verifyGroupedSumCount(pipeline, kDeopt, vDeopt, deoptReference);

        // Never speculate: the key domain exceeds the array cap, so it runs on the hash table from row 0.
        long[] kSparse = new long[rows];
        long[] vSparse = new long[rows];
        Map<Long, long[]> sparseReference = new HashMap<>();
        for (int i = 0; i < rows; i++) {
            kSparse[i] = (i % 1000) * 1_000_000L;   // max ~1e9 > array cap
            vSparse[i] = (i % 11) - 2;
            if (vSparse[i] > 0) {
                long[] acc = sparseReference.computeIfAbsent(kSparse[i], ignored -> new long[2]);
                acc[0] += vSparse[i];
                acc[1]++;
            }
        }
        verifyGroupedSumCount(pipeline, kSparse, vSparse, sparseReference);
    }

    private static void verifyGroupedSumCount(Plan.Pipeline pipeline, long[] k, long[] v, Map<Long, long[]> reference)
    {
        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(new long[][][] {{k, v}}, new int[] {k.length});
        assertThat(result.rowCount()).isEqualTo(reference.size());
        long[] keys = result.columns()[0];
        long[] sums = result.columns()[1];
        long[] counts = result.columns()[2];
        for (int g = 0; g < result.rowCount(); g++) {
            long[] expected = reference.get(keys[g]);
            assertThat(expected).as("group %d", keys[g]).isNotNull();
            assertThat(sums[g]).as("sum for group %d", keys[g]).isEqualTo(expected[0]);
            assertThat(counts[g]).as("count for group %d", keys[g]).isEqualTo(expected[1]);
        }
    }

    @Test
    void compilesAndComputesMultiKeyGroupedAggregate()
    {
        // SELECT k0, k1, sum(v), count(*) GROUP BY k0, k1
        Plan.Pipeline pipeline = new Plan.Pipeline(
                3,
                List.of(),
                List.of(new Plan.Col(0), new Plan.Col(1)),
                List.of(
                        new Plan.Aggregate("sum", new Plan.Col(2)),
                        new Plan.Aggregate("count", null)));

        int rows = 200_000;
        long[] k0 = new long[rows];
        long[] k1 = new long[rows];
        long[] v = new long[rows];
        Map<List<Long>, long[]> reference = new HashMap<>();   // (k0, k1) -> {sum, count}
        for (int i = 0; i < rows; i++) {
            k0[i] = i % 50;
            k1[i] = i % 37;        // 50 * 37 = 1850 distinct composite groups
            v[i] = i % 13;
            long[] acc = reference.computeIfAbsent(List.of(k0[i], k1[i]), ignored -> new long[2]);
            acc[0] += v[i];
            acc[1]++;
        }

        System.out.println("=== generated multi-key grouped source ===\n" + PipelineCompiler.render(pipeline));

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(new long[][][] {{k0, k1, v}}, new int[] {rows});
        assertThat(result.rowCount()).isEqualTo(reference.size());

        long[] outK0 = result.columns()[0];
        long[] outK1 = result.columns()[1];
        long[] sums = result.columns()[2];
        long[] counts = result.columns()[3];
        for (int g = 0; g < result.rowCount(); g++) {
            long[] expected = reference.get(List.of(outK0[g], outK1[g]));
            assertThat(expected).as("group (%d, %d)", outK0[g], outK1[g]).isNotNull();
            assertThat(sums[g]).as("sum for group (%d, %d)", outK0[g], outK1[g]).isEqualTo(expected[0]);
            assertThat(counts[g]).as("count for group (%d, %d)", outK0[g], outK1[g]).isEqualTo(expected[1]);
        }
    }

    @Test
    void compilesAndComputesJoinGroupedAggregate()
    {
        // fact(fk, measure) JOIN dim(dkey, dattr) ON fk=dkey GROUP BY dattr, sum(measure)
        // combined columns: probe [0=fk, 1=measure], build [2=dkey, 3=dattr]
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                new Plan.Build(2, 0),
                0,
                List.of(),
                List.of(new Plan.Col(3)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(1))));

        int dim = 1000;
        int groups = 20;
        long[] dkey = new long[dim];
        long[] dattr = new long[dim];
        for (int d = 0; d < dim; d++) {
            dkey[d] = d;
            dattr[d] = d % groups;
        }
        int fact = 500_000;
        long[] fk = new long[fact];
        long[] measure = new long[fact];
        Map<Long, Long> reference = new HashMap<>();
        for (int i = 0; i < fact; i++) {
            fk[i] = i % dim;
            measure[i] = (i % 50) + 1;
            long attr = dattr[(int) fk[i]];
            reference.merge(attr, measure[i], Long::sum);
        }

        System.out.println("=== generated join source ===\n" + PipelineCompiler.render(pipeline));

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline)
                .execute(new long[][][] {{fk, measure}, {dkey, dattr}}, new int[] {fact, dim});

        assertThat(result.rowCount()).isEqualTo(reference.size());
        long[] keys = result.columns()[0];
        long[] sums = result.columns()[1];
        for (int g = 0; g < result.rowCount(); g++) {
            assertThat(sums[g]).as("sum for attr %d", keys[g]).isEqualTo(reference.get(keys[g]));
        }

        // Same adaptive plan, but sparse build keys (range >> rows) force the runtime to fall back to the
        // hash table instead of the array. Results must be identical.
        long stride = 1_000_000;
        long[] sparseKey = new long[dim];
        for (int d = 0; d < dim; d++) {
            sparseKey[d] = d * stride;
        }
        long[] sparseFk = new long[fact];
        Map<Long, Long> sparseReference = new HashMap<>();
        for (int i = 0; i < fact; i++) {
            int d = i % dim;
            sparseFk[i] = sparseKey[d];
            sparseReference.merge(dattr[d], measure[i], Long::sum);
        }
        CompiledPipeline.Result sparseResult = PipelineCompiler.compile(pipeline)
                .execute(new long[][][] {{sparseFk, measure}, {sparseKey, dattr}}, new int[] {fact, dim});
        assertThat(sparseResult.rowCount()).isEqualTo(sparseReference.size());
        long[] sKeys = sparseResult.columns()[0];
        long[] sSums = sparseResult.columns()[1];
        for (int g = 0; g < sparseResult.rowCount(); g++) {
            assertThat(sSums[g]).as("sparse sum for attr %d", sKeys[g]).isEqualTo(sparseReference.get(sKeys[g]));
        }
    }

    @Test
    void compilesStddevAggregate()
    {
        // SELECT k, stddev(v) GROUP BY k -- 3-cell aggregate, double result; added by registration only.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(),
                List.of(new Plan.Col(0)),
                List.of(new Plan.Aggregate("stddev", new Plan.Col(1))));

        int rows = 60_000;
        long[] k = new long[rows];
        long[] v = new long[rows];
        Map<Long, List<Long>> groups = new HashMap<>();
        for (int i = 0; i < rows; i++) {
            k[i] = i % 40;
            v[i] = (i % 17);
            groups.computeIfAbsent(k[i], ignored -> new java.util.ArrayList<>()).add(v[i]);
        }

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(new long[][][] {{k, v}}, new int[] {rows});
        assertThat(result.types()[1]).isEqualTo(ColumnType.DOUBLE);
        long[] keys = result.columns()[0];
        long[] bits = result.columns()[1];
        for (int g = 0; g < result.rowCount(); g++) {
            List<Long> values = groups.get(keys[g]);
            double mean = values.stream().mapToLong(Long::longValue).average().orElse(0);
            double ss = values.stream().mapToDouble(x -> (x - mean) * (x - mean)).sum();
            double expected = values.size() < 2 ? 0.0 : Math.sqrt(ss / (values.size() - 1));
            assertThat(Double.longBitsToDouble(bits[g])).as("stddev for k %d", keys[g]).isEqualTo(expected, within(1e-6));
        }
    }

    @Test
    void compilesAvgWithDoubleResult()
    {
        // SELECT k, sum(v), count(*), avg(v) GROUP BY k -- avg is a 2-cell aggregate with a double result.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(),
                List.of(new Plan.Col(0)),
                List.of(
                        new Plan.Aggregate("sum", new Plan.Col(1)),
                        new Plan.Aggregate("count", null),
                        new Plan.Aggregate("avg", new Plan.Col(1))));

        int rows = 100_000;
        long[] k = new long[rows];
        long[] v = new long[rows];
        Map<Long, long[]> sumCount = new HashMap<>();   // k -> {sum, count}
        for (int i = 0; i < rows; i++) {
            k[i] = i % 137;
            v[i] = (i % 50) + 1;
            long[] acc = sumCount.computeIfAbsent(k[i], ignored -> new long[2]);
            acc[0] += v[i];
            acc[1]++;
        }

        System.out.println("=== generated avg source ===\n" + PipelineCompiler.render(pipeline));

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(new long[][][] {{k, v}}, new int[] {rows});
        assertThat(result.rowCount()).isEqualTo(sumCount.size());
        // Result columns: 0=k, 1=sum (LONG), 2=count (LONG), 3=avg (DOUBLE).
        assertThat(result.types()[3]).isEqualTo(ColumnType.DOUBLE);
        assertThat(result.types()[1]).isEqualTo(ColumnType.LONG);
        long[] keys = result.columns()[0];
        long[] sums = result.columns()[1];
        long[] counts = result.columns()[2];
        long[] avgBits = result.columns()[3];
        for (int g = 0; g < result.rowCount(); g++) {
            long[] expected = sumCount.get(keys[g]);
            assertThat(sums[g]).isEqualTo(expected[0]);
            assertThat(counts[g]).isEqualTo(expected[1]);
            double avg = Double.longBitsToDouble(avgBits[g]);
            assertThat(avg).as("avg for k %d", keys[g]).isEqualTo((double) expected[0] / expected[1], within(1e-9));
        }
    }

    @Test
    void compilesHavingAndOrderBy()
    {
        // SELECT k, sum(v) GROUP BY k HAVING sum(v) > T ORDER BY k LIMIT 20
        long threshold = 700_000;
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(),
                List.of(new Plan.Col(0)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(1))))
                .withHaving(new Plan.Predicate(">", new Plan.Col(1), new Plan.Lit(threshold)))
                .withOrdering(new Plan.Ordering(List.of(new Plan.SortKey(0, false)), 20));

        int rows = 400_000;
        long[] k = new long[rows];
        long[] v = new long[rows];
        Map<Long, Long> sums = new HashMap<>();
        for (int i = 0; i < rows; i++) {
            k[i] = i % 50;
            v[i] = (i % 100) + 1;
            sums.merge(k[i], v[i], Long::sum);
        }
        List<long[]> qualifying = new java.util.ArrayList<>();
        sums.forEach((key, sum) -> {
            if (sum > threshold) {
                qualifying.add(new long[] {key, sum});
            }
        });
        qualifying.sort((a, b) -> Long.compare(a[0], b[0]));
        List<long[]> expected = qualifying.size() > 20 ? qualifying.subList(0, 20) : qualifying;

        System.out.println("=== generated having source ===\n" + PipelineCompiler.render(pipeline));

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(new long[][][] {{k, v}}, new int[] {rows});
        assertThat(result.rowCount()).isEqualTo(expected.size());
        long[] outKeys = result.columns()[0];
        long[] outSums = result.columns()[1];
        for (int r = 0; r < expected.size(); r++) {
            assertThat(outKeys[r]).isEqualTo(expected.get(r)[0]);
            assertThat(outSums[r]).isEqualTo(expected.get(r)[1]);
            assertThat(outSums[r]).isGreaterThan(threshold);
        }
    }

    @Test
    void compilesOrderByLimit()
    {
        // SELECT k, sum(v) GROUP BY k ORDER BY sum(v) DESC, k ASC LIMIT 10
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(),
                List.of(new Plan.Col(0)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(1))))
                .withOrdering(new Plan.Ordering(
                        List.of(new Plan.SortKey(1, true), new Plan.SortKey(0, false)),
                        10));

        int rows = 200_000;
        long[] k = new long[rows];
        long[] v = new long[rows];
        Map<Long, Long> sums = new HashMap<>();
        for (int i = 0; i < rows; i++) {
            k[i] = i % 137;
            v[i] = (i % 100) + 1;
            sums.merge(k[i], v[i], Long::sum);
        }
        // Reference top-10 by sum desc, then key asc.
        List<long[]> expected = new java.util.ArrayList<>();
        sums.forEach((key, sum) -> expected.add(new long[] {key, sum}));
        expected.sort((a, b) -> a[1] != b[1] ? Long.compare(b[1], a[1]) : Long.compare(a[0], b[0]));

        System.out.println("=== generated order-by source ===\n" + PipelineCompiler.render(pipeline));

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(new long[][][] {{k, v}}, new int[] {rows});
        assertThat(result.rowCount()).isEqualTo(10);
        long[] outKeys = result.columns()[0];
        long[] outSums = result.columns()[1];
        for (int r = 0; r < 10; r++) {
            assertThat(outKeys[r]).as("key at rank %d", r).isEqualTo(expected.get(r)[0]);
            assertThat(outSums[r]).as("sum at rank %d", r).isEqualTo(expected.get(r)[1]);
        }
    }

    @Test
    void compilesScalarFunctionsAndCase()
    {
        // SELECT k, sum(CASE WHEN v < 0 THEN -v ELSE v END), sum(v / 2), max(abs(v)) GROUP BY k
        Plan.Expr absViaCase = new Plan.Case(
                List.of(new Plan.Case.Branch(
                        new Plan.Predicate("<", new Plan.Col(1), new Plan.Lit(0)),
                        new Plan.Call("negate", new Plan.Col(1)))),
                new Plan.Col(1));
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(),
                List.of(new Plan.Col(0)),
                List.of(
                        new Plan.Aggregate("sum", absViaCase),
                        new Plan.Aggregate("sum", new Plan.Bin("/", new Plan.Col(1), new Plan.Lit(2))),
                        new Plan.Aggregate("max", new Plan.Call("abs", new Plan.Col(1)))));

        int rows = 100_000;
        long[] k = new long[rows];
        long[] v = new long[rows];
        Map<Long, long[]> reference = new HashMap<>();   // k -> {sum|v|, sum(v/2), max|v|}
        for (int i = 0; i < rows; i++) {
            k[i] = i % 100;
            v[i] = (i % 201) - 100;     // negatives and positives
            long[] acc = reference.computeIfAbsent(k[i], ignored -> new long[] {0, 0, Long.MIN_VALUE});
            acc[0] += Math.abs(v[i]);
            acc[1] += v[i] / 2;         // Java long division (truncates toward zero), matching the codegen
            acc[2] = Math.max(acc[2], Math.abs(v[i]));
        }

        System.out.println("=== generated scalar/case source ===\n" + PipelineCompiler.render(pipeline));

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(new long[][][] {{k, v}}, new int[] {rows});
        assertThat(result.rowCount()).isEqualTo(reference.size());
        long[] keys = result.columns()[0];
        long[] absSum = result.columns()[1];
        long[] halfSum = result.columns()[2];
        long[] maxAbs = result.columns()[3];
        for (int g = 0; g < result.rowCount(); g++) {
            long[] expected = reference.get(keys[g]);
            assertThat(absSum[g]).as("sum|v| for k %d", keys[g]).isEqualTo(expected[0]);
            assertThat(halfSum[g]).as("sum(v/2) for k %d", keys[g]).isEqualTo(expected[1]);
            assertThat(maxAbs[g]).as("max|v| for k %d", keys[g]).isEqualTo(expected[2]);
        }
    }

    @Test
    void compilesAndComputesEncodedColumns()
    {
        // SELECT k, sum(v), count(*) GROUP BY k, with k dictionary-encoded and v a constant column.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(),
                List.of(new Plan.Col(0)),
                List.of(
                        new Plan.Aggregate("sum", new Plan.Col(1)),
                        new Plan.Aggregate("count", null)));

        int rows = 100_000;
        int dictSize = 50;
        long[] dictionary = new long[dictSize];
        for (int d = 0; d < dictSize; d++) {
            dictionary[d] = 1000 + d * 7L;          // sparse-valued dictionary
        }
        int[] ids = new int[rows];
        long constant = 5;
        long[] flatKey = new long[rows];
        long[] flatValue = new long[rows];
        Map<Long, long[]> reference = new HashMap<>();   // value -> {sum, count}
        for (int i = 0; i < rows; i++) {
            ids[i] = i % dictSize;
            flatKey[i] = dictionary[ids[i]];
            flatValue[i] = constant;
            long[] acc = reference.computeIfAbsent(flatKey[i], ignored -> new long[2]);
            acc[0] += constant;
            acc[1]++;
        }

        ColumnEncoding[][] encodings = {{ColumnEncoding.DICTIONARY, ColumnEncoding.CONSTANT}};
        System.out.println("=== generated encoded source ===\n" + PipelineCompiler.render(pipeline, encodings));

        CompiledPipeline compiled = PipelineCompiler.compile(pipeline, encodings);
        Column[][] inputs = {{new Column.DictionaryColumn(ids, dictionary), new Column.ConstantColumn(constant)}};
        CompiledPipeline.Result result = compiled.execute(inputs, new int[] {rows});

        assertThat(result.rowCount()).isEqualTo(reference.size());
        long[] keys = result.columns()[0];
        long[] sums = result.columns()[1];
        long[] counts = result.columns()[2];
        for (int g = 0; g < result.rowCount(); g++) {
            long[] expected = reference.get(keys[g]);
            assertThat(expected).as("group %d", keys[g]).isNotNull();
            assertThat(sums[g]).as("sum for group %d", keys[g]).isEqualTo(expected[0]);
            assertThat(counts[g]).as("count for group %d", keys[g]).isEqualTo(expected[1]);
        }

        // The flat-input plan over decoded columns must produce the identical grouping.
        CompiledPipeline.Result flat = PipelineCompiler.compile(pipeline).execute(new long[][][] {{flatKey, flatValue}}, new int[] {rows});
        assertThat(flat.rowCount()).isEqualTo(result.rowCount());
    }

    @Test
    void compilesAndComputesMinMaxGroupedAggregate()
    {
        // SELECT k, min(v), max(v), sum(v) GROUP BY k -- min/max are registered in AggregateLibrary with no
        // compiler changes. The deopt scenario exercises their identity and merge fragments.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(),
                List.of(new Plan.Col(0)),
                List.of(
                        new Plan.Aggregate("min", new Plan.Col(1)),
                        new Plan.Aggregate("max", new Plan.Col(1)),
                        new Plan.Aggregate("sum", new Plan.Col(1))));

        int rows = 200_000;
        long[] k = new long[rows];
        long[] v = new long[rows];
        // Narrow first, then wide: forces the speculative array group to deopt mid-stream into the hash table.
        Map<Long, long[]> reference = new HashMap<>();   // k -> {min, max, sum}
        for (int i = 0; i < rows; i++) {
            k[i] = i < 4096 ? i % 30 : i % 4000;
            v[i] = ((i * 2654435761L) % 1000) - 500;     // spread of positives and negatives
            long[] acc = reference.computeIfAbsent(k[i], ignored -> new long[] {Long.MAX_VALUE, Long.MIN_VALUE, 0});
            acc[0] = Math.min(acc[0], v[i]);
            acc[1] = Math.max(acc[1], v[i]);
            acc[2] += v[i];
        }

        System.out.println("=== generated min/max source ===\n" + PipelineCompiler.render(pipeline));

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(new long[][][] {{k, v}}, new int[] {rows});
        assertThat(result.rowCount()).isEqualTo(reference.size());
        long[] keys = result.columns()[0];
        long[] mins = result.columns()[1];
        long[] maxs = result.columns()[2];
        long[] sums = result.columns()[3];
        for (int g = 0; g < result.rowCount(); g++) {
            long[] expected = reference.get(keys[g]);
            assertThat(mins[g]).as("min for k %d", keys[g]).isEqualTo(expected[0]);
            assertThat(maxs[g]).as("max for k %d", keys[g]).isEqualTo(expected[1]);
            assertThat(sums[g]).as("sum for k %d", keys[g]).isEqualTo(expected[2]);
        }
    }

    @Test
    void compilesAndComputesBooleanFilterTree()
    {
        // SELECT k, sum(v) WHERE (k IN (1, 3, 5) OR k BETWEEN 10 AND 12) AND NOT (v == 0) GROUP BY k
        Plan.Condition inList = new Plan.Or(
                new Plan.Predicate("==", new Plan.Col(0), new Plan.Lit(1)),
                new Plan.Predicate("==", new Plan.Col(0), new Plan.Lit(3)),
                new Plan.Predicate("==", new Plan.Col(0), new Plan.Lit(5)));
        Plan.Condition between = new Plan.And(
                new Plan.Predicate(">=", new Plan.Col(0), new Plan.Lit(10)),
                new Plan.Predicate("<=", new Plan.Col(0), new Plan.Lit(12)));
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(
                        new Plan.Or(inList, between),
                        new Plan.Not(new Plan.Predicate("==", new Plan.Col(1), new Plan.Lit(0)))),
                List.of(new Plan.Col(0)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(1))));

        int rows = 100_000;
        long[] k = new long[rows];
        long[] v = new long[rows];
        Map<Long, Long> reference = new HashMap<>();
        for (int i = 0; i < rows; i++) {
            k[i] = i % 20;
            v[i] = (i % 5) - 2;       // includes 0 -> excluded by NOT (v == 0)
            boolean keep = (k[i] == 1 || k[i] == 3 || k[i] == 5 || (k[i] >= 10 && k[i] <= 12)) && v[i] != 0;
            if (keep) {
                reference.merge(k[i], v[i], Long::sum);
            }
        }

        System.out.println("=== generated boolean-filter source ===\n" + PipelineCompiler.render(pipeline));

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(new long[][][] {{k, v}}, new int[] {rows});
        assertThat(result.rowCount()).isEqualTo(reference.size());
        long[] keys = result.columns()[0];
        long[] sums = result.columns()[1];
        for (int g = 0; g < result.rowCount(); g++) {
            assertThat(sums[g]).as("sum for k %d", keys[g]).isEqualTo(reference.get(keys[g]));
        }
    }

    @Test
    void compilesAndComputesTwoJoinStarGroupedAggregate()
    {
        // fact(fk_a, fk_b, measure) JOIN dimA(da_key, da_attr) ON fk_a=da_key JOIN dimB(db_key, db_attr) ON
        // fk_b=db_key WHERE db_attr >= 2 GROUP BY da_attr, sum(measure)
        // combined columns: probe [0=fk_a, 1=fk_b, 2=measure], dimA [3=da_key, 4=da_attr], dimB [5=db_key, 6=db_attr]
        Plan.Pipeline pipeline = new Plan.Pipeline(
                3,
                List.of(
                        new Plan.Join(new Plan.Build(2, 0), 0),
                        new Plan.Join(new Plan.Build(2, 0), 1)),
                List.of(new Plan.Predicate(">=", new Plan.Col(6), new Plan.Lit(2))),
                List.of(new Plan.Col(4)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(2))));

        int dimA = 100;
        int dimB = 50;
        long[] daKey = new long[dimA];
        long[] daAttr = new long[dimA];
        for (int d = 0; d < dimA; d++) {
            daKey[d] = d;
            daAttr[d] = d % 10;
        }
        long[] dbKey = new long[dimB];
        long[] dbAttr = new long[dimB];
        for (int d = 0; d < dimB; d++) {
            dbKey[d] = d;
            dbAttr[d] = d % 5;
        }
        int fact = 500_000;
        long[] fkA = new long[fact];
        long[] fkB = new long[fact];
        long[] measure = new long[fact];
        Map<Long, Long> reference = new HashMap<>();
        for (int i = 0; i < fact; i++) {
            fkA[i] = i % dimA;
            fkB[i] = i % dimB;
            measure[i] = (i % 50) + 1;
            if (dbAttr[(int) fkB[i]] >= 2) {
                reference.merge(daAttr[(int) fkA[i]], measure[i], Long::sum);
            }
        }

        System.out.println("=== generated two-join star source ===\n" + PipelineCompiler.render(pipeline));

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline)
                .execute(new long[][][] {{fkA, fkB, measure}, {daKey, daAttr}, {dbKey, dbAttr}}, new int[] {fact, dimA, dimB});

        assertThat(result.rowCount()).isEqualTo(reference.size());
        long[] keys = result.columns()[0];
        long[] sums = result.columns()[1];
        for (int g = 0; g < result.rowCount(); g++) {
            assertThat(sums[g]).as("sum for da_attr %d", keys[g]).isEqualTo(reference.get(keys[g]));
        }
    }

    @Test
    void compilesAndComputesMultiKeyJoinGroupedAggregate()
    {
        // fact(fk0, fk1, measure) JOIN dim(dk0, dk1, dattr) ON fk0=dk0 AND fk1=dk1 GROUP BY dattr, sum(measure)
        // combined columns: probe [0=fk0, 1=fk1, 2=measure], build [3=dk0, 4=dk1, 5=dattr]
        Plan.Pipeline pipeline = new Plan.Pipeline(
                3,
                new Plan.Build(3, new int[] {0, 1}),
                new int[] {0, 1},
                List.of(),
                List.of(new Plan.Col(5)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(2))));

        int dim0 = 100;
        int dim1 = 10;
        int dim = dim0 * dim1;            // 1000 dim rows, unique (dk0, dk1)
        int groups = 20;
        long[] dk0 = new long[dim];
        long[] dk1 = new long[dim];
        long[] dattr = new long[dim];
        for (int a0 = 0; a0 < dim0; a0++) {
            for (int a1 = 0; a1 < dim1; a1++) {
                int row = a0 * dim1 + a1;
                dk0[row] = a0;
                dk1[row] = a1;
                dattr[row] = row % groups;
            }
        }
        int fact = 500_000;
        long[] fk0 = new long[fact];
        long[] fk1 = new long[fact];
        long[] measure = new long[fact];
        Map<Long, Long> reference = new HashMap<>();
        for (int i = 0; i < fact; i++) {
            fk0[i] = i % dim0;
            fk1[i] = i % dim1;
            measure[i] = (i % 50) + 1;
            long attr = dattr[(int) (fk0[i] * dim1 + fk1[i])];
            reference.merge(attr, measure[i], Long::sum);
        }

        System.out.println("=== generated multi-key join source ===\n" + PipelineCompiler.render(pipeline));

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline)
                .execute(new long[][][] {{fk0, fk1, measure}, {dk0, dk1, dattr}}, new int[] {fact, dim});

        assertThat(result.rowCount()).isEqualTo(reference.size());
        long[] keys = result.columns()[0];
        long[] sums = result.columns()[1];
        for (int g = 0; g < result.rowCount(); g++) {
            assertThat(sums[g]).as("sum for attr %d", keys[g]).isEqualTo(reference.get(keys[g]));
        }
    }
}
