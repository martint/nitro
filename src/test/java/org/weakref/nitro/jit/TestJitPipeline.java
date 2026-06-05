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
