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

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(new long[][][] {{k, v}}, new int[] {rows});
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
    }
}
