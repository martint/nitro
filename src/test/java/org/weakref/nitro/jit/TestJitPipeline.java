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

import java.util.List;

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
        CompiledPipeline.Result result = compiled.execute(new long[][] {a, b}, rows);

        System.out.println("=== generated source ===\n" + PipelineCompiler.render(pipeline));
        assertThat(result.rowCount()).isEqualTo(1);
        assertThat(result.columns()[0][0]).isEqualTo(expectedSum);
        assertThat(result.columns()[1][0]).isEqualTo(expectedCount);
    }
}
