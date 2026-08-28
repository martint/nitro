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
package org.weakref.nitro.clickbench;

import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class TestClickBenchBenchmarkNumbering
{
    private static final List<String> PUBLISHED_QUERY_IDS = IntStream.rangeClosed(0, 42)
            .mapToObj(query -> "query%02d".formatted(query))
            .toList();

    @Test
    void testOperatorBenchmarksUsePublishedZeroBasedIds()
    {
        assertBenchmarkIds(BenchmarkQueries.class);
        assertBenchmarkIds(org.weakref.trino.clickbench.BenchmarkQueries.class);
    }

    @Test
    void testCompiledBenchmarksUsePublishedZeroBasedIds()
            throws ReflectiveOperationException
    {
        Param queryIds = BenchmarkCompiledQueries.class.getField("query").getAnnotation(Param.class);
        assertThat(queryIds.value())
                .containsExactlyElementsOf(PUBLISHED_QUERY_IDS.stream()
                        .map(id -> id.substring("query".length()))
                        .toList());
    }

    private static void assertBenchmarkIds(Class<?> benchmarkClass)
    {
        List<String> queryIds = Arrays.stream(benchmarkClass.getMethods())
                .filter(method -> method.isAnnotationPresent(Benchmark.class))
                .map(method -> method.getName())
                .filter(name -> name.startsWith("query"))
                .sorted()
                .toList();
        assertThat(queryIds).containsExactlyElementsOf(PUBLISHED_QUERY_IDS);
        assertThat(benchmarkClass.getMethods())
                .anySatisfy(method -> {
                    assertThat(method.getName()).isEqualTo("allColumnsScan");
                    assertThat(method.isAnnotationPresent(Benchmark.class)).isTrue();
                });
    }
}
