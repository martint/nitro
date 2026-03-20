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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.OperatorAssertions.operator;
import static org.weakref.nitro.data.Row.row;

public class TestClickBenchHitsQueries
{
    @TempDir
    Path tempDirectory;

    private Path writeHitsFixture()
            throws IOException
    {
        return ClickBenchHitsSupport.writeHitsFixture(tempDirectory.resolve("clickbench-hits.parquet"), ClickBenchHitsSupport.DEFAULT_ROW_COUNT);
    }

    @Test
    void testClickBenchQuery1CountAll()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query1CountAll(new Allocator(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(row(7L)));
        }
    }

    @Test
    void testClickBenchQuery2CountNonZeroAdvEngineId()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query2CountNonZeroAdvEngineId(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(row(5L)));
        }
    }

    @Test
    void testClickBenchQuery7MinAndMaxEventDate()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query7MinAndMaxEventDate(new Allocator(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(row(20130701L, 20130801L)));
        }
    }

    @Test
    void testClickBenchQuery8GroupByAdvEngineId()
            throws IOException
    {
        Allocator allocator = new Allocator();
        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        try (Operator query = ClickBenchHitsSupport.query8GroupByAdvEngineId(allocator, primitiveRegistry, writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row(20L, 3L),
                    row(10L, 2L)));
        }
    }

    @Test
    void testClickBenchQuery21CountUrlsContainingGoogle()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query21CountUrlsContainingGoogle(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(row(3L)));
        }
    }
}
