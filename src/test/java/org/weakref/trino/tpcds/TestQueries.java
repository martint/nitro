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
package org.weakref.trino.tpcds;

import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestInstance;
import org.weakref.nitro.tpcds.TpcdsQueryCatalog;
import org.weakref.nitro.trino.TrinoTpcdsSupport;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestQueries
{
    private static final String TPCDS_SMOKE_SCHEMA_PROPERTY = "nitro.tpcds.smoke.schema";
    private static final String TPCDS_SMOKE_HEAVY_SCHEMA_PROPERTY = "nitro.tpcds.smoke.heavy.schema";
    private static final String TPCDS_SMOKE_DIVISION_SCHEMA_PROPERTY = "nitro.tpcds.smoke.division.schema";

    private TrinoTpcdsSupport support;

    @BeforeAll
    void setUp()
    {
        assumeTrue(TpcdsQueryCatalog.isAvailable(), "Set -D" + TpcdsQueryCatalog.TPCDS_TRINO_ROOT_PROPERTY + "=/path/to/trino to enable TPC-DS query tests");
        support = new TrinoTpcdsSupport(smokeSchema());
    }

    @AfterAll
    void tearDown()
    {
        if (support != null) {
            support.close();
        }
    }

    @TestFactory
    Stream<DynamicTest> testQueries()
    {
        return TpcdsQueryCatalog.benchmarkQueryIds().stream()
                .map(queryId -> DynamicTest.dynamicTest("q" + queryId, () -> {
                    MaterializedResult result = support.executeBenchmarkQuery(queryId, schemaForQuery(queryId));
                    assertThat(result).isNotNull();
                    assertThat(result.getTypes()).isNotEmpty();
                    assertThat(result.getRowCount()).isGreaterThanOrEqualTo(0);
                }));
    }

    @Test
    void testHighRiskQueries()
    {
        assertThat(support.executeBenchmarkQuery("12", schemaForQuery("12"))).isNotNull();
        assertThat(support.executeBenchmarkQuery("64", schemaForQuery("64"))).isNotNull();
        assertThat(support.executeBenchmarkQuery("90", schemaForQuery("90"))).isNotNull();
    }

    private static String smokeSchema()
    {
        return System.getProperty(TPCDS_SMOKE_SCHEMA_PROPERTY, "sf0.01");
    }

    private static String heavySmokeSchema()
    {
        return System.getProperty(TPCDS_SMOKE_HEAVY_SCHEMA_PROPERTY, "sf0.001");
    }

    private static String divisionSafeSchema()
    {
        return System.getProperty(TPCDS_SMOKE_DIVISION_SCHEMA_PROPERTY, "sf1");
    }

    private static String schemaForQuery(String queryId)
    {
        return switch (queryId) {
            case "12", "90" -> divisionSafeSchema();
            case "64" -> heavySmokeSchema();
            default -> smokeSchema();
        };
    }
}
