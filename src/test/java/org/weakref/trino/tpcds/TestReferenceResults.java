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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestInstance;
import org.weakref.nitro.tpcds.TpcdsQueryCatalog;
import org.weakref.nitro.tpcds.TpcdsResultAssertions;
import org.weakref.nitro.trino.TrinoTpcdsSupport;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestReferenceResults
{
    private static final String RUN_REFERENCE_RESULTS_PROPERTY = "nitro.tpcds.runReferenceResults";

    private TrinoTpcdsSupport support;

    @BeforeAll
    void setUp()
    {
        assumeTrue(referenceResultsEnabled(), "Set -D" + RUN_REFERENCE_RESULTS_PROPERTY + "=true to enable full TPC-DS reference-result validation");
        assumeTrue(TpcdsQueryCatalog.isAvailable(), "Set -D" + TpcdsQueryCatalog.TPCDS_TRINO_ROOT_PROPERTY + "=/path/to/trino to enable TPC-DS reference-result validation");
        support = new TrinoTpcdsSupport("sf1");
    }

    @AfterAll
    void tearDown()
    {
        if (support != null) {
            support.close();
        }
    }

    @TestFactory
    Stream<DynamicTest> testReferenceQueriesMatchExpectedResults()
    {
        return TpcdsQueryCatalog.referenceQueryIds().stream()
                .map(queryId -> DynamicTest.dynamicTest("q" + queryId, () ->
                        TpcdsResultAssertions.assertMatches(
                                TpcdsQueryCatalog.referenceResultLines(queryId),
                                support.executeReferenceQuery(queryId))));
    }

    private static boolean referenceResultsEnabled()
    {
        return "true".equalsIgnoreCase(System.getProperty(RUN_REFERENCE_RESULTS_PROPERTY));
    }
}
