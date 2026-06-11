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
package org.weakref.nitro.tpch;

import org.weakref.nitro.trino.TrinoTpchSupport;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Dumps Trino's optimized logical plans for the 22 TPC-H queries -- the authoritative shapes the harness
 * operator trees mirror (the TPC-H twin of {@link org.weakref.nitro.tpcds.ExplainUnsupportedQueries}).
 */
public final class ExplainTpchQueries
{
    private ExplainTpchQueries() {}

    public static void main(String[] args)
    {
        String schema = System.getProperty("nitro.tpch.explain.schema", args.length > 0 ? args[0] : "sf10");
        Path outputDirectory = Path.of(System.getProperty("nitro.tpch.explain.output", "target/tpch-explain")).toAbsolutePath().normalize();
        try {
            Files.createDirectories(outputDirectory);
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to create explain output directory: " + outputDirectory, exception);
        }

        try (TrinoTpchSupport support = new TrinoTpchSupport(schema)) {
            for (String queryId : TpchQueryCatalog.queryIds()) {
                String explain = support.explainQuery(queryId);
                Path file = outputDirectory.resolve("q" + queryId + ".explain.txt");
                try {
                    Files.writeString(file, explain);
                }
                catch (IOException exception) {
                    throw new UncheckedIOException("Unable to write " + file, exception);
                }
                System.out.println("Wrote " + file);
            }
        }
    }
}
