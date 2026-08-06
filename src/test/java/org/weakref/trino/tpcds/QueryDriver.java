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

import org.weakref.nitro.tpcds.TpcdsParquetTables;
import org.weakref.nitro.trino.TrinoOperatorCpuProfile;
import org.weakref.nitro.trino.TrinoTpcdsParquetSupport;

/** Runs one Trino operator-based TPC-DS query in a warmed loop for focused profiling. */
public final class QueryDriver
{
    private QueryDriver() {}

    public static void main(String[] args)
    {
        String query = args[0].matches("\\d+") ? String.format("query%02d", Integer.parseInt(args[0])) : args[0];
        if (!query.equals("query67")) {
            throw new IllegalArgumentException("Operator profiling is not yet wired for " + query);
        }
        int warmup = args.length > 1 ? Integer.parseInt(args[1]) : 5;
        int measured = args.length > 2 ? Integer.parseInt(args[2]) : 1;
        TpcdsParquetTables tables = TpcdsParquetTables.requiredActual();

        try (TrinoTpcdsParquetSupport support = new TrinoTpcdsParquetSupport()) {
            long sink = 0;
            for (int iteration = 0; iteration < warmup; iteration++) {
                sink += support.query67(tables).getRowCount();
            }

            TrinoOperatorCpuProfile profile = new TrinoOperatorCpuProfile();
            long start = System.nanoTime();
            for (int iteration = 0; iteration < measured; iteration++) {
                sink += TrinoTpcdsParquetSupport.withOperatorCpuProfile(profile, () -> support.query67(tables)).getRowCount();
            }
            long nanos = System.nanoTime() - start;
            System.out.printf("%s: %d iters, %.1f ms/iter, sink=%d%n", query, measured, nanos / 1e6 / measured, sink);
            System.out.println(profile.formatReport());
        }
    }
}
