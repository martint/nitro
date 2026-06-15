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
/*
 * Measurement harness: for every faithful compiled query across TPC-H, TPC-DS and ClickBench, split engine time
 * into Parquet scan/decode vs downstream compute (joins/group/aggregate) via CompiledQuerySupport.ScanProfile.
 * Drives the real BenchmarkCompiledQueries.full() of each suite. Dormant unless -Dnitro.computeSplit=true.
 * The scan FRACTION is the robust output (a within-run ratio); absolute totals run hotter than JMH, so the final
 * analysis multiplies this fraction by the authoritative JMH sweep totals. Output CSV: suite,query,total_ms,scan_ms,scan_frac.
 */
package org.weakref.nitro.tpcds;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class MeasureComputeSplit
{
    private record Row(String suite, String query, double total, double scan, double frac) {}

    @Test
    public void measure()
            throws Exception
    {
        Assumptions.assumeTrue(Boolean.getBoolean("nitro.computeSplit"), "set -Dnitro.computeSplit=true");

        List<Row> rows = new ArrayList<>();
        rows.addAll(run("tpch", tpchQueries(), MeasureComputeSplit::tpchDriver));
        rows.addAll(run("tpcds", tpcdsQueries(), MeasureComputeSplit::tpcdsDriver));
        rows.addAll(run("clickbench", clickbenchQueries(), MeasureComputeSplit::clickbenchDriver));

        Path out = Path.of("benchmarks/compute-split-nitro-20260617.csv");
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out))) {
            w.println("suite,query,total_ms,scan_ms,scan_frac");
            for (Row r : rows) {
                w.printf("%s,%s,%.1f,%.1f,%.3f%n", r.suite(), r.query(), r.total(), r.scan(), r.frac());
            }
        }
        System.out.println("wrote " + out + " (" + rows.size() + " queries)");
    }

    private record Driver(Runnable setupInvocation, Supplier<Object> full) {}

    private interface DriverFactory
    {
        Driver create(String query);
    }

    private static Driver tpchDriver(String query)
    {
        org.weakref.nitro.tpch.BenchmarkCompiledQueries b = new org.weakref.nitro.tpch.BenchmarkCompiledQueries();
        b.query = query;
        b.setup();
        return new Driver(b::setupInvocation, b::full);
    }

    private static Driver tpcdsDriver(String query)
    {
        org.weakref.nitro.tpcds.BenchmarkCompiledQueries b = new org.weakref.nitro.tpcds.BenchmarkCompiledQueries();
        b.query = query;
        b.setup();
        return new Driver(b::setupInvocation, b::full);
    }

    private static Driver clickbenchDriver(String query)
    {
        org.weakref.nitro.clickbench.BenchmarkCompiledQueries b = new org.weakref.nitro.clickbench.BenchmarkCompiledQueries();
        b.query = query;
        b.setup();
        return new Driver(b::setupInvocation, b::full);
    }

    private static List<Row> run(String suite, List<String> queries, DriverFactory factory)
    {
        List<Row> rows = new ArrayList<>();
        for (String q : queries) {
            Driver d;
            try {
                d = factory.create(q);
            }
            catch (Exception e) {
                System.out.printf("%s q%s: setup failed (%s) -- skipped%n", suite, q, e);
                continue;
            }
            try {
                for (int i = 0; i < 2; i++) {
                    d.setupInvocation().run();
                    d.full().get();
                }
                double bestProfiled = Double.MAX_VALUE;
                double total = 0;
                double scan = 0;
                for (int i = 0; i < 3; i++) {
                    d.setupInvocation().run();
                    CompiledQuerySupport.ScanProfile.enable(true);
                    CompiledQuerySupport.ScanProfile.reset();
                    long t0 = System.nanoTime();
                    d.full().get();
                    double profiled = (System.nanoTime() - t0) / 1e6;
                    double s = CompiledQuerySupport.ScanProfile.scanNanos() / 1e6;
                    CompiledQuerySupport.ScanProfile.enable(false);
                    if (profiled < bestProfiled) {
                        bestProfiled = profiled;
                        total = profiled;
                        scan = s;
                    }
                }
                rows.add(new Row(suite, q, total, scan, total > 0 ? scan / total : 0));
                System.out.printf("%s q%s: total=%.0f scan=%.0f frac=%.2f%n", suite, q, total, scan, total > 0 ? scan / total : 0);
            }
            catch (RuntimeException e) {
                System.out.printf("%s q%s: run failed (%s) -- skipped%n", suite, q, e);
            }
        }
        return rows;
    }

    private static List<String> tpchQueries()
    {
        List<String> q = new ArrayList<>();
        for (int i = 1; i <= 22; i++) {
            q.add(String.format("%02d", i));
        }
        return q;
    }

    private static List<String> tpcdsQueries()
    {
        return List.of(("01,03,04,05,06,07,08,09,10,11,12,14,15,16,17,19,20,21,23,25,26,27,28,29,30,32,35,36,38,40,"
                + "41,42,43,45,46,47,49,50,51,52,53,54,55,57,58,61,62,63,64,65,66,67,68,69,70,72,73,74,76,79,80,81,"
                + "84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99").split(","));
    }

    private static List<String> clickbenchQueries()
    {
        return List.of(("01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18,19,21,22,23,24,25,26,27,28,29,30,31,"
                + "32,33,34,35,36,37,38,39,40,41,43").split(","));
    }
}
