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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.AggregationOperator;
import org.weakref.nitro.operator.HashJoinOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.TableOperator;
import org.weakref.nitro.operator.TableOperator.Page;
import org.weakref.nitro.operator.aggregation.Sum;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Operator-level benchmark for the hash join's output construction when a probe column arrives RLE-encoded. The join
 * wraps each probe output column as a dictionary over its source, mapping every (ascending, probe-ordered) output
 * position to a source run index. That resolution uses a forward hint (O(runs) total) when the positions are monotonic
 * -- as they always are for probe output -- instead of a per-position binary search (O(positions x log runs)).
 *
 * <p>Toggle {@code -Dnitro.join.rleRunIndexHint=false} to force the binary search, for a same-build A/B of the two
 * paths at the operator level. The run count is swept because the two are equivalent for few runs and diverge as the
 * count grows. Terminal sum materializes the wrapped column (matches the Velox copyResults idiom).
 */
@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 6, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkJoinRleOutput
{
    private static final int PROBE_ROWS = 8_000_000;
    private static final int BUILD_ROWS = 1_000_000;
    private static final int BATCH = 32_768;

    // Runs per batch in the probe's RLE output column: few (long runs) -> many (approaching one run per row).
    @Param({"64", "1024", "8192", "32768"})
    public int runsPerBatch;

    private List<Page> probe;
    private List<Page> build;
    private Allocator allocator;

    @Setup
    public void setup()
    {
        build = new ArrayList<>();
        for (int start = 0; start < BUILD_ROWS; start += BATCH) {
            int rows = Math.min(BATCH, BUILD_ROWS - start);
            long[] keys = new long[rows];
            long[] payload = new long[rows];
            for (int index = 0; index < rows; index++) {
                keys[index] = start + index;
                payload[index] = start + index;
            }
            build.add(Page.values(rows, new Vector[] {new I64Vector(keys), new I64Vector(payload)}, Mask.all(rows)));
        }

        probe = new ArrayList<>();
        for (int start = 0; start < PROBE_ROWS; start += BATCH) {
            int rows = Math.min(BATCH, PROBE_ROWS - start);
            long[] keys = new long[rows];
            for (int index = 0; index < rows; index++) {
                keys[index] = (start + index) % BUILD_ROWS; // dense: each probe row matches one build row
            }
            int runs = Math.min(runsPerBatch, rows);
            int[] counts = new int[runs];
            long[] runValues = new long[runs];
            int baseCount = rows / runs;
            int remainder = rows - baseCount * runs;
            for (int run = 0; run < runs; run++) {
                counts[run] = baseCount + (run < remainder ? 1 : 0);
                runValues[run] = run;
            }
            RleVector rlePayload = new RleVector(counts, new I64Vector(runValues));
            probe.add(Page.values(rows, new Vector[] {new I64Vector(keys), rlePayload}, Mask.all(rows)));
        }
    }

    @Setup(Level.Invocation)
    public void setupInvocation()
    {
        allocator = new Allocator(EngineResources.createDefault());
    }

    @Benchmark
    public void joinRleOutput()
    {
        // Join on key (column 0); output = build key/payload + probe key + probe RLE payload. Sum all four columns so
        // the RLE-wrapped output column is fully materialized (its run indices resolved).
        Operator join = new HashJoinOperator(allocator, new TableOperator(2, probe), 0, new TableOperator(2, build), 0);
        List<Sum> sums = new ArrayList<>();
        for (int column = 0; column < 4; column++) {
            sums.add(new Sum(column));
        }
        try (Operator operator = new AggregationOperator(allocator, new ArrayList<>(sums), join)) {
            while (operator.hasNext()) {
                try (var batch = operator.next()) {
                    if (batch.borrowMask().none()) {
                        continue;
                    }
                    for (int column = 0; column < operator.outputCount(); column++) {
                        Vector vector = batch.output(column).borrow(Stream.VALUES);
                        if (vector.length() == Integer.MIN_VALUE) {
                            throw new AssertionError();
                        }
                    }
                }
            }
        }
    }
}
