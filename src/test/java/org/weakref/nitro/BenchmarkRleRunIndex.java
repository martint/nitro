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
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.RleVector;

import java.util.concurrent.TimeUnit;

/**
 * Resolving a run-length-encoded vector's run index for an ascending (monotonic) position sequence — the shape the
 * hash-join output produces when it wraps an RLE-encoded outer column, since output rows are emitted one per match in
 * probe order. {@link RleVector#runIndex} does a per-position binary search: O(positions × log runs). {@link
 * RleVector#runIndexFromHint} advances a forward cursor: O(runs) total, amortized O(1) per position for dense access.
 *
 * <p>The two are equivalent for a few runs (binary search over a short array is cheap) and diverge as the run count
 * grows — the case a high-cardinality column carried as RLE hits. Sweeping the run count makes the architectural
 * difference visible even though the current query suites happen to use few-run RLE columns.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkRleRunIndex
{
    private static final int N = 1 << 20; // positions resolved per op

    @Param({"16", "1024", "65536", "1048576"}) // number of runs: few -> one run per position
    private int runCount;

    private RleVector rle;
    private int[] positions;

    @Setup
    public void setup()
    {
        int[] counts = new int[runCount];
        int base = N / runCount;
        int remainder = N - base * runCount;
        for (int index = 0; index < runCount; index++) {
            counts[index] = base + (index < remainder ? 1 : 0);
        }
        rle = new RleVector(counts, new I64Vector(new long[runCount]));

        // Ascending positions across the whole vector, exactly the monotonic shape the join output resolves.
        positions = new int[N];
        for (int index = 0; index < N; index++) {
            positions[index] = index;
        }
        // Prime the cached run-ends so both benchmarks measure only the lookup, not the one-time build.
        rle.runIndex(0);
    }

    @Benchmark
    public void binarySearch(Blackhole blackhole)
    {
        int sum = 0;
        for (int position : positions) {
            sum += rle.runIndex(position);
        }
        blackhole.consume(sum);
    }

    @Benchmark
    public void forwardHint(Blackhole blackhole)
    {
        int sum = 0;
        int hint = 0;
        for (int position : positions) {
            hint = rle.runIndexFromHint(position, hint);
            sum += hint;
        }
        blackhole.consume(sum);
    }
}
