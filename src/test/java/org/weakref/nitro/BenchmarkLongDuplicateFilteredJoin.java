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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.HashJoinOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.TableOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.weakref.nitro.function.scalar.builtin.JoinFilterFunctions.longNotEqual;

/**
 * The duplicate single-long join plus residual inequality shape used repeatedly by TPC-H q21. Each matching probe key has
 * four build rows and the residual predicate removes the row with the same payload value. A varied match rate exercises the common
 * duplicate-match cursor and ordinary output producer without introducing query-specific semantics. Key spacing
 * distinguishes a dense direct-index domain from a wide domain requiring the hashed duplicate representation.
 */
@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 7, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkLongDuplicateFilteredJoin
{
    private static final int DISTINCT_KEYS = 100_000;
    private static final int BUILD_ROWS_PER_KEY = 4;
    private static final int PROBE_ROWS = 4_000_000;
    private static final int CHUNK = 4096;

    private EngineResources resources;
    private Allocator allocator;
    private List<TableOperator.Page> probePages;
    private List<TableOperator.Page> buildPages;

    @Param({"factory", "registry"})
    public String filterImplementation;

    @Param({"0", "10", "100"})
    public int matchingProbePercent;

    @Param({"1", "1000003"})
    public long keyStride;

    @Setup
    public void setup()
    {
        resources = EngineResources.createDefault();
        allocator = new Allocator(resources);

        long[] probeKeys = new long[PROBE_ROWS];
        long[] probePayload = new long[PROBE_ROWS];
        for (int row = 0; row < PROBE_ROWS; row++) {
            probeKeys[row] = (row % 100 < matchingProbePercent ? row % DISTINCT_KEYS : DISTINCT_KEYS + row) * keyStride;
            probePayload[row] = row % BUILD_ROWS_PER_KEY;
        }

        long[] buildKeys = new long[DISTINCT_KEYS * BUILD_ROWS_PER_KEY];
        long[] buildPayload = new long[buildKeys.length];
        for (int key = 0; key < DISTINCT_KEYS; key++) {
            for (int value = 0; value < BUILD_ROWS_PER_KEY; value++) {
                int row = key * BUILD_ROWS_PER_KEY + value;
                buildKeys[row] = key * keyStride;
                buildPayload[row] = value;
            }
        }
        probePages = pages(probeKeys, probePayload);
        buildPages = pages(buildKeys, buildPayload);

        long expected = (long) PROBE_ROWS * matchingProbePercent / 100 * (BUILD_ROWS_PER_KEY - 1);
        long actual = joinAndCount();
        if (actual != expected) {
            throw new IllegalStateException("join row count mismatch: expected=" + expected + ", actual=" + actual);
        }
    }

    @TearDown
    public void tearDown()
    {
        resources.close();
    }

    @Benchmark
    public long joinAndCount()
    {
        Operator probe = new TableOperator(2, probePages);
        Operator build = new TableOperator(2, buildPages);
        Operator join = new HashJoinOperator(
                allocator,
                probe,
                0,
                build,
                0,
                filter());

        long rows = 0;
        try (join) {
            while (join.hasNext()) {
                try (var batch = join.next()) {
                    Mask mask = batch.borrowMask();
                    rows += mask.selectedCount();
                }
            }
        }
        return rows;
    }

    private HashJoinOperator.JoinFilter filter()
    {
        if (filterImplementation.equals("factory")) {
            return longNotEqual(1, 1);
        }
        return new HashJoinOperator.JoinFilter(
                1,
                1,
                (HashJoinOperator.LongJoinFilterFunction) (outerValue, innerValue) -> outerValue != innerValue);
    }

    private static List<TableOperator.Page> pages(long[] keys, long[] payload)
    {
        List<TableOperator.Page> pages = new ArrayList<>();
        for (int offset = 0; offset < keys.length; offset += CHUNK) {
            int length = Math.min(CHUNK, keys.length - offset);
            long[] keyChunk = new long[length];
            long[] payloadChunk = new long[length];
            System.arraycopy(keys, offset, keyChunk, 0, length);
            System.arraycopy(payload, offset, payloadChunk, 0, length);
            pages.add(new TableOperator.Page(
                    length,
                    new Streams[] {
                            Streams.ofValues(new I64Vector(keyChunk)),
                            Streams.ofValues(new I64Vector(payloadChunk))},
                    Mask.all(length)));
        }
        return pages;
    }

    public static void main(String[] args)
            throws Exception
    {
        Benchmarks.benchmark(BenchmarkLongDuplicateFilteredJoin.class).run();
    }
}
