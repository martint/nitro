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
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.HashJoinOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.TableOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The duplicate composite-key shape from TPC-DS q72's inventory join. The logical join key omits warehouse,
 * leaving several build rows for every {@code (item, date)} pair. The probe cardinality is intentionally much
 * larger than the build, and the benchmark consumes a build payload used by q72's residual predicate.
 */
@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 4, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkLongPairDuplicateJoin
{
    private static final int PROBE_ROWS = 4_000_000;
    private static final int DISTINCT_KEYS = 100_000;
    private static final int BUILD_ROWS_PER_KEY = 5;
    private static final int BUILD_ROWS = DISTINCT_KEYS * BUILD_ROWS_PER_KEY;
    private static final int CHUNK = 4096;

    @Param({"false", "true"})
    private boolean compactChains;

    private EngineResources resources;
    private Allocator allocator;
    private List<TableOperator.Page> probePages;
    private List<TableOperator.Page> buildPages;

    @Setup
    public void setup()
    {
        System.setProperty("nitro.join.compactChains", Boolean.toString(compactChains));
        resources = EngineResources.createDefault();
        allocator = new Allocator(resources);

        long[] probeItem = new long[PROBE_ROWS];
        long[] probeDate = new long[PROBE_ROWS];
        long[] probeQuantity = new long[PROBE_ROWS];
        for (int row = 0; row < PROBE_ROWS; row++) {
            int key = row % DISTINCT_KEYS;
            probeItem[row] = key / 100;
            probeDate[row] = key % 100;
            probeQuantity[row] = row % 100;
        }

        long[] buildItem = new long[BUILD_ROWS];
        long[] buildDate = new long[BUILD_ROWS];
        long[] inventoryQuantity = new long[BUILD_ROWS];
        for (int key = 0; key < DISTINCT_KEYS; key++) {
            for (int warehouse = 0; warehouse < BUILD_ROWS_PER_KEY; warehouse++) {
                int row = key * BUILD_ROWS_PER_KEY + warehouse;
                buildItem[row] = key / 100;
                buildDate[row] = key % 100;
                inventoryQuantity[row] = 20 + warehouse * 10L;
            }
        }
        probePages = pages(probeItem, probeDate, probeQuantity);
        buildPages = pages(buildItem, buildDate, inventoryQuantity);

        long expectedChecksumPerHundredProbeRows = 0;
        for (int quantity = 0; quantity < 100; quantity++) {
            for (int warehouse = 0; warehouse < BUILD_ROWS_PER_KEY; warehouse++) {
                long inventory = 20 + warehouse * 10L;
                if (quantity < inventory) {
                    expectedChecksumPerHundredProbeRows += inventory;
                }
            }
        }
        long expectedChecksum = expectedChecksumPerHundredProbeRows * (PROBE_ROWS / 100L);
        long actualChecksum = joinAndEvaluateResidual();
        if (actualChecksum != expectedChecksum) {
            throw new IllegalStateException("join checksum mismatch: expected=" + expectedChecksum + ", actual=" + actualChecksum);
        }
    }

    @TearDown
    public void tearDown()
    {
        resources.close();
        System.clearProperty("nitro.join.compactChains");
    }

    @Benchmark
    public long joinAndEvaluateResidual()
    {
        Operator probe = new TableOperator(3, probePages);
        Operator build = new TableOperator(3, buildPages);
        Operator join = new HashJoinOperator(allocator, probe, new int[] {0, 1}, build, new int[] {0, 1});

        long checksum = 0;
        try (join) {
            while (join.hasNext()) {
                try (var batch = join.next()) {
                    Mask mask = batch.borrowMask();
                    VectorAccess.LongValues probeQuantity = VectorAccess.longValues(batch.output(2).borrow(Stream.VALUES));
                    VectorAccess.LongValues inventoryQuantity = VectorAccess.longValues(batch.output(5).borrow(Stream.VALUES));
                    for (int index = 0; index < mask.selectedCount(); index++) {
                        int position = mask.position(index);
                        if (probeQuantity.value(position) < inventoryQuantity.value(position)) {
                            checksum += inventoryQuantity.value(position);
                        }
                    }
                }
            }
        }
        return checksum;
    }

    private static List<TableOperator.Page> pages(long[] first, long[] second, long[] payload)
    {
        List<TableOperator.Page> pages = new ArrayList<>();
        for (int offset = 0; offset < first.length; offset += CHUNK) {
            int length = Math.min(CHUNK, first.length - offset);
            long[] firstChunk = new long[length];
            long[] secondChunk = new long[length];
            long[] payloadChunk = new long[length];
            System.arraycopy(first, offset, firstChunk, 0, length);
            System.arraycopy(second, offset, secondChunk, 0, length);
            System.arraycopy(payload, offset, payloadChunk, 0, length);
            pages.add(new TableOperator.Page(
                    length,
                    new Streams[] {
                            Streams.ofValues(new I64Vector(firstChunk)),
                            Streams.ofValues(new I64Vector(secondChunk)),
                            Streams.ofValues(new I64Vector(payloadChunk))},
                    Mask.all(length)));
        }
        return pages;
    }

    public static void main(String[] args)
            throws Exception
    {
        Benchmarks.benchmark(BenchmarkLongPairDuplicateJoin.class).run();
    }
}
