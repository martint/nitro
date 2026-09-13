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
 * A unique two-long-key join shaped like TPC-DS q85's web sales/returns join after dynamic filtering.
 */
@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 8, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkLongPairUniqueJoin
{
    private static final int BUILD_ROWS = 35_000;
    private static final int PROBE_ROWS = 52_000;
    private static final int CHUNK = 4096;

    private EngineResources resources;
    private List<TableOperator.Page> probePages;
    private List<TableOperator.Page> buildPages;
    private long expectedChecksum;

    @Setup
    public void setup()
    {
        resources = EngineResources.createDefault();

        long[] buildItem = new long[BUILD_ROWS];
        long[] buildOrder = new long[BUILD_ROWS];
        long[] buildPayload = new long[BUILD_ROWS];
        for (int row = 0; row < BUILD_ROWS; row++) {
            buildItem[row] = row % 18_000;
            buildOrder[row] = 10_000_000L + row;
            buildPayload[row] = row + 1L;
        }

        long[] probeItem = new long[PROBE_ROWS];
        long[] probeOrder = new long[PROBE_ROWS];
        long[] probePayload = new long[PROBE_ROWS];
        for (int row = 0; row < PROBE_ROWS; row++) {
            int buildRow = row % BUILD_ROWS;
            boolean match = (row & 1) == 0;
            probeItem[row] = buildItem[buildRow];
            probeOrder[row] = match ? buildOrder[buildRow] : 20_000_000L + row;
            probePayload[row] = row + 3L;
            if (match) {
                expectedChecksum += probePayload[row] + buildPayload[buildRow];
            }
        }
        probePages = pages(probeItem, probeOrder, probePayload);
        buildPages = pages(buildItem, buildOrder, buildPayload);

        long actualChecksum = join();
        if (actualChecksum != expectedChecksum) {
            throw new IllegalStateException("join checksum mismatch: expected=" + expectedChecksum + ", actual=" + actualChecksum);
        }
    }

    @TearDown
    public void tearDown()
    {
        resources.close();
    }

    @Benchmark
    public long join()
    {
        Operator probe = new TableOperator(3, probePages);
        Operator build = new TableOperator(3, buildPages);
        long checksum = 0;
        try (Allocator allocator = new Allocator(resources);
                Operator join = new HashJoinOperator(allocator, probe, new int[] {0, 1}, build, new int[] {0, 1})) {
            while (join.hasNext()) {
                try (var batch = join.next()) {
                    Mask mask = batch.borrowMask();
                    VectorAccess.LongValues probePayload = VectorAccess.longValues(batch.output(2).borrow(Stream.VALUES));
                    VectorAccess.LongValues buildPayload = VectorAccess.longValues(batch.output(5).borrow(Stream.VALUES));
                    for (int index = 0; index < mask.selectedCount(); index++) {
                        int position = mask.position(index);
                        checksum += probePayload.value(position) + buildPayload.value(position);
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
        Benchmarks.benchmark(BenchmarkLongPairUniqueJoin.class).run();
    }
}
