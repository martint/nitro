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
import org.openjdk.jmh.infra.Blackhole;

import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

/**
 * Isolates the dominant parquet-decode primitive (the dictionary gather {@code out[i] = dict[ids[i]]}, which the
 * scan profile shows as ~16% of a scan-bound query via LongColumnAdapter.decodeDictionaryIds + copyValue) and
 * compares it to the sequential-copy memory-bandwidth ceiling, to determine whether Nitro's decode is at the
 * hardware limit (fundamental) or has headroom (a code issue). Throughput is in values/op; divide by the time to
 * get values/s, ×8 bytes for the output stream's GB/s.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkDecodePrimitives
{
    private static final int N = 1 << 20; // 1,048,576 values per op (a few parquet batches)

    @Param({"1024", "16384", "131072"}) // dict bytes: 8KB (L1), 128KB (L2), 1MB (L3) — cache-residency sweep
    private int dictionarySize;

    private long[] dictionary;
    private int[] ids;
    private long[] out;
    private long[] src;

    @Setup(Level.Trial)
    public void setup()
    {
        SplittableRandom random = new SplittableRandom(42);
        dictionary = new long[dictionarySize];
        for (int i = 0; i < dictionarySize; i++) {
            dictionary[i] = random.nextLong();
        }
        ids = new int[N];
        for (int i = 0; i < N; i++) {
            ids[i] = random.nextInt(dictionarySize); // realistic: ids span the dictionary, repeated
        }
        out = new long[N];
        src = new long[N];
        for (int i = 0; i < N; i++) {
            src[i] = random.nextLong();
        }
    }

    /** The actual decode-time gather: random read into the dictionary per row. */
    @Benchmark
    public void dictionaryGather(Blackhole bh)
    {
        long[] d = dictionary;
        int[] id = ids;
        long[] o = out;
        for (int i = 0; i < N; i++) {
            o[i] = d[id[i]];
        }
        bh.consume(o);
    }

    /** Sequential copy: the memory-bandwidth ceiling for producing the same N longs. */
    @Benchmark
    public void sequentialCopy(Blackhole bh)
    {
        System.arraycopy(src, 0, out, 0, N);
        bh.consume(out);
    }

    /** Sequential read+write (no arraycopy intrinsic) — bandwidth ceiling for an explicit loop. */
    @Benchmark
    public void sequentialLoop(Blackhole bh)
    {
        long[] s = src;
        long[] o = out;
        for (int i = 0; i < N; i++) {
            o[i] = s[i];
        }
        bh.consume(o);
    }
}
