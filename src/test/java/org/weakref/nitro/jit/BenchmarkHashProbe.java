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
package org.weakref.nitro.jit;

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
import org.openjdk.jmh.annotations.Warmup;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * The memory-level-parallelism question for the hash-mode join probe (open addressing + linear probing, the layout
 * the pipeline compiler generates: parallel {@code long[] jKey} / {@code int[] jRow} arrays, Murmur3-finalizer hash).
 * Unlike the array-mode probe, each lookup is a dependent <em>chain</em> -- load {@code jRow[slot]}, compare
 * {@code jKey[slot]}, on a collision step to the next slot and load again -- so the question is whether keeping many
 * chains in flight beats the naive serial loop.
 * <ul>
 *   <li>{@link #scalar} -- the generated per-key probe: one chain at a time, the CPU's out-of-order window the only
 *       source of overlap;</li>
 *   <li>{@link #pipelined} -- {@value #GROUP} probes advanced round-robin (software-pipelined), making the
 *       independent chains explicit so the load buffer sees {@value #GROUP} independent misses at once.</li>
 * </ul>
 * If the pipelined form does not beat scalar, the out-of-order engine already extracts the available MLP and an
 * explicit SIMD/gather probe would not help either (the same conclusion the array-mode benchmark reached). Probe keys
 * are mostly hits ({@code hitRate}) to mirror the real laggards, where fact->dimension joins match ~100%.
 */
@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 8, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkHashProbe
{
    private static final int GROUP = 16;

    /** Build-side row count (unique keys). cap = next power of two > rows / 0.75; cap*12 bytes is the table footprint. */
    @Param({"1048576", "4194304", "16777216"})
    public int rows;

    /** Fraction of probes whose key is present in the build table (a hit). */
    @Param("0.95")
    public double hitRate;

    private int probeRows;
    private long[] jKey;
    private int[] jRow;
    private int mask;
    private long[] probeKeys;
    private int[] out;

    @Setup
    public void setup()
    {
        int capacity = 16;
        while (capacity * 0.75f < rows) {
            capacity <<= 1;
        }
        mask = capacity - 1;
        jKey = new long[capacity];
        jRow = new int[capacity];
        java.util.Arrays.fill(jRow, -1);
        // Build keys 0..rows-1 (distinct; the hash scatters them across the table).
        for (int r = 0; r < rows; r++) {
            long key = r;
            int slot = mix(key) & mask;
            while (jRow[slot] != -1) {
                slot = (slot + 1) & mask;
            }
            jKey[slot] = key;
            jRow[slot] = r;
        }

        Random random = new Random(42);
        probeRows = 1 << 20;
        probeKeys = new long[probeRows];
        for (int i = 0; i < probeRows; i++) {
            probeKeys[i] = random.nextDouble() < hitRate
                    ? random.nextInt(rows)          // present
                    : (long) rows + random.nextInt(rows);   // absent
        }
        out = new int[probeRows];
    }

    private static int mix(long key)
    {
        long h = key;
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        return (int) h;
    }

    @Benchmark
    public int scalar()
    {
        scalarProbe(jKey, jRow, mask, probeKeys, probeRows, out);
        return checksum(out, probeRows);
    }

    @Benchmark
    public int pipelined()
    {
        pipelinedProbe(jKey, jRow, mask, probeKeys, probeRows, out);
        return checksum(out, probeRows);
    }

    /** Naive serial open-addressing probe: one dependent chain at a time. */
    static void scalarProbe(long[] jKey, int[] jRow, int mask, long[] keys, int count, int[] out)
    {
        for (int i = 0; i < count; i++) {
            long key = keys[i];
            int slot = mix(key) & mask;
            int row = -1;
            while (jRow[slot] != -1) {
                if (jKey[slot] == key) {
                    row = jRow[slot];
                    break;
                }
                slot = (slot + 1) & mask;
            }
            out[i] = row;
        }
    }

    /** Software-pipelined probe: GROUP chains advanced round-robin so the load buffer sees many independent misses. */
    static void pipelinedProbe(long[] jKey, int[] jRow, int mask, long[] keys, int count, int[] out)
    {
        int[] cursorIndex = new int[GROUP];
        long[] cursorKey = new long[GROUP];
        int[] cursorSlot = new int[GROUP];
        int next = 0;
        int active = 0;
        while (active < GROUP && next < count) {
            cursorIndex[active] = next;
            cursorKey[active] = keys[next];
            cursorSlot[active] = mix(keys[next]) & mask;
            active++;
            next++;
        }
        while (active > 0) {
            int c = 0;
            while (c < active) {
                int slot = cursorSlot[c];
                int row = jRow[slot];
                boolean retire;
                if (row == -1) {
                    out[cursorIndex[c]] = -1;
                    retire = true;
                }
                else if (jKey[slot] == cursorKey[c]) {
                    out[cursorIndex[c]] = row;
                    retire = true;
                }
                else {
                    cursorSlot[c] = (slot + 1) & mask;
                    retire = false;
                }
                if (!retire) {
                    c++;
                }
                else if (next < count) {
                    cursorIndex[c] = next;
                    cursorKey[c] = keys[next];
                    cursorSlot[c] = mix(keys[next]) & mask;
                    next++;
                    c++;
                }
                else {
                    active--;
                    cursorIndex[c] = cursorIndex[active];
                    cursorKey[c] = cursorKey[active];
                    cursorSlot[c] = cursorSlot[active];
                }
            }
        }
    }

    private static int checksum(int[] out, int count)
    {
        int sum = 0;
        for (int i = 0; i < count; i++) {
            if (out[i] != -1) {
                sum += out[i];
            }
        }
        return sum;
    }
}
