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
package org.weakref.nitro.operator;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.weakref.nitro.data.PrimitiveArrayPool;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class BenchmarkLongJoinTableProbe
{
    private static final int PROBE_COUNT = 4096;

    @Param({"1024", "65536", "1048576", "4194304"})
    public int capacity;

    @Param({"0", "50", "100"})
    public int matchPercent;

    @Param({"4096", "1048576"})
    public int probeWorkingSet;

    private PrimitiveArrayPool pool;
    private LongJoinHashTable table;
    private long[] keys;
    private int[] slots;
    private long[] candidateKeys;
    private long[] probeData;
    private int probeOffset;

    @Setup
    public void setup()
    {
        if (probeWorkingSet < PROBE_COUNT || probeWorkingSet % PROBE_COUNT != 0) {
            throw new IllegalArgumentException("Probe working set must contain whole batches");
        }
        pool = new PrimitiveArrayPool(PROBE_COUNT, 0);
        table = new LongJoinHashTable(pool, capacity, true, true, -1);
        int keyCount = capacity * 3 / 4 - 1;
        for (int index = 0; index < keyCount; index++) {
            long key = key(index);
            table.initialize(~table.findSlotForInsert(key), key, index);
        }
        keys = pool.borrowLongs(PROBE_COUNT);
        slots = pool.borrowInts(PROBE_COUNT);
        candidateKeys = pool.borrowLongs(PROBE_COUNT);
        probeData = pool.borrowLongs(probeWorkingSet);
        Random random = new Random(139);
        for (int index = 0; index < probeData.length; index++) {
            probeData[index] = key(index % 100 < matchPercent ? random.nextInt(keyCount) : keyCount + index);
        }
    }

    private static long key(int index)
    {
        return index * 0x9E3779B97F4A7C15L;
    }

    @TearDown
    public void tearDown()
    {
        table.release();
        pool.release(keys);
        pool.release(slots);
        pool.release(candidateKeys);
        pool.release(probeData);
    }

    private void prepareProbeBatch()
    {
        System.arraycopy(probeData, probeOffset, keys, 0, PROBE_COUNT);
        probeOffset += PROBE_COUNT;
        if (probeOffset == probeData.length) {
            probeOffset = 0;
        }
    }

    @Benchmark
    @OperationsPerInvocation(PROBE_COUNT)
    public int[] scalar()
    {
        prepareProbeBatch();
        for (int index = 0; index < PROBE_COUNT; index++) {
            int slot = table.findSlotForInsert(keys[index]);
            slots[index] = slot < 0 ? -1 : slot;
        }
        return slots;
    }

    @Benchmark
    @OperationsPerInvocation(PROBE_COUNT)
    public int[] batched()
    {
        prepareProbeBatch();
        table.findSlots(keys, PROBE_COUNT, slots, candidateKeys);
        return slots;
    }
}
