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
package org.weakref.nitro.legacy.pipeline;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
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
 * What actually costs in the array-mode join probe -- the dependent random load, or the unpredictable branch that
 * consumes its result? The build side is a direct-indexed array ({@code buildRowByKey[key - min]} holds the build
 * row, or {@code -1}); each probe is one random load followed by a "did it match?" decision. Three variants separate
 * the two effects, all summing matched build rows so the work is identical:
 * <ul>
 *   <li>{@link #scalarBranchy} -- scalar load + a data-dependent {@code if (row != -1)} (a coin-flip branch at 50%
 *       density);</li>
 *   <li>{@link #scalarBranchless} -- scalar load + branchless masked add (no branch);</li>
 *   <li>{@link #gather} -- {@link IntVector} gather a lane-width at a time + branchless masked reduce.</li>
 * </ul>
 * If branchless-scalar already matches the gather, the lever is the branch, not memory-level parallelism: the CPU's
 * load buffer already overlaps the random misses, and the hardware gather adds nothing over scalar out-of-order
 * loads. The {@code range} parameter sweeps from an L2/L3-resident table to one far larger than the last-level cache.
 */
@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 8, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkJoinProbe
{
    private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;
    private static final long MIN_KEY = 1000;

    /** Build-table key cardinality (size of the direct-indexed array). 1<<20 ~ 4 MB, 1<<24 ~ 64 MB, 1<<26 ~ 256 MB. */
    @Param({"1048576", "16777216", "67108864"})
    public int range;

    /** Fraction of build slots that hold a real row (the rest are -1, i.e. probe misses). */
    @Param("0.5")
    public double density;

    private int probeRows;
    private int[] buildRowByKey;
    private long[] probeKeys;
    private int[] slot;   // scratch for key -> slot, reused so the steady state is allocation-free

    @Setup
    public void setup()
    {
        Random random = new Random(42);
        buildRowByKey = new int[range];
        for (int r = 0; r < range; r++) {
            buildRowByKey[r] = random.nextDouble() < density ? r : -1;
        }
        probeRows = 1 << 20;
        probeKeys = new long[probeRows];
        for (int i = 0; i < probeRows; i++) {
            probeKeys[i] = MIN_KEY + random.nextInt(range);   // all in range; misses come from -1 slots
        }
        slot = new int[probeRows];
    }

    @Benchmark
    public long scalarBranchy()
    {
        long[] keys = probeKeys;
        int[] table = buildRowByKey;
        long sum = 0;
        for (int i = 0; i < probeRows; i++) {
            int row = table[(int) (keys[i] - MIN_KEY)];
            if (row != -1) {
                sum += row;
            }
        }
        return sum;
    }

    @Benchmark
    public long scalarBranchless()
    {
        long[] keys = probeKeys;
        int[] table = buildRowByKey;
        long sum = 0;
        for (int i = 0; i < probeRows; i++) {
            int row = table[(int) (keys[i] - MIN_KEY)];
            // row is -1 (all bits set) for a miss, else >= 0. (row >> 31) is -1 for a miss and 0 for a hit;
            // ~(row >> 31) masks the value to 0 on a miss without a branch.
            sum += row & ~(row >> 31);
        }
        return sum;
    }

    @Benchmark
    public long gather()
    {
        long[] keys = probeKeys;
        int[] table = buildRowByKey;
        int[] slots = slot;
        for (int i = 0; i < probeRows; i++) {
            slots[i] = (int) (keys[i] - MIN_KEY);
        }
        long sum = 0;
        int upper = SPECIES.loopBound(probeRows);
        int i = 0;
        for (; i < upper; i += SPECIES.length()) {
            IntVector rows = IntVector.fromArray(SPECIES, table, 0, slots, i);
            VectorMask<Integer> matched = rows.compare(VectorOperators.NE, -1);
            sum += rows.reduceLanes(VectorOperators.ADD, matched);
        }
        for (; i < probeRows; i++) {
            int row = table[slots[i]];
            sum += row & ~(row >> 31);
        }
        return sum;
    }
}
