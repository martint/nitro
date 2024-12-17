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
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@OperationsPerInvocation(BenchmarkAggregationStrategies.ROW_COUNT)
public class BenchmarkAggregationStrategies
{
    public static final int ROW_COUNT = 10_000;

    @Param({"1", "100", "1000", "10000", "100000"})
    private int groupCount = 100_000;

    private int[] groupIds;
    private long[] inputs;

    @Param({
            "1",   /*"3",*/  "5",  /*"7",*/  /*"9",*/ "10",
            /*"11", "13", */"15", /*"17", "19",*/ "20",
            /*"21", "23",*/ "25", /*"27", "29",*/ "30",
            /*"21", "23",*/ "35", /*"27", "29",*/ "40",
            /*"21", "23",*/ "45", /*"27", "29",*/ "50",
    })
    private int stateCount = 1;

    private long[][] columnarStates;
    private long[][] rowOrientedStates;
    private long[] flatStates;

    @Setup
    public void setup()
    {
        groupIds = randomIntColumn(ROW_COUNT, 0, groupCount);
        inputs = randomColumn(ROW_COUNT, 0, Long.MAX_VALUE);

        columnarStates = new long[stateCount][groupCount];
        rowOrientedStates = new long[groupCount][stateCount];
        flatStates = new long[groupCount * stateCount];
    }

    private static int[] randomIntColumn(int rowCount, int min, int max)
    {
        int[] values = new int[rowCount];
        for (int row = 0; row < values.length; row++) {
            values[row] = ThreadLocalRandom.current().nextInt(min, max);
        }
        return values;
    }

    private static long[] randomColumn(int rowCount, long min, long max)
    {
        long[] values = new long[rowCount];
        for (int row = 0; row < values.length; row++) {
            values[row] = ThreadLocalRandom.current().nextLong(min, max);
        }
        return values;
    }

    //    @Benchmark
    public long[][] columnar()
    {
        for (int i = 0; i < stateCount; i++) {
            long[] state = columnarStates[i];
            for (int row = 0; row < inputs.length; row++) {
                int group = groupIds[row];
                long value = inputs[row];
                state[group] += value;
            }
        }

        return columnarStates;
    }

    //    @Benchmark
    public long[][] rowOriented()
    {
        for (int row = 0; row < inputs.length; row++) {
            int group = groupIds[row];
            long[] state = rowOrientedStates[group];
            for (int i = 0; i < state.length; i++) {
                state[i] += inputs[row];
            }
        }

        return rowOrientedStates;
    }

    @Benchmark
    public long[] columnarFlat()
    {
        for (int state = 0; state < stateCount; state++) {
            int base = state * groupCount;
            for (int row = 0; row < inputs.length; row++) {
                flatStates[base + groupIds[row]] += inputs[row];
            }
        }

        return flatStates;
    }

//    @Benchmark
//    public long[] columnarFlat2()
//    {
//        for (int row = 0; row < inputs.length; row++) {
//            for (int state = 0; state < stateCount; state++) {
//                int base = state * groupCount;
//                flatStates[base + (int) groupIds[row]] += inputs[row];
//            }
//        }
//
//        return flatStates;
//    }

    @Benchmark
    public long[] rowOrientedFlat()
    {
        for (int row = 0; row < inputs.length; row++) {
            int base = groupIds[row] * stateCount;
            for (int state = 0; state < stateCount; state++) {
                flatStates[base + state] += inputs[row];
            }
        }

        return flatStates;
    }

//    @Benchmark
//    public long[] rowOrientedFlat2()
//    {
//        for (int state = 0; state < stateCount; state++) {
//            for (int row = 0; row < inputs.length; row++) {
//                int base = (int) (groupIds[row] * stateCount);
//                flatStates[base + state] += inputs[row];
//            }
//        }
//
//        return flatStates;
//    }

    public static void main(String[] args)
            throws Exception
    {
        Benchmarks.benchmark(BenchmarkAggregationStrategies.class)
                .run();
    }
}
