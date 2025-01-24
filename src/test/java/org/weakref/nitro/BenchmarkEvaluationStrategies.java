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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@OperationsPerInvocation(BenchmarkEvaluationStrategies.ROW_COUNT)
public class BenchmarkEvaluationStrategies
{
    public static final int ROW_COUNT = 10_000;

    private long[] a;
    private long[] b;
    private long[] c;
    private long[] d;
    private long[] temp;

    @Setup
    public void setup()
    {
        a = randomColumn(ROW_COUNT, 0, Long.MAX_VALUE);
        b = randomColumn(ROW_COUNT, 0, Long.MAX_VALUE);
        c = randomColumn(ROW_COUNT, 0, Long.MAX_VALUE);
        d = randomColumn(ROW_COUNT, 0, Long.MAX_VALUE);
        temp = new long[ROW_COUNT];
    }

    private static long[] randomColumn(int rowCount, long min, long max)
    {
        long[] values = new long[rowCount];
        for (int row = 0; row < values.length; row++) {
            values[row] = ThreadLocalRandom.current().nextLong(min, max);
        }
        return values;
    }

    @Benchmark
    public long[] pairwiseShortStride()
    {
        int stride = 8;
        for (int row = 0; row < ROW_COUNT; row += stride) {
            for (int i = 0; i < stride; i++) {
                temp[row + i] = a[row + i] * b[row + i];
            }
            for (int i = 0; i < stride; i++) {
                temp[row + i] = temp[row + i] + c[row + i];
            }
            for (int i = 0; i < stride; i++) {
                temp[row + i] = temp[row + i] * d[row + i];
            }
        }

        return temp;
    }

    @Benchmark
    public long[] pairwise()
    {
        for (int i = 0; i < ROW_COUNT; i++) {
            temp[i] = a[i] * b[i];
        }
        for (int i = 0; i < ROW_COUNT; i++) {
            temp[i] = temp[i] + c[i];
        }
        for (int i = 0; i < ROW_COUNT; i++) {
            temp[i] = temp[i] * d[i];
        }

        return temp;
    }

    @Benchmark
    public long[] allAtOnce()
    {
        for (int i = 0; i < ROW_COUNT; i++) {
            temp[i] = (a[i] * b[i] + c[i]) * d[i];
        }

        return temp;
    }

    public static void main(String[] args)
            throws Exception
    {
        Benchmarks.benchmark(BenchmarkEvaluationStrategies.class)
                .run();
    }
}
