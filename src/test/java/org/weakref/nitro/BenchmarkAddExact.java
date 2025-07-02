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
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.functions.AddI64Exact;
import org.weakref.nitro.operator.evaluator.functions.Or;
import org.weakref.nitro.operator.evaluator.Result;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@OperationsPerInvocation(BenchmarkAddExact.ROW_COUNT)
public class BenchmarkAddExact
{
    public static final int ROW_COUNT = 10_000;
    private static final AddI64Exact ADD = new AddI64Exact();
    private static final Or OR = new Or();

    private I64Vector aValues;
    private BooleanVector aNulls;
    private I64Vector bValues;
    private BooleanVector bNulls;

    private I64Vector resultValues;
    private BooleanVector resultNulls;
    private BooleanVector resultErrors;

    private Mask mask;

    @Setup
    public void setup()
    {
        mask = Mask.all(ROW_COUNT);

        aValues = new I64Vector(randomI64(ROW_COUNT, 0, Long.MAX_VALUE));
        aNulls = new BooleanVector(randomBoolean(ROW_COUNT, 0, Long.MAX_VALUE));
        bValues = new I64Vector(randomI64(ROW_COUNT, 0, Long.MAX_VALUE));
        bNulls = new BooleanVector(randomBoolean(ROW_COUNT, 0, Long.MAX_VALUE));

        resultValues = new I64Vector(ROW_COUNT);
        resultNulls = new BooleanVector(ROW_COUNT);
        resultErrors = new BooleanVector(ROW_COUNT);
    }

    private static boolean[] randomBoolean(int rowCount, long min, long max)
    {
        boolean[] values = new boolean[rowCount];
        for (int row = 0; row < values.length; row++) {
            values[row] = ThreadLocalRandom.current().nextBoolean();
        }
        return values;
    }

    private static long[] randomI64(int rowCount, long min, long max)
    {
        long[] values = new long[rowCount];
        for (int row = 0; row < values.length; row++) {
            values[row] = ThreadLocalRandom.current().nextLong(min, max);
        }
        return values;
    }

    @Benchmark
    public Triple benchmarkVector()
    {
        resultNulls = (BooleanVector) OR.apply(aNulls, bNulls, mask, resultNulls);
        Result result = ADD.apply(aValues, bValues, mask.andNot(resultNulls), new Result(resultValues, resultErrors));

        return new Triple(result.result(), resultNulls, result.errors());
    }

    @Benchmark
    public Triple benchmarkScalar()
    {
        for (int position : mask) {
            if (aNulls.values()[position] || bNulls.values()[position]) {
                resultNulls.values()[position] = true;
                continue;
            }
            
            long leftValue = aValues.values()[position];
            long rightValue = bValues.values()[position];

            try {
                long result = leftValue + rightValue;
                resultValues.values()[position] = result;
                resultErrors.values()[position] = ((leftValue ^ result) & (rightValue ^ result)) < 0;
            }
            catch (Exception e) {
                resultErrors.values()[position] = true;
            }
        }

        return new Triple(resultValues, resultNulls, resultErrors);
    }

    record Triple(Vector values, BooleanVector nulls, BooleanVector errors)
    {}

    static void main()
            throws Exception
    {
//        BenchmarkAddExact benchmark = new BenchmarkAddExact();
//        benchmark.setup();
//        Result result = benchmark.benchmarkVector();
//        System.out.println(result.result());
//        System.out.println(result.errors());

        
        Benchmarks.benchmark(BenchmarkAddExact.class)
                .run();
    }
}
