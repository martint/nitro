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
import org.openjdk.jmh.annotations.Warmup;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/// Repeated, non-consecutive values at distinct flat positions, with and without a reusable input hash.
@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 1)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkValueIdInterner
{
    private static final int POSITIONS = 16_384;
    private static final int VALUE_BYTES = 32;

    @Param({"16", "1024", "65536"})
    public int distinctValues;

    private ValueIdInterner interner;
    private byte[] input;
    private int[] hashes;

    @Setup
    public void setup()
    {
        Random random = new Random(42);
        byte[] dictionary = new byte[distinctValues * VALUE_BYTES];
        random.nextBytes(dictionary);
        interner = new ValueIdInterner(distinctValues, FlatKeyTablePolicy.ValueIds.defaults());
        for (int position = 0; position < distinctValues; position++) {
            interner.intern(dictionary, position * VALUE_BYTES, VALUE_BYTES);
        }
        input = new byte[POSITIONS * VALUE_BYTES];
        hashes = new int[POSITIONS];
        for (int position = 0; position < POSITIONS; position++) {
            System.arraycopy(dictionary, random.nextInt(distinctValues) * VALUE_BYTES,
                    input, position * VALUE_BYTES, VALUE_BYTES);
            hashes[position] = OperatorVectorSupport.binaryHash(input, position * VALUE_BYTES, VALUE_BYTES);
        }
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public long lookup()
    {
        long result = 0;
        for (int position = 0; position < POSITIONS; position++) {
            result += interner.intern(input, position * VALUE_BYTES, VALUE_BYTES);
        }
        return result;
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public long lookupPrehashed()
    {
        long result = 0;
        for (int position = 0; position < POSITIONS; position++) {
            result += interner.intern(input, position * VALUE_BYTES, VALUE_BYTES, hashes[position]);
        }
        return result;
    }
}
