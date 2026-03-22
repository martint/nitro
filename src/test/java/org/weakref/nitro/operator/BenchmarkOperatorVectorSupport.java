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
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Fork(3)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkOperatorVectorSupport
{
    @Param({"8", "16", "32", "64", "128"})
    public int length;

    @Param({"equal", "first_byte_diff", "last_byte_diff"})
    public String scenario;

    private byte[] left;
    private byte[] right;

    @Setup
    public void setup()
    {
        left = new byte[length + 7];
        right = new byte[length + 11];
        for (int index = 0; index < length; index++) {
            byte value = (byte) (31 + (index * 13));
            left[index + 3] = value;
            right[index + 5] = value;
        }
        if (scenario.equals("first_byte_diff")) {
            right[5] ^= 1;
        }
        if (scenario.equals("last_byte_diff")) {
            right[length + 4] ^= 1;
        }
    }

    @Benchmark
    public boolean binaryEqualsLoop()
    {
        return binaryEqualsLoop(left, 3, right, 5, length);
    }

    @Benchmark
    public boolean binaryEqualsMismatch()
    {
        return Arrays.mismatch(left, 3, 3 + length, right, 5, 5 + length) == -1;
    }

    private static boolean binaryEqualsLoop(byte[] left, int leftOffset, byte[] right, int rightOffset, int length)
    {
        for (int index = 0; index < length; index++) {
            if (left[leftOffset + index] != right[rightOffset + index]) {
                return false;
            }
        }
        return true;
    }
}
