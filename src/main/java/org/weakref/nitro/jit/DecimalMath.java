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

import java.math.BigInteger;

/**
 * Runtime helpers for fixed-point (decimal) arithmetic on values stored as scaled {@code long}s. Generated
 * pipelines call into these for operations that cannot be a single inline Java expression. The semantics match
 * the interpreted scalar built-ins (e.g. {@code divide_scale_round_i64}) so a compiled stage and the operator
 * tree compute byte-identical decimal results.
 */
public final class DecimalMath
{
    private DecimalMath() {}

    /**
     * {@code round(numerator * scale / denominator)} with half-up rounding, computed in {@link BigInteger} to
     * avoid overflow -- the rescaling division used for decimal averages and ratios. Returns 0 when the
     * denominator is 0 (matching the built-in's guard).
     */
    public static long roundScaledDivide(long numerator, long denominator, long scale)
    {
        if (denominator == 0) {
            return 0;
        }
        boolean negative = (numerator < 0) ^ (denominator < 0) ^ (scale < 0);
        BigInteger scaledNumerator = BigInteger.valueOf(numerator).abs().multiply(BigInteger.valueOf(scale).abs());
        BigInteger positiveDenominator = BigInteger.valueOf(denominator).abs();
        BigInteger[] division = scaledNumerator.divideAndRemainder(positiveDenominator);
        BigInteger quotient = division[0];
        if (division[1].shiftLeft(1).compareTo(positiveDenominator) >= 0) {
            quotient = quotient.add(BigInteger.ONE);
        }
        long rounded = quotient.longValueExact();
        return negative ? -rounded : rounded;
    }
}
