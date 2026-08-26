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
package org.weakref.nitro.operator.aggregation;

import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

/** Exact, allocation-free admission and reduction for frequency-bearing double dictionaries. */
public final class ExactDoubleDomainSum
{
    private static final double MAX_EXACT_INTEGER = 1L << 53;

    private ExactDoubleDomainSum() {}

    /** Returns NaN when domain reduction cannot preserve Java's row-order sum exactly. */
    public static double sumOrNaN(DictionaryVector dictionary, Vector nulls, double initialValue)
    {
        if (!(dictionary.values() instanceof F64Vector dictionaryValues) ||
                !dictionary.hasDomainFrequencies() ||
                !Double.isFinite(initialValue)) {
            return Double.NaN;
        }
        if (!supportsDictionaryNulls(dictionary, nulls)) {
            return Double.NaN;
        }
        boolean[] dictionaryNulls = dictionaryNulls(nulls);

        double[] values = dictionaryValues.values();
        int commonExponent = initialValue == 0 ? Integer.MAX_VALUE : unitExponent(initialValue);
        if (commonExponent == Integer.MIN_VALUE) {
            return Double.NaN;
        }
        for (int dictionaryId = 0; dictionaryId < values.length; dictionaryId++) {
            if ((dictionaryNulls != null && dictionaryNulls[dictionaryId]) || dictionary.domainFrequency(dictionaryId) == 0) {
                continue;
            }
            int exponent = unitExponent(values[dictionaryId]);
            if (exponent == Integer.MIN_VALUE) {
                return Double.NaN;
            }
            if (values[dictionaryId] != 0) {
                commonExponent = Math.min(commonExponent, exponent);
            }
        }
        if (commonExponent == Integer.MAX_VALUE) {
            return initialValue;
        }

        double scaledInitial = Math.scalb(initialValue, -commonExponent);
        if (!exactInteger(scaledInitial)) {
            return Double.NaN;
        }
        double absoluteBound = Math.abs(scaledInitial);
        for (int dictionaryId = 0; dictionaryId < values.length; dictionaryId++) {
            if (dictionaryNulls != null && dictionaryNulls[dictionaryId]) {
                continue;
            }
            int frequency = dictionary.domainFrequency(dictionaryId);
            double scaled = Math.scalb(values[dictionaryId], -commonExponent);
            if (!exactInteger(scaled)) {
                return Double.NaN;
            }
            double contribution = Math.abs(scaled) * frequency;
            if (contribution > MAX_EXACT_INTEGER - absoluteBound) {
                return Double.NaN;
            }
            absoluteBound += contribution;
        }

        long scaledSum = (long) scaledInitial;
        for (int dictionaryId = 0; dictionaryId < values.length; dictionaryId++) {
            if (dictionaryNulls != null && dictionaryNulls[dictionaryId]) {
                continue;
            }
            long scaled = (long) Math.scalb(values[dictionaryId], -commonExponent);
            scaledSum += scaled * dictionary.domainFrequency(dictionaryId);
        }
        return Math.scalb((double) scaledSum, commonExponent);
    }

    public static long nonNullCount(DictionaryVector dictionary, Vector nulls)
    {
        if (!supportsDictionaryNulls(dictionary, nulls)) {
            return -1;
        }
        boolean[] dictionaryNulls = dictionaryNulls(nulls);
        long count = 0;
        for (int dictionaryId = 0; dictionaryId < dictionary.values().length(); dictionaryId++) {
            if (dictionaryNulls == null || !dictionaryNulls[dictionaryId]) {
                count += dictionary.domainFrequency(dictionaryId);
            }
        }
        return count;
    }

    private static boolean supportsDictionaryNulls(DictionaryVector dictionary, Vector nulls)
    {
        if (VectorAccess.isAllFalseNulls(nulls)) {
            return true;
        }
        return nulls instanceof DictionaryVector nullDictionary &&
                dictionary.hasSameRowMapping(nullDictionary) &&
                nullDictionary.values() instanceof BooleanVector values &&
                values.length() == dictionary.values().length();
    }

    private static boolean[] dictionaryNulls(Vector nulls)
    {
        return nulls instanceof DictionaryVector dictionary ? ((BooleanVector) dictionary.values()).values() : null;
    }

    private static boolean exactInteger(double value)
    {
        return Double.isFinite(value) && Math.abs(value) <= MAX_EXACT_INTEGER && value == Math.rint(value);
    }

    private static int unitExponent(double value)
    {
        if (!Double.isFinite(value)) {
            return Integer.MIN_VALUE;
        }
        long bits = Double.doubleToRawLongBits(value) & Long.MAX_VALUE;
        long significand = bits & ((1L << 52) - 1);
        int encodedExponent = (int) (bits >>> 52);
        int baseExponent;
        if (encodedExponent == 0) {
            if (significand == 0) {
                return Integer.MAX_VALUE;
            }
            baseExponent = -1074;
        }
        else {
            significand |= 1L << 52;
            baseExponent = encodedExponent - 1023 - 52;
        }
        return baseExponent + Long.numberOfTrailingZeros(significand);
    }
}
