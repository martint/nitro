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

import org.weakref.nitro.data.VectorAccess;

import static com.google.common.base.Preconditions.checkArgument;

/** Immutable, composition-owned physical policy for repeated-value expansion. */
public record UnnestOperatorPolicy(
        int maxRowsPerBatch,
        int valueRunSampleSize,
        int minimumAverageValueRunLength,
        int ordinalityDomainMaxEntries,
        int ordinalityDomainMinimumReduction)
{
    public UnnestOperatorPolicy
    {
        checkArgument(maxRowsPerBatch > 0, "maxRowsPerBatch must be positive");
        checkArgument(valueRunSampleSize > 0, "valueRunSampleSize must be positive");
        checkArgument(minimumAverageValueRunLength > 1, "minimumAverageValueRunLength must be greater than one");
        checkArgument(ordinalityDomainMaxEntries >= 0, "ordinalityDomainMaxEntries is negative");
        checkArgument(ordinalityDomainMinimumReduction > 0, "ordinalityDomainMinimumReduction must be positive");
    }

    public UnnestOperatorPolicy(int maxRowsPerBatch)
    {
        this(maxRowsPerBatch, 1_024, 4, 4_096, 8);
    }

    public UnnestOperatorPolicy(int maxRowsPerBatch, int valueRunSampleSize, int minimumAverageValueRunLength)
    {
        this(maxRowsPerBatch, valueRunSampleSize, minimumAverageValueRunLength, 4_096, 8);
    }

    public static UnnestOperatorPolicy defaults()
    {
        return new UnnestOperatorPolicy(65_536);
    }

    boolean admitsValueRuns(VectorAccess.RepeatedValues values, int positionCount)
    {
        if (!values.supportsValueRuns()) {
            return false;
        }
        int sampledPositions = Math.min(positionCount, valueRunSampleSize);
        int sampledRuns = values.valueRunCount(sampledPositions);
        return (long) sampledRuns * minimumAverageValueRunLength <= sampledPositions;
    }

    /** Standalone composition adapter; production operators receive the resulting immutable instance. */
    public static UnnestOperatorPolicy fromSystemProperties()
    {
        return new UnnestOperatorPolicy(
                Integer.getInteger("nitro.unnest.maxRowsPerBatch", 65_536),
                Integer.getInteger("nitro.unnest.valueRunSampleSize", 1_024),
                Integer.getInteger("nitro.unnest.minimumAverageValueRunLength", 4),
                Integer.getInteger("nitro.unnest.ordinalityDomainMaxEntries", 4_096),
                Integer.getInteger("nitro.unnest.ordinalityDomainMinimumReduction", 8));
    }

    boolean admitsOrdinalityDomain(int outputCount, int domainSize)
    {
        return domainSize > 0 &&
                domainSize <= ordinalityDomainMaxEntries &&
                (long) domainSize * ordinalityDomainMinimumReduction <= outputCount;
    }
}
