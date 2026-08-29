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

import jdk.incubator.vector.LongVector;

/// Engine-selected physical policy for the pooled scalar-long distinct hash table.
public record PooledLongHashSetPolicy(
        float loadFactor,
        boolean vectorTags,
        boolean vectorKeys,
        int minimumVectorTagNewKeyPercent,
        int maximumVectorKeyNewKeyPercent,
        int tagGroupBits,
        int keyGroupBits,
        boolean debug)
{
    public PooledLongHashSetPolicy
    {
        if (!(loadFactor > 0 && loadFactor < 1)) {
            throw new IllegalArgumentException("loadFactor must be between 0 and 1: " + loadFactor);
        }
        if (minimumVectorTagNewKeyPercent < 0 || minimumVectorTagNewKeyPercent > 100) {
            throw new IllegalArgumentException("minimumVectorTagNewKeyPercent must be between 0 and 100");
        }
        if (maximumVectorKeyNewKeyPercent < 0 || maximumVectorKeyNewKeyPercent > 100) {
            throw new IllegalArgumentException("maximumVectorKeyNewKeyPercent must be between 0 and 100");
        }
        if (tagGroupBits != 64 && tagGroupBits != 128 && tagGroupBits != 256 && tagGroupBits != 512) {
            throw new IllegalArgumentException("tagGroupBits must be 64, 128, 256, or 512");
        }
        if (keyGroupBits != 64 && keyGroupBits != 128 && keyGroupBits != 256 && keyGroupBits != 512) {
            throw new IllegalArgumentException("keyGroupBits must be 64, 128, 256, or 512");
        }
    }

    public static PooledLongHashSetPolicy defaults()
    {
        return new PooledLongHashSetPolicy(0.75f, false, supportsVectorKeyGroups(), 5, 50, 128, preferredKeyGroupBits(), false);
    }

    public static PooledLongHashSetPolicy fromSystemProperties()
    {
        return new PooledLongHashSetPolicy(
                Float.parseFloat(System.getProperty("nitro.distinct.scalarLongLoadFactor", "0.75")),
                Boolean.parseBoolean(System.getProperty("nitro.distinct.scalarLongVectorTags", "false")),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.distinct.scalarLongVectorKeys",
                        Boolean.toString(supportsVectorKeyGroups()))),
                Integer.getInteger("nitro.distinct.scalarLongVectorTagMinNewKeyPercent", 5),
                Integer.getInteger("nitro.distinct.scalarLongVectorKeyMaxNewKeyPercent", 50),
                Integer.getInteger("nitro.distinct.scalarLongTagGroupBits", 128),
                Integer.getInteger("nitro.distinct.scalarLongKeyGroupBits", preferredKeyGroupBits()),
                Boolean.getBoolean("nitro.debug.scalarLongDistinct"));
    }

    private static int preferredKeyGroupBits()
    {
        return Math.clamp(LongVector.SPECIES_PREFERRED.vectorBitSize(), 64, 512);
    }

    private static boolean supportsVectorKeyGroups()
    {
        // End-to-end high-cardinality grouping shows that four-lane AVX2 probes repay their setup even where an
        // isolated probe microbenchmark does not: fewer probe iterations also reduce surrounding grouping and
        // output pressure. Keep scalar keys only on targets narrower than four long lanes.
        return LongVector.SPECIES_PREFERRED.vectorBitSize() >= 256;
    }
}
