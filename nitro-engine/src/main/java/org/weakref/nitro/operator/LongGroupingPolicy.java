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

/**
 * Engine-selected representation and admission policy for single-long grouping.
 */
public record LongGroupingPolicy(
        boolean runCache,
        boolean idIndexed,
        int idIndexedMinGroups,
        int idIndexedMaxGroups,
        boolean idIndexedDenseRehash,
        int idIndexedActivationCapacityMultiplier,
        boolean direct,
        int directMinGroups,
        int directMaxRange,
        int directLatestAdmissionGroups,
        int stagedDirectMaxRange,
        int stagedDirectLatestAdmissionGroups,
        boolean stagedCompressedDirect,
        int stagedCompressedDirectMinGroups,
        int stagedCompressedDirectMaxRange,
        int stagedCompressedDirectLatestAdmissionGroups,
        int directMaxRangePerGroupNumerator,
        int directMaxRangePerGroupDenominator,
        boolean debugDirect)
{
    private static final int MAX_ID_INDEXED_GROUPS = 0x03FF_FFFF;

    public LongGroupingPolicy
    {
        if (idIndexedMinGroups < 0 || idIndexedMaxGroups <= 0 || idIndexedMaxGroups > MAX_ID_INDEXED_GROUPS) {
            throw new IllegalArgumentException("Invalid id-indexed group bounds");
        }
        if (idIndexedActivationCapacityMultiplier <= 0 ||
                directMinGroups < 0 ||
                directMaxRange <= 0 ||
                directLatestAdmissionGroups < 0 ||
                stagedDirectMaxRange <= 0 ||
                stagedDirectLatestAdmissionGroups < 0 ||
                stagedCompressedDirectMinGroups < 0 ||
                stagedCompressedDirectMaxRange <= 0 ||
                stagedCompressedDirectLatestAdmissionGroups < 0 ||
                directMaxRangePerGroupNumerator <= 0 ||
                directMaxRangePerGroupDenominator <= 0) {
            throw new IllegalArgumentException("Long grouping thresholds must be positive or non-negative as applicable");
        }
    }

    public static LongGroupingPolicy defaults(boolean poolZeroedLongDirectIds)
    {
        return new LongGroupingPolicy(
                true,
                true,
                1 << 20,
                MAX_ID_INDEXED_GROUPS,
                true,
                16,
                true,
                1 << 13,
                1 << 17,
                1 << 14,
                1 << 22,
                1 << 20,
                true,
                1 << 20,
                1 << 26,
                1 << 23,
                poolZeroedLongDirectIds ? 26 : 23,
                2,
                false);
    }

    public static LongGroupingPolicy fromSystemProperties(boolean poolZeroedLongDirectIds)
    {
        LongGroupingPolicy defaults = defaults(poolZeroedLongDirectIds);
        return new LongGroupingPolicy(
                booleanProperty("nitro.group.longRunCache", defaults.runCache()),
                booleanProperty("nitro.group.idIndexedLong", defaults.idIndexed()),
                Integer.getInteger("nitro.group.idIndexedLongMinGroups", defaults.idIndexedMinGroups()),
                Math.min(
                        Integer.getInteger("nitro.group.idIndexedLongMaxGroups", defaults.idIndexedMaxGroups()),
                        MAX_ID_INDEXED_GROUPS),
                booleanProperty("nitro.group.idIndexedLongDenseRehash", defaults.idIndexedDenseRehash()),
                Integer.getInteger(
                        "nitro.group.idIndexedLongActivationCapacityMultiplier",
                        defaults.idIndexedActivationCapacityMultiplier()),
                booleanProperty("nitro.group.longDirectGrouping", defaults.direct()),
                Integer.getInteger("nitro.group.longDirectMinGroups", defaults.directMinGroups()),
                Integer.getInteger("nitro.group.longDirectMaxRange", defaults.directMaxRange()),
                Integer.getInteger("nitro.group.longDirectLatestAdmissionGroups", defaults.directLatestAdmissionGroups()),
                Integer.getInteger("nitro.group.stagedLongDirectMaxRange", defaults.stagedDirectMaxRange()),
                Integer.getInteger(
                        "nitro.group.stagedLongDirectLatestAdmissionGroups",
                        defaults.stagedDirectLatestAdmissionGroups()),
                booleanProperty("nitro.group.stagedCompressedLongDirectGrouping", defaults.stagedCompressedDirect()),
                Integer.getInteger(
                        "nitro.group.stagedCompressedLongDirectMinGroups",
                        defaults.stagedCompressedDirectMinGroups()),
                Integer.getInteger(
                        "nitro.group.stagedCompressedLongDirectMaxRange",
                        defaults.stagedCompressedDirectMaxRange()),
                Integer.getInteger(
                        "nitro.group.stagedCompressedLongDirectLatestAdmissionGroups",
                        defaults.stagedCompressedDirectLatestAdmissionGroups()),
                Integer.getInteger(
                        "nitro.group.longDirectMaxRangePerGroupNumerator",
                        defaults.directMaxRangePerGroupNumerator()),
                Integer.getInteger(
                        "nitro.group.longDirectMaxRangePerGroupDenominator",
                        defaults.directMaxRangePerGroupDenominator()),
                Boolean.getBoolean("nitro.debug.longDirectGrouping"));
    }

    private static boolean booleanProperty(String name, boolean defaultValue)
    {
        return Boolean.parseBoolean(System.getProperty(name, Boolean.toString(defaultValue)));
    }
}
