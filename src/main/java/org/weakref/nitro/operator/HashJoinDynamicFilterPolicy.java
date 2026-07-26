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

/// Collection, representation, publication, and probe-first policy for hash-join dynamic filters.
public record HashJoinDynamicFilterPolicy(
        boolean enabled,
        boolean shareSparseLongRange,
        boolean multiKey,
        int maxValues,
        boolean preSizeValueSets,
        boolean deferDeduplication,
        int deferDeduplicationMinRows,
        double deferDeduplicationMaxDensity,
        boolean trackValueRange,
        long buildRowLimit,
        boolean probeFirstBuildFilter,
        long probeFirstMinBuildRows,
        int probeFirstMaxProbeRows,
        boolean debug)
{
    public static HashJoinDynamicFilterPolicy defaults()
    {
        return new HashJoinDynamicFilterPolicy(
                true,
                true,
                true,
                1 << 16,
                true,
                true,
                4096,
                0.005,
                true,
                1L << 16,
                true,
                4L << 20,
                1 << 16,
                false);
    }

    public static HashJoinDynamicFilterPolicy fromSystemProperties()
    {
        HashJoinDynamicFilterPolicy defaults = defaults();
        return new HashJoinDynamicFilterPolicy(
                booleanProperty("nitro.dynamicFilter", defaults.enabled()),
                booleanProperty("nitro.dynamicFilter.shareSparseLongRange", defaults.shareSparseLongRange()),
                booleanProperty("nitro.dynamicFilter.multiKey", defaults.multiKey()),
                Integer.getInteger("nitro.dynamicFilter.maxValues", defaults.maxValues()),
                booleanProperty("nitro.dynamicFilter.preSizeValueSets", defaults.preSizeValueSets()),
                booleanProperty("nitro.dynamicFilter.deferDeduplication", defaults.deferDeduplication()),
                Integer.getInteger(
                        "nitro.dynamicFilter.deferDeduplicationMinRows",
                        defaults.deferDeduplicationMinRows()),
                Double.parseDouble(System.getProperty(
                        "nitro.dynamicFilter.deferDeduplicationMaxDensity",
                        Double.toString(defaults.deferDeduplicationMaxDensity()))),
                booleanProperty("nitro.dynamicFilter.trackValueRange", defaults.trackValueRange()),
                Long.getLong("nitro.dynamicFilter.buildRowLimit", defaults.buildRowLimit()),
                booleanProperty("nitro.hash.join.probeFirstBuildFilter", defaults.probeFirstBuildFilter()),
                Long.getLong("nitro.hash.join.probeFirstMinBuildRows", defaults.probeFirstMinBuildRows()),
                Integer.getInteger("nitro.hash.join.probeFirstMaxProbeRows", defaults.probeFirstMaxProbeRows()),
                Boolean.getBoolean("nitro.debug.dynamicFilter"));
    }

    private static boolean booleanProperty(String name, boolean defaultValue)
    {
        return Boolean.parseBoolean(System.getProperty(name, Boolean.toString(defaultValue)));
    }
}
