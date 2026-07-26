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

/// Physical policy for the adaptive compact multi-long table shared by grouping and distinct execution.
public record AdaptiveLongGroupingPolicy(
        float loadFactor,
        boolean highDensityPairCapacity,
        float highDensityPairLoadFactor,
        int highDensityPairMinSlots,
        boolean highDensityPairTerminalJump,
        int highDensityPairTerminalJumpSlots,
        boolean groupedProbe,
        int groupedProbeMinSlots,
        boolean debugShapes)
{
    public AdaptiveLongGroupingPolicy
    {
        if (!(loadFactor > 0 && loadFactor < 1) ||
                !(highDensityPairLoadFactor > 0 && highDensityPairLoadFactor < 1) ||
                highDensityPairMinSlots <= 0 ||
                Integer.bitCount(highDensityPairMinSlots) != 1 ||
                highDensityPairTerminalJumpSlots <= 0 ||
                Integer.bitCount(highDensityPairTerminalJumpSlots) != 1 ||
                groupedProbeMinSlots <= 0 ||
                Integer.bitCount(groupedProbeMinSlots) != 1) {
            throw new IllegalArgumentException("Adaptive long grouping thresholds are invalid");
        }
    }

    public static AdaptiveLongGroupingPolicy defaults()
    {
        return new AdaptiveLongGroupingPolicy(
                0.75f,
                true,
                0.825f,
                1 << 24,
                true,
                1 << 22,
                true,
                1 << 20,
                false);
    }

    public static AdaptiveLongGroupingPolicy fromSystemProperties()
    {
        AdaptiveLongGroupingPolicy defaults = defaults();
        return new AdaptiveLongGroupingPolicy(
                defaults.loadFactor(),
                booleanProperty(
                        "nitro.group.adaptiveLongHighDensityPair",
                        defaults.highDensityPairCapacity()),
                defaults.highDensityPairLoadFactor(),
                defaults.highDensityPairMinSlots(),
                booleanProperty(
                        "nitro.group.adaptiveLongHighDensityPairTerminalJump",
                        defaults.highDensityPairTerminalJump()),
                defaults.highDensityPairTerminalJumpSlots(),
                booleanProperty("nitro.group.adaptiveLongGroupedProbe", defaults.groupedProbe()),
                defaults.groupedProbeMinSlots(),
                Boolean.getBoolean("nitro.debug.adaptiveLongGrouping"));
    }

    private static boolean booleanProperty(String name, boolean defaultValue)
    {
        return Boolean.parseBoolean(System.getProperty(name, Boolean.toString(defaultValue)));
    }
}
