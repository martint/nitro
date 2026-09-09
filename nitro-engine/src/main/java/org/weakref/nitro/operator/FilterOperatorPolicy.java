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
 * Engine-selected physical policies for filter execution.
 */
public record FilterOperatorPolicy(
        boolean fuseConstantRanges,
        boolean pushStaticLongEquality,
        boolean pushStaticBinaryEquality,
        boolean pushStaticLongRanges,
        boolean recycleOutputMasks)
{
    public static FilterOperatorPolicy defaults()
    {
        return new FilterOperatorPolicy(true, true, false, true, true);
    }

    public static FilterOperatorPolicy fromSystemProperties()
    {
        return new FilterOperatorPolicy(
                Boolean.parseBoolean(System.getProperty("nitro.expression.fuseLongConstantRanges", "true")),
                Boolean.parseBoolean(System.getProperty("nitro.filter.pushStaticLongEquality", "true")),
                Boolean.parseBoolean(System.getProperty("nitro.filter.pushStaticBinaryEquality", "false")),
                Boolean.parseBoolean(System.getProperty("nitro.filter.pushStaticLongRanges", "true")),
                Boolean.parseBoolean(System.getProperty("nitro.filter.recycleOutputMasks", "true")));
    }
}
