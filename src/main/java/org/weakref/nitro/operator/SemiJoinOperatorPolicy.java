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

import static java.util.Objects.requireNonNull;

/// Engine-selected dynamic-filter and membership policy for semi joins.
public record SemiJoinOperatorPolicy(
        boolean dynamicFilterEnabled,
        int dynamicFilterMaxValues,
        int smallBinarySetMaxValues,
        boolean cacheDictionaryMatches,
        MembershipSetPolicy membershipSet)
{
    public SemiJoinOperatorPolicy
    {
        requireNonNull(membershipSet, "membershipSet is null");
    }

    public static SemiJoinOperatorPolicy defaults()
    {
        return new SemiJoinOperatorPolicy(true, 1 << 13, 64, true, MembershipSetPolicy.defaults());
    }

    public static SemiJoinOperatorPolicy fromSystemProperties()
    {
        SemiJoinOperatorPolicy defaults = defaults();
        return new SemiJoinOperatorPolicy(
                booleanProperty("nitro.dynamicFilter", defaults.dynamicFilterEnabled()),
                Integer.getInteger("nitro.dynamicFilter.maxValues", defaults.dynamicFilterMaxValues()),
                Integer.getInteger("nitro.semiJoin.smallBinarySetMaxValues", defaults.smallBinarySetMaxValues()),
                booleanProperty("nitro.semiJoin.cacheDictionaryMatches", defaults.cacheDictionaryMatches()),
                MembershipSetPolicy.fromSystemProperties());
    }

    private static boolean booleanProperty(String name, boolean defaultValue)
    {
        return Boolean.parseBoolean(System.getProperty(name, Boolean.toString(defaultValue)));
    }
}
