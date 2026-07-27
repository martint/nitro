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

/// Engine-selected dense-long representation policy for exact membership sets.
public record MembershipSetPolicy(
        int denseLongMinCapacityBits,
        int denseLongMaxCapacityBits,
        long denseLongMaxBitsPerObservedKey)
{
    public MembershipSetPolicy
    {
        if (denseLongMinCapacityBits <= 0 || Integer.bitCount(denseLongMinCapacityBits) != 1) {
            throw new IllegalArgumentException("denseLongMinCapacityBits must be a positive power of two");
        }
        if (denseLongMaxCapacityBits < denseLongMinCapacityBits ||
                Integer.bitCount(denseLongMaxCapacityBits) != 1) {
            throw new IllegalArgumentException("denseLongMaxCapacityBits must be a power of two at least as large as the minimum");
        }
        if (denseLongMaxBitsPerObservedKey <= 0) {
            throw new IllegalArgumentException("denseLongMaxBitsPerObservedKey must be positive");
        }
    }

    public static MembershipSetPolicy defaults()
    {
        return new MembershipSetPolicy(1 << 16, 1 << 26, 256);
    }

    public static MembershipSetPolicy fromSystemProperties()
    {
        MembershipSetPolicy defaults = defaults();
        return new MembershipSetPolicy(
                Integer.getInteger("nitro.semiJoin.denseLongMinCapacityBits", defaults.denseLongMinCapacityBits()),
                Integer.getInteger("nitro.semiJoin.denseLongMaxCapacityBits", defaults.denseLongMaxCapacityBits()),
                Long.getLong("nitro.semiJoin.denseLongMaxBitsPerObservedKey", defaults.denseLongMaxBitsPerObservedKey()));
    }
}
