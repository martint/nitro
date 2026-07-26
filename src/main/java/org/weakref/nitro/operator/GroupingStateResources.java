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

/**
 * Engine-owner-scoped pooling policy and identity used by grouping state.
 */
public final class GroupingStateResources
{
    private final boolean poolZeroedLongDirectIds;
    private final LongGroupingPolicy longGroupingPolicy;
    private final CompositeGroupingPolicy compositeGroupingPolicy;
    private final Object zeroedLongDirectIdsFamily = new Object();
    private final Object groupOperatorBufferPool = new Object();
    private final Object markDistinctMaskPool = new Object();
    private final Object markDistinctMarkerBufferPool = new Object();
    private final Object semiJoinBufferPool = new Object();

    public GroupingStateResources(
            boolean poolZeroedLongDirectIds,
            LongGroupingPolicy longGroupingPolicy,
            CompositeGroupingPolicy compositeGroupingPolicy)
    {
        this.poolZeroedLongDirectIds = poolZeroedLongDirectIds;
        this.longGroupingPolicy = requireNonNull(longGroupingPolicy, "longGroupingPolicy is null");
        this.compositeGroupingPolicy = requireNonNull(compositeGroupingPolicy, "compositeGroupingPolicy is null");
    }

    boolean poolZeroedLongDirectIds()
    {
        return poolZeroedLongDirectIds;
    }

    LongGroupingPolicy longGroupingPolicy()
    {
        return longGroupingPolicy;
    }

    CompositeGroupingPolicy compositeGroupingPolicy()
    {
        return compositeGroupingPolicy;
    }

    Object zeroedLongDirectIdsFamily()
    {
        return zeroedLongDirectIdsFamily;
    }

    Object groupOperatorBufferPool()
    {
        return groupOperatorBufferPool;
    }

    Object markDistinctMaskPool()
    {
        return markDistinctMaskPool;
    }

    Object markDistinctMarkerBufferPool()
    {
        return markDistinctMarkerBufferPool;
    }

    Object semiJoinBufferPool()
    {
        return semiJoinBufferPool;
    }
}
