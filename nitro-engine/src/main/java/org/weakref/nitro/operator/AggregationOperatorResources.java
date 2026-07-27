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
 * Engine-owner-scoped compatibility domains used by global aggregation.
 *
 * <p>The token carries no storage itself. An allocator uses its identity to let completed aggregate operators recycle
 * compatible buffers without giving one operator ownership of another operator's live state.
 */
public final class AggregationOperatorResources
{
    private final AggregationOperatorPolicy policy;
    private final Object bufferPoolGroup = new Object();

    public AggregationOperatorResources(AggregationOperatorPolicy policy)
    {
        this.policy = requireNonNull(policy, "policy is null");
    }

    AggregationOperatorPolicy policy()
    {
        return policy;
    }

    Object bufferPoolGroup()
    {
        return bufferPoolGroup;
    }
}
