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

/// Engine-selected output batching policy for nested-loop joins.
///
/// The property-backed factory is a standalone composition adapter. Nested-loop joins receive one immutable policy
/// from their resource owner and never consult process-global configuration.
public record NestedLoopJoinPolicy(int maxBatchRows)
{
    public static NestedLoopJoinPolicy defaults()
    {
        return new NestedLoopJoinPolicy(10_000);
    }

    public static NestedLoopJoinPolicy fromSystemProperties()
    {
        NestedLoopJoinPolicy defaults = defaults();
        return new NestedLoopJoinPolicy(
                Integer.getInteger("nitro.nestedloop.maxBatchRows", defaults.maxBatchRows()));
    }
}
