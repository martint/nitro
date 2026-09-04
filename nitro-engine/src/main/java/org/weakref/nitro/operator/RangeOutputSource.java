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

/** Optional operator capability for synchronous result-range composition without batch transport wrappers. */
public interface RangeOutputSource
{
    /** Drains provider primitive contributions when both sides expose an exact structural match. */
    default boolean drainPrimitiveTo(PrimitiveRangeInputSink sink)
    {
        return false;
    }

    /**
     * Drains every remaining output range to {@code sink}. Returns {@code false} without advancing when the source
     * cannot satisfy the sink directly.
     */
    boolean drainTo(RangeInputSink sink);
}
