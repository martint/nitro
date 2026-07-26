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
package org.weakref.nitro.core.function.mask;

import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Streams;

/**
 * Registry-produced physical range metadata used only while lowering a logical mask plan.
 */
public final class RangeConstraint
{
    private RangeConstraint() {}

    public enum Position
    {
        LOWER_EXCLUSIVE,
        UPPER_EXCLUSIVE
    }

    /**
     * Applies a pair of compatible bounds. Returning false means unsupported physical input and must not mutate mask.
     */
    @FunctionalInterface
    public interface Kernel
    {
        boolean apply(Streams input, Object lowerExclusive, Object upperExclusive, Mask mask);
    }
}
