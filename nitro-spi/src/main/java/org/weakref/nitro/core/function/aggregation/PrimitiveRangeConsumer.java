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
package org.weakref.nitro.core.function.aggregation;

/** Provider-owned consumer of primitive forward-range contributions. */
public interface PrimitiveRangeConsumer
{
    default void addLong(long value)
    {
        throw new UnsupportedOperationException("long contributions are not supported");
    }

    /**
     * Adds one physical long contribution with an exact logical multiplicity. Providers may override this to update
     * state once; the default preserves the scalar calling convention exactly.
     */
    default void addRepeatedLong(long value, long count)
    {
        if (count < 0) {
            throw new IllegalArgumentException("count is negative");
        }
        for (long index = 0; index < count; index++) {
            addLong(value);
        }
    }

    default void addNull()
    {
        throw new UnsupportedOperationException("null contributions are not supported");
    }

    default void addCardinality(long count)
    {
        throw new UnsupportedOperationException("cardinality contributions are not supported");
    }
}
