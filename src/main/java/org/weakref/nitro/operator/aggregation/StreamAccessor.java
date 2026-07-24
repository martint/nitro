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
package org.weakref.nitro.operator.aggregation;

import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.operator.evaluator.ir.Stream;

/**
 * Read-only accessor for the input streams seen by an accumulator.
 */
@FunctionalInterface
public interface StreamAccessor
{
    /**
     * Returns the requested stream for {@code column}.
     * <p>
     * Optional streams may be absent and are then returned as {@code null}.
     */
    Vector stream(int column, Stream stream);

    /**
     * Returns the VALUES stream for {@code column}.
     */
    default Vector values(int column)
    {
        return stream(column, Stream.VALUES);
    }

    /**
     * Returns the NULLS stream for {@code column}, or {@code null} if the input has no null side
     * stream.
     */
    default Vector nulls(int column)
    {
        return stream(column, Stream.NULLS);
    }

    default VectorAccess.BooleanValues nullValues(int column)
    {
        return VectorAccess.booleanValues(nulls(column));
    }

    default VectorAccess.LongValues longValues(int column)
    {
        return VectorAccess.longValues(values(column));
    }

    default VectorAccess.DoubleValues doubleValues(int column)
    {
        return VectorAccess.doubleValues(values(column));
    }

    default VectorAccess.BinaryValues binaryValues(int column)
    {
        return VectorAccess.binaryValues(values(column));
    }
}
