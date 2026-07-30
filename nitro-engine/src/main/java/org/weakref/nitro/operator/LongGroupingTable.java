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

import org.weakref.nitro.data.VectorAccess;

/**
 * Shape-neutral contract between the grouping operator and a physical table compiled for a structural
 * long-key schema. Implementations may use any compact layout and generated monomorphic loop, but layout
 * arrays and arity-specific mechanics remain private to the table implementation.
 */
interface LongGroupingTable
{
    int arity();

    /** Whether {@code positions == null} denotes the dense physical range {@code 0..positionCount-1}. */
    default boolean supportsImplicitDensePositions()
    {
        return false;
    }

    /** Whether a null entry in {@code nullAccessors} denotes a provably non-null key column. */
    default boolean supportsSparseNullAccessors()
    {
        return false;
    }

    long assignBatch(
            VectorAccess.LongValues[] keyAccessors,
            VectorAccess.BooleanValues[] nullAccessors,
            int[] positions,
            int positionCount,
            long[] result,
            long startGroupId);

    void ensureCapacity(long expectedSize);

    long groupedValue(int column, int groupId);

    boolean groupedValueIsNull(int column, int groupId);

    default long retainedBytes()
    {
        return 0;
    }

    void releaseBuffers();
}
