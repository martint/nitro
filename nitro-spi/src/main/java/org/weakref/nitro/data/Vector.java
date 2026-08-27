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
package org.weakref.nitro.data;

import java.util.function.Consumer;

/**
 * Base contract for all columnar value containers in Nitro.
 * <p>
 * A vector represents one logical stream for a batch. Concrete implementations may store values
 * directly ({@link FlatVector}), indirectly through wrappers such as dictionary or run-length
 * encoding, or as nested composite structures.
 * <p>
 * The methods on this interface are intentionally split into:
 * <ul>
 *     <li>lifecycle and accounting ({@link #retainedBytes()}, pooling hooks)</li>
 *     <li>structural copy operations used by generic runtime code</li>
 *     <li>child traversal for recursive release/transfer</li>
 * </ul>
 * Generic operators should prefer these hooks over branching on concrete vector classes.
 */
public interface Vector
{
    long NO_CONTENT_FINGERPRINT = Long.MIN_VALUE;

    /**
     * Returns the logical row count represented by this vector.
     */
    int length();

    /**
     * Returns the bytes retained directly by this vector instance.
     * <p>
     * Child vectors are reported separately through {@link #forEachChildVector(Consumer)} so the
     * allocator can account for ownership trees without double-counting.
     */
    long retainedBytes();

    /**
     * Identifies the current logical contents of a reusable vector instance.
     *
     * <p>Derived-state caches may reuse work across batches only when both object identity and this generation
     * match. The default declines that capability; pooled concrete vectors can opt in by advancing the generation
     * whenever a new logical lifetime begins.
     */
    default long contentGeneration()
    {
        return -1;
    }

    /**
     * Marks this vector's current logical contents immutable for the remainder of its allocator lifetime.
     * Derived encodings can use this together with {@link #contentGeneration()} to share cached work while the
     * owner retains the vector. Implementations that do not expose stable content retain the default no-op.
     */
    default Vector freezeContent()
    {
        return this;
    }

    /** Whether {@link #freezeContent()} established an immutable current logical lifetime. */
    default boolean contentImmutable()
    {
        return false;
    }

    /**
     * A representation-owned fingerprint for collision-checked encoded-domain reuse. Implementations return
     * {@link #NO_CONTENT_FINGERPRINT} when they cannot compare logical contents cheaply and exactly.
     */
    default long contentFingerprint()
    {
        return NO_CONTENT_FINGERPRINT;
    }

    /** Exact logical-content comparison used only after matching non-sentinel fingerprints. */
    default boolean hasSameContent(Vector other)
    {
        return this == other && contentGeneration() == other.contentGeneration();
    }

    /**
     * Whether copying this vector's logical values includes variable-size payload storage in addition to its
     * position metadata. Buffering frameworks use this representation property to decide when eliminating a full
     * intermediate copy can amortize a different output layout; operators need not recognize concrete data types.
     */
    default boolean isVariableWidth()
    {
        return false;
    }

    /**
     * Whether incremental writes into a shared output must visit output positions in ascending order.
     *
     * <p>Repeated vectors append child values while their parent offsets are constructed. A branch-at-a-time merge
     * would visit disjoint parent positions out of order and corrupt that offset/child relationship. Composite and
     * encoded vectors propagate the requirement from their physical children.
     */
    default boolean requiresMonotonicOutputWrites()
    {
        return false;
    }

    /**
     * Produces a logical copy of the entire vector in the target allocator context.
     */
    Vector copy(Allocator allocator, Allocator.Context allocationContext);

    /**
     * Produces a compact copy containing only the supplied logical positions, reindexed densely
     * from {@code 0..positions.length-1}.
     */
    Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions);

    /**
     * Copies the selected positions into a vector with the same logical length as the source.
     * <p>
     * Positions not present in {@code mask} are left unchanged in {@code existing}. Generic merge
     * paths use this to preserve branch/output indexing while updating only the active rows.
     */
    default Vector copyMasked(Allocator allocator, Allocator.Context allocationContext, Vector existing, Mask mask)
    {
        throw new UnsupportedOperationException("Vector does not support copyMasked: " + getClass().getSimpleName());
    }

    /**
     * Copies {@code sourceCount} positions from this vector into {@code existing}, starting at
     * {@code outputStart} in a result vector of logical size {@code size}.
     * <p>
     * This is the append/compaction-oriented copy hook used by joins, top-N buffering, and other
     * operators that build new dense outputs from arbitrary source row selections.
     */
    default Vector copyPositionsInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        throw new UnsupportedOperationException("Vector does not support copyPositionsInto: " + getClass().getSimpleName());
    }

    /**
     * Copies the supplied logical positions into {@code existing} without first materializing the
     * selected row set into an intermediate array.
     * <p>
     * Encoded vectors may override this to compose row selections lazily. The default
     * implementation materializes the positions and delegates to
     * {@link #copyPositionsInto(Allocator, Allocator.Context, Vector, int[], int, int, int)}.
     */
    default Vector copySelectedPositionsInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, SelectedPositions sourcePositions, int outputStart, int size)
    {
        int[] positions = sourcePositions.materialize(null);
        return copyPositionsInto(allocator, allocationContext, existing, positions, positions.length, outputStart, size);
    }

    /**
     * Copies one logical position into {@code existing} at {@code outputPosition} in a result
     * vector of logical size {@code size}.
     * <p>
     * This is the fine-grained companion to {@link #copyPositionsInto(Allocator, Allocator.Context, Vector, int[], int, int, int)}.
     */
    default Vector copySinglePositionInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        throw new UnsupportedOperationException("Vector does not support copySinglePositionInto: " + getClass().getSimpleName());
    }

    /**
     * Copies one logical position into every position in the contiguous output range.
     *
     * <p>The default preserves compatibility for uncommon vector shapes. Flat and nested vectors
     * override this operation so full-partition window results can be broadcast without one
     * allocation-aware copy dispatch per output row.
     */
    default Vector copySinglePositionRangeInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int sourcePosition, int outputStart, int outputEnd, int size)
    {
        Vector output = existing;
        for (int outputPosition = outputStart; outputPosition < outputEnd; outputPosition++) {
            output = copySinglePositionInto(allocator, allocationContext, output, sourcePosition, outputPosition, size);
        }
        return output;
    }

    /**
     * Creates an empty vector with the same logical type/shape as this vector.
     * <p>
     * Traits or nested stream schemas should be preserved, while the returned vector has length
     * zero and no row data.
     */
    default Vector emptyLike(Allocator allocator, Allocator.Context allocationContext)
    {
        throw new UnsupportedOperationException("Vector does not support emptyLike: " + getClass().getSimpleName());
    }

    /**
     * Materializes a dense vector by concatenating the rows from {@code rows}.
     * <p>
     * Each entry in {@code rows} is expected to represent the same logical stream shape as this
     * vector. This hook lets nested and encoded vectors define their own row assembly semantics.
     */
    default Vector materializeRows(Allocator allocator, Allocator.Context allocationContext, Vector[] rows)
    {
        throw new UnsupportedOperationException("Vector does not support materializeRows: " + getClass().getSimpleName());
    }

    /**
     * Copies the entire contents of this vector into {@code target}.
     * <p>
     * The caller is responsible for ensuring that {@code target} is compatible and has sufficient
     * capacity.
     */
    default void copyInto(Vector target)
    {
        throw new UnsupportedOperationException("Vector does not support copyInto: " + getClass().getSimpleName());
    }

    /**
     * Clears reusable state before the vector is returned from a pool.
     */
    default void clearForReuse()
    {
        throw new UnsupportedOperationException("Vector does not support clearForReuse: " + getClass().getSimpleName());
    }

    /**
     * Returns the pool family key used by the allocator.
     * <p>
     * Vectors that are not reusable should return {@code null}.
     */
    default Object poolFamily()
    {
        return null;
    }

    /**
     * Returns the capacity used as the allocator pool bucket key for this vector.
     */
    default int poolCapacity()
    {
        return 0;
    }

    /**
     * Returns the maximum number of idle vectors of this family/capacity that the allocator should
     * retain.
     */
    default int poolMaxRetained()
    {
        return 0;
    }

    /**
     * Returns the allocator retention class for this vector.
     */
    default VectorPoolRetentionClass poolRetentionClass()
    {
        return VectorPoolRetentionClass.VECTOR_DEFAULT;
    }

    /** Returns the number of directly owned child vectors without allocating a traversal callback. */
    default int childVectorCount()
    {
        return 0;
    }

    /** Returns one directly owned child vector in stable structural order. */
    default Vector childVector(int index)
    {
        throw new IndexOutOfBoundsException(index);
    }

    /**
     * Visits directly referenced child vectors, if any.
     * <p>
     * The allocator uses this to transfer and release ownership trees recursively.
     */
    default void forEachChildVector(Consumer<Vector> consumer)
    {
        for (int index = 0; index < childVectorCount(); index++) {
            consumer.accept(childVector(index));
        }
    }

    /**
     * Converts producer-context ownership that must survive {@code Output.take()} into a transferable lease.
     * Implementations should only lease storage they own directly; generic allocator traversal invokes this hook for
     * every node in the vector tree before detaching the remaining producer-owned nodes.
     */
    default void prepareBufferTransfer(Allocator allocator, Allocator.Context producerContext)
    {
    }

    /** Releases buffers directly owned by this vector whose ownership crossed an output boundary through take. */
    default void releaseTransferredBuffers()
    {
    }
}
