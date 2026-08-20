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

import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Mask;

import java.util.Optional;
import java.util.Set;

/**
 * Pull-based batch operator.
 * <p>
 * Operators expose batches one at a time through the {@link #hasNext()}/{@link #next()} contract.
 * The normal usage pattern is:
 * <pre>{@code
 * while (operator.hasNext()) {
 *     try (Batch batch = operator.next()) {
 *         ...
 *     }
 * }
 * }</pre>
 * {@link #hasNext()} may prefetch internally, so {@link #next()} must return the same staged batch
 * rather than advancing the source again.
 */
public interface Operator
        extends AutoCloseable
{
    /**
     * Returns the number of logical outputs this operator exposes in each batch.
     */
    int outputCount();

    /// Returns the immutable logical output schema known before execution.
    ///
    /// The default is a migration bridge for legacy factories. New factories must supply bound
    /// logical types rather than infer them from the first batch.
    default Schema outputSchema()
    {
        return Schema.unspecified(outputCount());
    }

    /**
     * Returns {@code true} if another batch is available.
     */
    boolean hasNext();

    /**
     * Returns the next batch.
     * <p>
     * The caller becomes responsible for closing the batch.
     */
    Batch next();

    /**
     * Narrows the current pending batch to {@code mask}.
     * <p>
     * Operators may use this to avoid materializing work for rows that a downstream consumer has
     * already discarded.
     */
    void constrain(Mask mask);

    /// Maps demanded outputs of this operator to the leaf-source outputs needed to produce them.
    ///
    /// This is a pre-poll physical contract. Operators which cannot describe the mapping return empty, causing the
    /// source to retain its conservative all-output default. Pass-through and expression operators may override it
    /// so a source can avoid materializing values used only by predicates it has accepted for exact enforcement.
    default Optional<Set<Integer>> sourceOutputDemand(Set<Integer> demandedOutputs)
    {
        return Optional.empty();
    }

    /**
     * Returns whether batches from this operator may outlive calls that advance the operator.
     * <p>
     * If this is {@code false}, downstream operators must finish consuming or copying any required
     * data from the current batch before asking the source for another one.
     */
    default boolean supportsRetainedBatches()
    {
        return false;
    }

    /**
     * Returns whether repeated output borrows from the current open batch remain stable until that batch is closed.
     * <p>
     * This is deliberately weaker than {@link #supportsRetainedBatches()}: an operator may reuse its buffers when
     * advanced while still providing ordinary stable batch-lifetime views. Consumers that copy all outputs before
     * closing and advancing (for example a columnar sort buffer) should use this capability instead of requiring the
     * batch to outlive an advance.
     */
    default boolean supportsStableBatchBorrow()
    {
        return supportsRetainedBatches();
    }

    /**
     * Returns whether {@link #hasNext()} may be called while the most recent batch remains open without advancing
     * or invalidating that batch.
     * <p>
     * This is weaker than {@link #supportsRetainedBatches()}: an operator whose availability is already known from
     * instance-owned execution state can answer without touching its input, even when its output batch cannot
     * outlive the next {@link #next()} call.
     */
    default boolean supportsOpenBatchHasNext()
    {
        return supportsRetainedBatches();
    }

    /**
     * Returns the exact number of rows this operator will produce when that is known without executing it, or
     * {@code -1} otherwise. Consumers may use this only as a capacity hint; it must never affect query semantics.
     */
    default long exactOutputRows()
    {
        return -1;
    }

    /**
     * Returns whether the most recent batch can be re-borrowed after a {@link #constrain(Mask)}
     * narrows it, yielding values for the constrained positions.
     * <p>
     * This is weaker than {@link #supportsRetainedBatches()}: an operator that recomputes its
     * output on demand (for example a projection) can satisfy a constrained re-borrow even though it
     * does not let batches outlive an advance. Leaf sources whose underlying reader advances
     * irreversibly (for example a Parquet scan) must return {@code false}. Downstream operators use
     * this to decide whether non-key payload columns can be materialized lazily from a single
     * retained source batch instead of being eagerly copied during build.
     */
    default boolean supportsConstrainedReborrow()
    {
        return supportsRetainedBatches();
    }

    /**
     * Pushes a runtime {@link DynamicFilter} from a downstream join's build side toward this operator's source(s).
     * <p>
     * The default forwards nothing. Pass-through operators override this to remap the filter's column into their
     * source's output space and forward it; a scan applies it during decode (decode the filtered key, test
     * membership, skip-decode the rest only for survivors) — the Velox-style dynamic-filtering path. A filter is
     * always a superset over the join key, so applying it never changes query results, only how many rows decode.
     */
    default void pushDynamicFilter(DynamicFilter filter) {}

    /**
     * Offers an exact static predicate to the source and returns its eventual enforcement result.
     *
     * <p>The caller must retain its residual predicate unless the returned negotiation reports full enforcement.
     * Operators which only support pruning may use the default: it still forwards the domain through the ordinary
     * dynamic-filter path but never transfers semantic responsibility.
     */
    default StaticFilterEnforcement pushStaticFilter(DynamicFilter filter)
    {
        pushDynamicFilter(filter);
        return StaticFilterEnforcement.residual();
    }

    /**
     * Returns whether this operator can forward or consume a dynamic filter before it is read.
     *
     * <p>This is a physical capability, not a promise that a particular filter will be selective. It lets generic
     * joins avoid paying to derive a probe-side membership set when the build pipeline cannot use it.
     */
    default boolean supportsDynamicFilterPushdown(int column)
    {
        return false;
    }

    /**
     * Releases any operator-owned resources.
     */
    void close();
}
