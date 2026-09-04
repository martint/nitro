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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.ValueDemand;
import org.weakref.nitro.data.Vector;

/**
 * Classloader-neutral state protocol supplied by a dynamically resolved aggregate.
 *
 * <p>The implementation owns the opaque state object. The engine selects raw or intermediate
 * input and intermediate or final output from the physical plan; it does not infer a function or
 * rewrite neighboring aggregates.
 */
public interface AggregationImplementation
{
    /**
     * Physical value content requested for one raw input.
     *
     * <p>Implementations which can reduce an encoded domain using exact logical multiplicities may request
     * {@link ValueDemand#FULL_WITH_DOMAIN_COUNTS}. The engine propagates the request toward a capable source but
     * does not require the source or an intervening expression to preserve that representation.
     */
    default ValueDemand rawInputValueDemand(int input)
    {
        return ValueDemand.FULL;
    }

    /**
     * Returns an implementation that may use a provider-owned physical intermediate representation.
     *
     * <p>The representation is only valid while execution remains inside Nitro. A composition adapter must retain
     * the ordinary implementation whenever an intermediate result can cross a portable or host boundary. The
     * engine does not inspect or infer the representation.
     */
    default AggregationImplementation physicalIntermediateOutput()
    {
        return this;
    }

    /**
     * Selects the logical state capacity for a required group count.
     *
     * <p>The default preserves the engine's amortized-growth capacity. Implementations backed by
     * incrementally allocated storage may return {@code requiredGroups} to avoid unused geometric
     * headroom and whole-state copy peaks.
     */
    default int stateCapacity(int requiredGroups, int defaultCapacity)
    {
        return defaultCapacity;
    }

    Object allocate(AggregationExecution execution, int groups);

    Object grow(Allocator allocator, Allocator.Context allocationContext, Object state, int groups);

    void initialize(Object state, int offset, int length);

    void addRawInput(Object state, int group, Mask mask, AggregationInput input);

    void addRawInput(Object state, Vector groups, Mask mask, AggregationInput input);

    /**
     * Whether grouped input may receive group ids as an encoded vector instead of a flat logical-row vector.
     *
     * <p>The encoding is only an alternate physical representation of the same logical group ids. Implementations
     * may consume a compatible encoded input domain directly and must fall back to ordinary logical-position
     * processing for incompatible input encodings. This capability describes a calling convention, not aggregate
     * identity or semantics.
     */
    default boolean supportsEncodedGroupedInput()
    {
        return false;
    }

    /**
     * Whether raw grouped input can consume an exact weighted physical key domain for this input batch.
     *
     * <p>The capability check must not mutate aggregate state. It may inspect input encodings and side streams so
     * the engine can fall back atomically before invoking any aggregate in the physical program.
     */
    default boolean supportsRawGroupedDomainInput(AggregationInput input)
    {
        return false;
    }

    /**
     * Whether this implementation may consume raw grouped-domain input for a compatible physical batch.
     *
     * <p>This shape-independent declaration lets planning retain the exact domain metadata needed to make the later
     * batch-local capability check meaningful. Returning {@code true} does not admit a batch: the engine must still
     * call {@link #supportsRawGroupedDomainInput(AggregationInput)} and the row-mapping overload atomically before
     * mutating state.
     */
    default boolean maySupportRawGroupedDomainInput()
    {
        return false;
    }

    /**
     * Whether the raw input can consume a domain formed from this row mapping. This check runs before grouping
     * mutates engine state, allowing the engine to retain its ordinary logical-row path when mappings differ.
     */
    default boolean supportsRawGroupedDomainInput(DictionaryVector rowMapping, AggregationInput input)
    {
        return supportsRawGroupedDomainInput(input);
    }

    /**
     * Whether this input needs one selected logical representative for each used physical-domain entry.
     */
    default boolean requiresRawGroupedDomainRepresentatives(DictionaryVector rowMapping, AggregationInput input)
    {
        return false;
    }

    /**
     * Whether raw grouped input can consume the particular domain selected by the engine.
     *
     * <p>The ordinary capability check runs before the grouping domain exists. Value-bearing implementations must
     * additionally prove here that their encoded inputs share the domain's row mapping. The default preserves the
     * behavior of implementations whose result depends only on weights, such as {@code count(*)}.
     */
    default boolean supportsRawGroupedDomainInput(GroupedAggregationDomain domain, AggregationInput input)
    {
        return supportsRawGroupedDomainInput(domain.rowMapping(), input);
    }

    /**
     * Adds raw input once per physical key using exact selected-row multiplicities.
     *
     * <p>Implementations own value, null, and numerical-reduction semantics. The engine supplies only resolved group
     * ids and exact weights; it does not identify or rewrite aggregate functions.
     */
    default void addRawGroupedDomainInput(Object state, GroupedAggregationDomain domain, AggregationInput input)
    {
        throw new UnsupportedOperationException("raw grouped domain input is not supported");
    }

    /**
     * Optionally binds a direct single-position update to one physical input batch.
     *
     * <p>Window execution uses this capability for running frames. Returning {@code null} retains the ordinary
     * mask-based update. The implementation, rather than the engine, owns physical type and function semantics.
     */
    default AggregationPositionAccumulator bindRawInputPosition(Object state, int group, AggregationInput input)
    {
        return null;
    }

    /**
     * Optionally binds exact add/remove updates for overlapping window frames.
     *
     * <p>Every removed position must previously have been added to the same provider state. Bindings may be refreshed
     * across physical source batches and must update that shared state exactly. Returning {@code null} retains frame
     * replay for the implementation.
     */
    default ReversibleAggregationPositionAccumulator bindReversibleRawInputPosition(
            Object state,
            int group,
            AggregationInput input)
    {
        return null;
    }

    /** Whether every physical binding for this implementation supports exact reversible updates. */
    default boolean supportsReversibleRawInputPosition()
    {
        return false;
    }

    /**
     * Optionally binds a provider-owned batch loop for pre-resolved, monotonically advancing window frames.
     * Returning {@code null} retains reversible single-position updates or complete-frame replay.
     */
    default ReversibleAggregationWindowKernel bindReversibleWindowKernel(Object state, int group, int inputCount)
    {
        return null;
    }

    /** Exact primitive result shape of the reversible window kernel, or {@code null} for vector-only output. */
    default PrimitiveRangeContribution primitiveWindowResultContribution(int inputCount)
    {
        return null;
    }

    /**
     * Optionally binds one raw-input primitive contribution directly to provider state. The returned input index is
     * relative to this aggregate's arguments; {@code -1} denotes range cardinality. Exact carrier and null semantics
     * are matched structurally with an upstream provider before either provider advances semantic state.
     */
    default PrimitiveAggregationInput bindPrimitiveRangeInput(Object state, int group, int inputCount)
    {
        return null;
    }

    /** Exact primitive raw-input shape, available without allocating or mutating provider state. */
    default PrimitiveRangeContribution primitiveRangeInputContribution(int inputCount)
    {
        return null;
    }

    /** Argument index consumed by {@link #primitiveRangeInputContribution}; {@code -1} denotes cardinality. */
    default int primitiveRangeInputIndex(int inputCount)
    {
        return -1;
    }

    void addIntermediate(Object state, int group, Mask mask, AggregationInput input);

    void addIntermediate(Object state, Vector groups, Mask mask, AggregationInput input);

    /**
     * Whether this implementation can lower selected raw rows directly to independent intermediate states.
     */
    default boolean supportsInitialRawIntermediate()
    {
        return false;
    }

    /**
     * Lowers selected raw rows directly to independent intermediate states.
     *
     * <p>Adaptive partial aggregation uses this when every input row is deliberately passed as its own group. The
     * function implementation owns the physical state representation; the execution engine does not recognize
     * individual aggregate functions or types.
     */
    default Streams initialRawIntermediate(
            Mask mask,
            AggregationInput input,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        throw new UnsupportedOperationException("direct initial intermediate state is not supported");
    }

    /**
     * Whether direct initial output can preserve the input mask's physical positions instead of compacting
     * selected rows to {@code [0, mask.count())}.
     */
    default boolean supportsPositionPreservingInitialRawIntermediate()
    {
        return false;
    }

    /**
     * Lowers raw rows to intermediate state at their original physical positions. Unselected positions are
     * unspecified and consumers must retain the supplied mask.
     */
    default Streams positionPreservingInitialRawIntermediate(
            Mask mask,
            AggregationInput input,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        throw new UnsupportedOperationException("position-preserving initial intermediate state is not supported");
    }

    Streams intermediate(
            int maxGroup,
            Object state,
            Streams existing,
            Allocator allocator,
            Allocator.Context allocationContext);

    default Streams intermediate(
            int maxGroup,
            Object state,
            Mask mask,
            Streams existing,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        return intermediate(maxGroup, state, existing, allocator, allocationContext);
    }

    Streams result(
            int maxGroup,
            Object state,
            Streams existing,
            Allocator allocator,
            Allocator.Context allocationContext);

    default Streams result(
            int maxGroup,
            Object state,
            Mask mask,
            Streams existing,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        return result(maxGroup, state, existing, allocator, allocationContext);
    }

    /**
     * Copies one intermediate result position without materializing every group.
     *
     * <p>Returning {@code null} requests the engine's full-result fallback.
     */
    default Streams copyIntermediatePosition(
            int group,
            int maxGroup,
            Object state,
            Streams existing,
            int outputPosition,
            int size,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        return null;
    }

    /**
     * Copies one final result position without materializing every group.
     *
     * <p>Returning {@code null} requests the engine's full-result fallback.
     */
    default Streams copyResultPosition(
            int group,
            int maxGroup,
            Object state,
            Streams existing,
            int outputPosition,
            int size,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        return null;
    }

    default Streams copyResultRange(
            int groupStart,
            int groupCount,
            int maxGroup,
            Object state,
            Streams existing,
            int outputStart,
            int size,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        return null;
    }

    default Streams copyIntermediateRange(
            int groupStart,
            int groupCount,
            int maxGroup,
            Object state,
            Streams existing,
            int outputStart,
            int size,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        return null;
    }
}
