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

import org.weakref.nitro.core.function.mask.StaticLongEqualityProvider;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.EvaluatorFunctionCallSite;
import org.weakref.nitro.operator.evaluator.PlanEvaluator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.MaskExpression;
import org.weakref.nitro.operator.evaluator.ir.MaskExpressionResolver;
import org.weakref.nitro.operator.evaluator.ir.RangeConstraintLowerer;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.ReferenceMask;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class FilterOperator
        implements Operator
{
    private final Allocator.Context allocationContext = new Allocator.Context("FilterOperator");

    private final Operator source;
    private final Allocator allocator;
    private final PlanEvaluator planEvaluator;
    private final EvaluationPlan evaluationPlan;
    private final PrimitiveRegistry primitiveRegistry;
    private final MaskExpression originalPredicateMask;
    private final List<StaticPredicatePushdown> staticPredicatePushdowns;
    private final FilterOperatorPolicy policy;

    private BatchState currentBatchState;
    private MaskExpression effectivePredicateMask;

    public FilterOperator(
            Operator source,
            EvaluationPlan evaluationPlan,
            PrimitiveRegistry primitiveRegistry,
            Reference predicateReference,
            Allocator allocator,
            FilterOperatorResources resources)
    {
        this(
                source,
                evaluationPlan,
                primitiveRegistry,
                MaskExpressionResolver.resolve(evaluationPlan, predicateReference),
                allocator,
                resources);
    }

    public FilterOperator(
            Operator source,
            EvaluationPlan evaluationPlan,
            PrimitiveRegistry primitiveRegistry,
            MaskExpression predicateMask,
            Allocator allocator,
            FilterOperatorResources resources)
    {
        this.source = source;
        this.allocator = allocator;
        this.evaluationPlan = evaluationPlan;
        this.primitiveRegistry = primitiveRegistry;
        this.policy = resources.policy();
        this.planEvaluator = new PlanEvaluator(evaluationPlan, primitiveRegistry, new PlanEvaluator.InputResolver()
        {
            @Override
            public Vector resolve(Reference reference, Mask currentMask)
            {
                return switch (reference.producer()) {
                    case org.weakref.nitro.operator.evaluator.ir.Input(int index) -> currentBatchState.sourceBatch().output(index).borrowOrNull(reference.stream(), currentMask);
                    default -> throw new IllegalArgumentException("Unexpected input reference: " + reference);
                };
            }

            @Override
            public Mask resolveMask(Reference reference, Mask currentMask, boolean selectTrue, Allocator resultAllocator, Allocator.Context resultAllocationContext)
            {
                return switch (reference.producer()) {
                    case org.weakref.nitro.operator.evaluator.ir.Input(int index) -> currentBatchState.sourceBatch().output(index).tryBorrowMask(reference.stream(), currentMask, selectTrue, resultAllocator, resultAllocationContext);
                    default -> null;
                };
            }
        }, allocator, resources.projectionMaskCompiler(), resources.evaluationPolicy());
        this.originalPredicateMask = predicateMask;
        java.util.ArrayList<StaticPredicatePushdown> pushdowns = new java.util.ArrayList<>();
        if (policy.pushStaticLongRanges()) {
            staticLongRangeCandidates(evaluationPlan, predicateMask, primitiveRegistry).forEach(candidate ->
                    pushdowns.add(new StaticPredicatePushdown(
                            candidate.terms(),
                            source.pushStaticFilter(candidate.filter()))));
        }
        if (policy.pushStaticLongEquality()) {
            staticLongEqualityCandidates(evaluationPlan, predicateMask, primitiveRegistry).forEach(candidate ->
                    pushdowns.add(new StaticPredicatePushdown(
                            candidate.terms(),
                            source.pushStaticFilter(candidate.filter()))));
        }
        this.staticPredicatePushdowns = List.copyOf(pushdowns);
    }

    /**
     * Extracts an exact {@code BIGINT input = integral literal} predicate as a scan filter. The original filter stays
     * unless the source explicitly accepts complete semantic enforcement.
     */
    static Optional<DynamicFilter> staticLongEqualityFilter(EvaluationPlan plan, MaskExpression predicateMask, PrimitiveRegistry primitiveRegistry)
    {
        return staticLongEqualityCandidate(plan, predicateMask, primitiveRegistry).map(StaticFilterCandidate::filter);
    }

    private static List<StaticFilterCandidate> staticLongEqualityCandidates(
            EvaluationPlan plan,
            MaskExpression predicateMask,
            PrimitiveRegistry primitiveRegistry)
    {
        return conjunctiveTerms(predicateMask)
                .map(term -> staticLongEqualityCandidate(plan, term, primitiveRegistry).orElse(null))
                .filter(java.util.Objects::nonNull)
                .toList();
    }

    static List<DynamicFilter> staticLongEqualityFilters(
            EvaluationPlan plan,
            MaskExpression predicateMask,
            PrimitiveRegistry primitiveRegistry)
    {
        return staticLongEqualityCandidates(plan, predicateMask, primitiveRegistry).stream()
                .map(StaticFilterCandidate::filter)
                .toList();
    }

    private static Optional<StaticFilterCandidate> staticLongEqualityCandidate(
            EvaluationPlan plan,
            MaskExpression predicateMask,
            PrimitiveRegistry primitiveRegistry)
    {
        if (!(predicateMask instanceof ReferenceMask(Reference(Variable predicate, Stream stream))) || stream != Stream.VALUES) {
            return Optional.empty();
        }
        Assignment predicateAssignment = assignment(plan, predicate);
        if (predicateAssignment == null || predicateAssignment.mask() != AllMask.ALL
                || !(predicateAssignment.operation() instanceof Call call)) {
            return Optional.empty();
        }
        Optional<StaticLongEqualityProvider> provider = primitiveRegistry.capability(call, StaticLongEqualityProvider.class);
        if (provider.isEmpty()) {
            return Optional.empty();
        }
        return provider.orElseThrow()
                .staticLongEquality(new EvaluatorFunctionCallSite(call, plan, primitiveRegistry))
                .filter(equality -> equality.inputArgument() < call.arguments().size())
                .flatMap(equality -> dynamicFilter(call.arguments().get(equality.inputArgument()), equality.value()))
                .map(filter -> new StaticFilterCandidate(filter, List.of(predicateMask)));
    }

    private static Optional<DynamicFilter> dynamicFilter(Reference inputReference, long value)
    {
        if (!(inputReference instanceof Reference(Input(int input), Stream stream)) || stream != Stream.VALUES) {
            return Optional.empty();
        }
        return Optional.of(DynamicFilter.fromRange(input, value, value));
    }

    private static Assignment assignment(EvaluationPlan plan, Variable output)
    {
        for (Assignment assignment : plan.assignments()) {
            if (assignment.output().equals(output)) {
                return assignment;
            }
        }
        return null;
    }

    /**
     * Extracts inclusive long domains from registry-owned range metadata. The logical predicate remains unless the
     * source explicitly accepts complete semantic enforcement.
     */
    static List<DynamicFilter> staticLongRangeFilters(
            EvaluationPlan plan,
            MaskExpression predicateMask,
            PrimitiveRegistry primitiveRegistry)
    {
        return staticLongRangeCandidates(plan, predicateMask, primitiveRegistry).stream()
                .map(StaticFilterCandidate::filter)
                .toList();
    }

    private static List<StaticFilterCandidate> staticLongRangeCandidates(
            EvaluationPlan plan,
            MaskExpression predicateMask,
            PrimitiveRegistry primitiveRegistry)
    {
        return RangeConstraintLowerer.staticLongRanges(plan, primitiveRegistry, predicateMask).stream()
                .filter(range -> range.input().producer() instanceof Input)
                .filter(range -> range.input().stream() == Stream.VALUES)
                .filter(range -> range.lowerExclusive() != Long.MAX_VALUE)
                .filter(range -> range.upperExclusive() != Long.MIN_VALUE)
                .map(range -> new StaticFilterCandidate(
                        DynamicFilter.fromRange(
                                ((Input) range.input().producer()).index(),
                                range.lowerExclusive() + 1,
                                range.upperExclusive() - 1),
                        range.terms()))
                .toList();
    }

    private static java.util.stream.Stream<MaskExpression> conjunctiveTerms(MaskExpression expression)
    {
        if (expression instanceof org.weakref.nitro.operator.evaluator.ir.AndMask(List<MaskExpression> terms)) {
            return terms.stream().flatMap(FilterOperator::conjunctiveTerms);
        }
        return java.util.stream.Stream.of(expression);
    }

    @Override
    public int outputCount()
    {
        return source.outputCount();
    }

    @Override
    public Schema outputSchema()
    {
        return source.outputSchema();
    }

    @Override
    public boolean hasNext()
    {
        return source.hasNext();
    }

    @Override
    public Batch next()
    {
        Batch sourceBatch = source.next();
        BatchState batchState = new BatchState(sourceBatch);
        currentBatchState = batchState;
        Mask batchMask = allocator.copyMask(allocationContext, sourceBatch.borrowMask());
        MaskExpression predicateMask = effectivePredicateMask();
        if (predicateMask != AllMask.ALL) {
            batchMask = planEvaluator.evaluateInPlace(predicateMask, batchMask);
        }
        batchState.ownedMask(batchMask);
        source.constrain(batchMask);
        sourceBatch.constrain(batchMask);
        // The predicate result has been reduced to the owned output mask; no evaluator vector escapes this point.
        planEvaluator.resetForReuse();
        batchState.constrain(batchMask);

        return Batch.forwarding(batchMask, batchState, sourceBatch);
    }

    private MaskExpression effectivePredicateMask()
    {
        if (effectivePredicateMask != null) {
            return effectivePredicateMask;
        }
        Set<MaskExpression> enforcedTerms = staticPredicatePushdowns.stream()
                .filter(pushdown -> pushdown.enforcement().enforced())
                .flatMap(pushdown -> pushdown.terms().stream())
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
        MaskExpression residual = removeEnforcedConjuncts(originalPredicateMask, enforcedTerms);
        effectivePredicateMask = RangeConstraintLowerer.lower(
                evaluationPlan,
                primitiveRegistry,
                residual,
                policy.fuseConstantRanges());
        return effectivePredicateMask;
    }

    private static MaskExpression removeEnforcedConjuncts(MaskExpression expression, Set<MaskExpression> enforcedTerms)
    {
        if (enforcedTerms.contains(expression)) {
            return AllMask.ALL;
        }
        if (!(expression instanceof org.weakref.nitro.operator.evaluator.ir.AndMask(List<MaskExpression> terms))) {
            return expression;
        }
        List<MaskExpression> residual = terms.stream()
                .map(term -> removeEnforcedConjuncts(term, enforcedTerms))
                .filter(term -> term != AllMask.ALL)
                .toList();
        return switch (residual.size()) {
            case 0 -> AllMask.ALL;
            case 1 -> residual.getFirst();
            default -> new org.weakref.nitro.operator.evaluator.ir.AndMask(residual);
        };
    }

    private record StaticFilterCandidate(DynamicFilter filter, List<MaskExpression> terms) {}

    private record StaticPredicatePushdown(
            List<MaskExpression> terms,
            StaticFilterEnforcement enforcement) {}

    @Override
    public void constrain(Mask mask)
    {
        source.constrain(mask);
        if (currentBatchState != null) {
            currentBatchState.constrain(mask);
        }
    }

    @Override
    public void pushDynamicFilter(org.weakref.nitro.operator.DynamicFilter filter)
    {
        // A filter only narrows rows; it leaves columns unchanged, so forward a pushed dynamic filter to the source.
        source.pushDynamicFilter(filter);
    }

    @Override
    public boolean supportsDynamicFilterPushdown(int column)
    {
        return source.supportsDynamicFilterPushdown(column);
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        return source.supportsRetainedBatches();
    }

    @Override
    public boolean supportsOpenBatchHasNext()
    {
        return source.supportsOpenBatchHasNext();
    }

    @Override
    public boolean supportsConstrainedReborrow()
    {
        // Filtering only narrows the active mask; re-borrow safety is whatever the source provides.
        return source.supportsConstrainedReborrow();
    }

    @Override
    public void close()
    {
        if (currentBatchState != null) {
            currentBatchState.sourceBatch().close();
            currentBatchState = null;
        }
        source.close();
        planEvaluator.resetForReuse();
        allocator.release(allocationContext);
    }

    private final class BatchState
            implements Batch.Lifecycle
    {
        private final Batch sourceBatch;
        private Mask ownedMask;

        private BatchState(Batch sourceBatch)
        {
            this.sourceBatch = sourceBatch;
        }

        private Batch sourceBatch()
        {
            return sourceBatch;
        }

        private void ownedMask(Mask ownedMask)
        {
            this.ownedMask = ownedMask;
        }

        @Override
        public void constrain(Mask mask)
        {
            sourceBatch.constrain(mask);
        }

        @Override
        public Mask takeMask(Mask mask)
        {
            if (policy.recycleOutputMasks() && mask == ownedMask) {
                allocator.transfer(allocationContext, mask);
            }
            return mask;
        }

        @Override
        public void releaseMask(Mask mask)
        {
            if (policy.recycleOutputMasks() && mask == ownedMask) {
                allocator.release(allocationContext, mask);
            }
        }

        @Override
        public void close()
        {
            if (currentBatchState == this) {
                currentBatchState = null;
            }
            sourceBatch.close();
        }
    }
}
