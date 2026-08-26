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

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.weakref.nitro.core.execution.ExecutionDiagnostics;
import org.weakref.nitro.core.function.mask.StaticLongEqualityProvider;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.ValueDemand;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.EvaluatorFunctionCallSite;
import org.weakref.nitro.operator.evaluator.PlanEvaluator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.InputDependencies;
import org.weakref.nitro.operator.evaluator.ir.LongDomainMask;
import org.weakref.nitro.operator.evaluator.ir.MaskExpression;
import org.weakref.nitro.operator.evaluator.ir.MaskExpressionResolver;
import org.weakref.nitro.operator.evaluator.ir.RangeConstraintLowerer;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.ReferenceMask;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class FilterOperator
        implements Operator
{
    public static final String PLANNED_ASSIGNMENTS = "nitro.filter.planned-assignments";
    public static final String PLANNED_SOURCE_MASK_OPTIMIZATIONS = "nitro.filter.planned-source-mask-optimizations";
    public static final String PLANNED_PREBOUND_MASKS = "nitro.filter.planned-prebound-masks";
    public static final String PLANNED_COMPILED_MASKS = "nitro.filter.planned-compiled-masks";
    public static final String INPUT_POSITIONS = "nitro.filter.input-positions";
    public static final String OUTPUT_POSITIONS = "nitro.filter.output-positions";
    public static final String SOURCE_MASK_SUCCESSES = "nitro.filter.source-mask-successes";
    public static final String COMPILED_MASK_ATTEMPTS = "nitro.filter.compiled-mask-attempts";
    public static final String COMPILED_MASK_SUCCESSES = "nitro.filter.compiled-mask-successes";
    public static final String COMPILED_MASK_FALLBACKS = "nitro.filter.compiled-mask-fallbacks";
    public static final String DIRECT_PREBOUND_MASK_SUCCESSES = "nitro.filter.direct-prebound-mask-successes";
    public static final String PRIMITIVE_MASK_ATTEMPTS = "nitro.filter.primitive-mask-attempts";
    public static final String PRIMITIVE_MASK_SUCCESSES = "nitro.filter.primitive-mask-successes";
    public static final String PRIMITIVE_MASK_FALLBACKS = "nitro.filter.primitive-mask-fallbacks";
    public static final String MATERIALIZED_MASK_FALLBACKS = "nitro.filter.materialized-mask-fallbacks";

    private final Allocator.Context allocationContext = new Allocator.Context("FilterOperator");

    private final Operator source;
    private final Allocator allocator;
    private final PlanEvaluator planEvaluator;
    private final EvaluationPlan evaluationPlan;
    private final PrimitiveRegistry primitiveRegistry;
    private final MaskExpression originalPredicateMask;
    private final List<StaticPredicatePushdown> staticPredicatePushdowns;
    private final FilterOperatorPolicy policy;
    private final ExecutionDiagnostics diagnostics;

    private BatchState currentBatchState;
    private MaskExpression effectivePredicateMask;
    private long inputPositions;
    private long outputPositions;
    private boolean diagnosticsReported;

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
            Reference predicateReference,
            Allocator allocator,
            FilterOperatorResources resources,
            ExecutionDiagnostics diagnostics)
    {
        this(
                source,
                evaluationPlan,
                primitiveRegistry,
                MaskExpressionResolver.resolve(evaluationPlan, predicateReference),
                allocator,
                resources,
                diagnostics);
    }

    public FilterOperator(
            Operator source,
            EvaluationPlan evaluationPlan,
            PrimitiveRegistry primitiveRegistry,
            MaskExpression predicateMask,
            Allocator allocator,
            FilterOperatorResources resources)
    {
        this(source, evaluationPlan, primitiveRegistry, predicateMask, allocator, resources, (_, _) -> {});
    }

    public FilterOperator(
            Operator source,
            EvaluationPlan evaluationPlan,
            PrimitiveRegistry primitiveRegistry,
            MaskExpression predicateMask,
            Allocator allocator,
            FilterOperatorResources resources,
            ExecutionDiagnostics diagnostics)
    {
        this.source = source;
        this.allocator = allocator;
        this.evaluationPlan = evaluationPlan;
        this.primitiveRegistry = primitiveRegistry;
        this.policy = resources.policy();
        this.diagnostics = requireNonNull(diagnostics, "diagnostics is null");
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
                            source.pushStaticFilter(candidate.filter()),
                            null)));
        }
        if (policy.pushStaticLongEquality()) {
            staticLongEqualityDomainCandidates(
                    evaluationPlan,
                    predicateMask,
                    primitiveRegistry,
                    resources.dynamicFilterPolicy()).forEach(candidate ->
                    pushdowns.add(new StaticPredicatePushdown(
                            candidate.terms(),
                            source.pushStaticFilter(candidate.filter()),
                            new LongDomainMask(
                                    new Reference(new Input(candidate.filter().column()), Stream.VALUES),
                                    candidate.filter()))));
            staticLongEqualityCandidates(evaluationPlan, predicateMask, primitiveRegistry).forEach(candidate ->
                    pushdowns.add(new StaticPredicatePushdown(
                            candidate.terms(),
                            source.pushStaticFilter(candidate.filter()),
                            null)));
        }
        this.staticPredicatePushdowns = List.copyOf(pushdowns);
        PlanEvaluator.MaskExecutionDiagnostics evaluatorDiagnostics = planEvaluator.maskExecutionDiagnostics();
        diagnostics.record(PLANNED_ASSIGNMENTS, evaluatorDiagnostics.plannedAssignments());
        diagnostics.record(PLANNED_SOURCE_MASK_OPTIMIZATIONS, evaluatorDiagnostics.sourceMaskOptimizations());
        diagnostics.record(PLANNED_PREBOUND_MASKS, evaluatorDiagnostics.preboundMasks());
        diagnostics.record(PLANNED_COMPILED_MASKS, evaluatorDiagnostics.compiledPreboundMasks());
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

    /**
     * Extracts each disjunction of exact long equalities over one input as one source domain. Function identity and
     * argument order come from registry metadata; the operator only combines compatible semantic capabilities.
     */
    static List<DynamicFilter> staticLongEqualityDomains(
            EvaluationPlan plan,
            MaskExpression predicateMask,
            PrimitiveRegistry primitiveRegistry,
            DynamicFilterPolicy dynamicFilterPolicy)
    {
        return staticLongEqualityDomainCandidates(plan, predicateMask, primitiveRegistry, dynamicFilterPolicy).stream()
                .map(StaticFilterCandidate::filter)
                .toList();
    }

    private static List<StaticFilterCandidate> staticLongEqualityDomainCandidates(
            EvaluationPlan plan,
            MaskExpression predicateMask,
            PrimitiveRegistry primitiveRegistry,
            DynamicFilterPolicy dynamicFilterPolicy)
    {
        return conjunctiveTerms(predicateMask)
                .map(term -> staticLongEqualityDomainCandidate(plan, term, primitiveRegistry, dynamicFilterPolicy).orElse(null))
                .filter(java.util.Objects::nonNull)
                .toList();
    }

    private static Optional<StaticFilterCandidate> staticLongEqualityDomainCandidate(
            EvaluationPlan plan,
            MaskExpression predicateMask,
            PrimitiveRegistry primitiveRegistry,
            DynamicFilterPolicy dynamicFilterPolicy)
    {
        if (!(predicateMask instanceof org.weakref.nitro.operator.evaluator.ir.OrMask(List<MaskExpression> terms))) {
            return Optional.empty();
        }
        int input = -1;
        LongOpenHashSet values = new LongOpenHashSet(terms.size());
        for (MaskExpression term : terms) {
            StaticLongEquality equality = staticLongEquality(plan, term, primitiveRegistry).orElse(null);
            if (equality == null || (input >= 0 && input != equality.input())) {
                return Optional.empty();
            }
            input = equality.input();
            values.add(equality.value());
        }
        return Optional.of(new StaticFilterCandidate(
                DynamicFilter.fromValues(input, values, dynamicFilterPolicy),
                List.of(predicateMask)));
    }

    private static Optional<StaticFilterCandidate> staticLongEqualityCandidate(
            EvaluationPlan plan,
            MaskExpression predicateMask,
            PrimitiveRegistry primitiveRegistry)
    {
        return staticLongEquality(plan, predicateMask, primitiveRegistry)
                .map(equality -> new StaticFilterCandidate(
                        DynamicFilter.fromRange(equality.input(), equality.value(), equality.value()),
                        List.of(predicateMask)));
    }

    private static Optional<StaticLongEquality> staticLongEquality(
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
                .flatMap(equality -> inputIndex(call.arguments().get(equality.inputArgument()))
                        .map(input -> new StaticLongEquality(input, equality.value())));
    }

    private static Optional<Integer> inputIndex(Reference inputReference)
    {
        if (!(inputReference instanceof Reference(Input(int input), Stream stream)) || stream != Stream.VALUES) {
            return Optional.empty();
        }
        return Optional.of(input);
    }

    private static Optional<DynamicFilter> dynamicFilter(Reference inputReference, long value)
    {
        return inputIndex(inputReference).map(input -> DynamicFilter.fromRange(input, value, value));
    }

    private record StaticLongEquality(int input, long value) {}

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
        inputPositions += batchMask.count();
        MaskExpression predicateMask = effectivePredicateMask();
        if (predicateMask != AllMask.ALL) {
            batchMask = planEvaluator.evaluateInPlace(predicateMask, batchMask);
        }
        batchState.ownedMask(batchMask);
        outputPositions += batchMask.count();
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
        residual = replaceResidualDomains(residual);
        effectivePredicateMask = RangeConstraintLowerer.lower(
                evaluationPlan,
                primitiveRegistry,
                residual,
                policy.fuseConstantRanges());
        return effectivePredicateMask;
    }

    private MaskExpression replaceResidualDomains(MaskExpression expression)
    {
        for (StaticPredicatePushdown pushdown : staticPredicatePushdowns) {
            if (!pushdown.enforcement().enforced() &&
                    pushdown.residualReplacement() != null &&
                    pushdown.terms().contains(expression)) {
                return pushdown.residualReplacement();
            }
        }
        if (!(expression instanceof org.weakref.nitro.operator.evaluator.ir.AndMask(List<MaskExpression> terms))) {
            return expression;
        }
        return new org.weakref.nitro.operator.evaluator.ir.AndMask(
                terms.stream().map(this::replaceResidualDomains).toList());
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
            StaticFilterEnforcement enforcement,
            MaskExpression residualReplacement) {}

    @Override
    public void constrain(Mask mask)
    {
        source.constrain(mask);
        if (currentBatchState != null) {
            currentBatchState.constrain(mask);
        }
    }

    @Override
    public Optional<Map<Integer, ValueDemand>> sourceOutputDemand(Map<Integer, ValueDemand> demandedOutputs)
    {
        requireNonNull(demandedOutputs, "demandedOutputs is null");
        java.util.HashMap<Integer, ValueDemand> required = new java.util.HashMap<>(demandedOutputs);
        InputDependencies.valueDemands(evaluationPlan, primitiveRegistry, effectivePredicateMask())
                .forEach((input, demand) -> required.merge(input, demand, ValueDemand::merge));
        return source.sourceOutputDemand(Map.copyOf(required));
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
        try {
            if (currentBatchState != null) {
                currentBatchState.sourceBatch().close();
                currentBatchState = null;
            }
            source.close();
            planEvaluator.resetForReuse();
            planEvaluator.close();
            allocator.release(allocationContext);
        }
        finally {
            reportDiagnostics();
        }
    }

    private void reportDiagnostics()
    {
        if (diagnosticsReported) {
            return;
        }
        diagnosticsReported = true;
        PlanEvaluator.MaskExecutionDiagnostics evaluatorDiagnostics = planEvaluator.maskExecutionDiagnostics();
        diagnostics.record(INPUT_POSITIONS, inputPositions);
        diagnostics.record(OUTPUT_POSITIONS, outputPositions);
        diagnostics.record(SOURCE_MASK_SUCCESSES, evaluatorDiagnostics.sourceMaskSuccesses());
        diagnostics.record(COMPILED_MASK_ATTEMPTS, evaluatorDiagnostics.compiledMaskAttempts());
        diagnostics.record(COMPILED_MASK_SUCCESSES, evaluatorDiagnostics.compiledMaskSuccesses());
        diagnostics.record(COMPILED_MASK_FALLBACKS, evaluatorDiagnostics.compiledMaskFallbacks());
        diagnostics.record(DIRECT_PREBOUND_MASK_SUCCESSES, evaluatorDiagnostics.directPreboundMaskSuccesses());
        diagnostics.record(PRIMITIVE_MASK_ATTEMPTS, evaluatorDiagnostics.primitiveMaskAttempts());
        diagnostics.record(PRIMITIVE_MASK_SUCCESSES, evaluatorDiagnostics.primitiveMaskSuccesses());
        diagnostics.record(PRIMITIVE_MASK_FALLBACKS, evaluatorDiagnostics.primitiveMaskFallbacks());
        diagnostics.record(MATERIALIZED_MASK_FALLBACKS, evaluatorDiagnostics.materializedMaskFallbacks());
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
