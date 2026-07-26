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
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.PlanEvaluator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.StaticLongEqualityProvider;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Literal;
import org.weakref.nitro.operator.evaluator.ir.MaskExpression;
import org.weakref.nitro.operator.evaluator.ir.MaskExpressionResolver;
import org.weakref.nitro.operator.evaluator.ir.RangeConstraintLowerer;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.ReferenceMask;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.util.Optional;
import java.util.OptionalLong;

public class FilterOperator
        implements Operator
{
    private final Allocator.Context allocationContext = new Allocator.Context("FilterOperator");

    private final Operator source;
    private final Allocator allocator;
    private final PlanEvaluator planEvaluator;
    private final MaskExpression predicateMask;
    private final FilterOperatorPolicy policy;

    private BatchState currentBatchState;

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
        }, allocator, resources.projectionMaskCompiler());
        this.predicateMask = RangeConstraintLowerer.lower(evaluationPlan, primitiveRegistry, predicateMask);
        if (policy.pushStaticLongEquality()) {
            staticLongEqualityFilter(evaluationPlan, predicateMask, primitiveRegistry).ifPresent(source::pushDynamicFilter);
        }
    }

    /**
     * Extracts an exact {@code BIGINT input = integral literal} predicate as a scan filter. The original filter stays
     * in this operator, so this is a conservative physical pushdown rather than a semantic rewrite.
     */
    static Optional<DynamicFilter> staticLongEqualityFilter(EvaluationPlan plan, MaskExpression predicateMask, PrimitiveRegistry primitiveRegistry)
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
                .staticLongEquality(call.arguments(), reference -> literalLong(plan, reference))
                .flatMap(equality -> dynamicFilter(equality.input(), equality.value()));
    }

    private static OptionalLong literalLong(EvaluationPlan plan, Reference reference)
    {
        if (!(reference instanceof Reference(Variable literal, Stream stream)) || stream != Stream.VALUES) {
            return OptionalLong.empty();
        }
        Assignment literalAssignment = assignment(plan, literal);
        if (literalAssignment == null || literalAssignment.mask() != AllMask.ALL
                || !(literalAssignment.operation() instanceof Literal(Long value))) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(value);
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
        batchMask = planEvaluator.evaluateInPlace(predicateMask, batchMask);
        batchState.ownedMask(batchMask);
        source.constrain(batchMask);
        sourceBatch.constrain(batchMask);
        // The predicate result has been reduced to the owned output mask; no evaluator vector escapes this point.
        planEvaluator.resetForReuse();
        batchState.constrain(batchMask);

        return Batch.forwarding(batchMask, batchState, sourceBatch);
    }

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
