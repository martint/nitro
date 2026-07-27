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
package org.weakref.nitro.operator.evaluator.ir;

import org.weakref.nitro.core.function.mask.RangeBoundProvider;
import org.weakref.nitro.core.function.mask.RangeConstraint;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.operator.evaluator.EvaluatorFunctionCallSite;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Converts function-owned range metadata into structural physical mask nodes before execution.
 */
public final class RangeConstraintLowerer
{
    private final Map<Variable, Assignment> assignments;
    private final PrimitiveRegistry registry;
    private final boolean fuseRanges;

    private RangeConstraintLowerer(EvaluationPlan plan, PrimitiveRegistry registry, boolean fuseRanges)
    {
        this.assignments = plan.assignments().stream()
                .collect(java.util.stream.Collectors.toMap(Assignment::output, Function.identity()));
        this.registry = registry;
        this.fuseRanges = fuseRanges;
    }

    public static MaskExpression lower(
            EvaluationPlan plan,
            PrimitiveRegistry registry,
            MaskExpression expression,
            boolean fuseRanges)
    {
        return new RangeConstraintLowerer(plan, registry, fuseRanges).lower(expression);
    }

    private MaskExpression lower(MaskExpression expression)
    {
        return switch (expression) {
            case AllMask _, RangeConstrainedAndMask _, ReferenceMask _ -> expression;
            case NotMask(MaskExpression source) -> new NotMask(lower(source));
            case AndMask(List<MaskExpression> terms) -> lowerAnd(terms);
            case OrMask(List<MaskExpression> terms) -> new OrMask(terms.stream().map(this::lower).toList());
        };
    }

    private MaskExpression lowerAnd(List<MaskExpression> sourceTerms)
    {
        List<MaskExpression> terms = sourceTerms.stream().map(this::lower).toList();
        if (!fuseRanges) {
            return new AndMask(terms);
        }
        for (int lowerIndex = 0; lowerIndex < terms.size(); lowerIndex++) {
            BoundRange lower = rangeBound(terms.get(lowerIndex));
            if (lower == null || lower.position() != RangeConstraint.Position.LOWER_EXCLUSIVE) {
                continue;
            }
            for (int upperIndex = 0; upperIndex < terms.size(); upperIndex++) {
                BoundRange upper = rangeBound(terms.get(upperIndex));
                if (upper == null ||
                        upper.position() != RangeConstraint.Position.UPPER_EXCLUSIVE ||
                        !upper.input().equals(lower.input()) ||
                        upper.kernel() != lower.kernel()) {
                    continue;
                }
                java.util.ArrayList<MaskExpression> remaining = new java.util.ArrayList<>(terms.size() - 2);
                for (int index = 0; index < terms.size(); index++) {
                    if (index != lowerIndex && index != upperIndex) {
                        remaining.add(terms.get(index));
                    }
                }
                return new RangeConstrainedAndMask(
                        lower.input(),
                        lower.bound(),
                        upper.bound(),
                        lower.kernel(),
                        remaining,
                        new AndMask(terms));
            }
        }
        return new AndMask(terms);
    }

    private BoundRange rangeBound(MaskExpression expression)
    {
        if (!(expression instanceof ReferenceMask(Reference reference))) {
            return null;
        }
        if (reference.stream() != Stream.VALUES || !(reference.producer() instanceof Variable variable)) {
            return null;
        }
        Assignment assignment = assignments.get(variable);
        if (assignment == null || assignment.mask() != AllMask.ALL || !(assignment.operation() instanceof Call call)) {
            return null;
        }
        RangeBoundProvider.RangeBound bound = registry.capability(call, RangeBoundProvider.class)
                .flatMap(provider -> provider.rangeBound(new EvaluatorFunctionCallSite(call, assignments, registry)))
                .orElse(null);
        if (bound == null || bound.inputArgument() >= call.arguments().size()) {
            return null;
        }
        return new BoundRange(
                call.arguments().get(bound.inputArgument()),
                bound.bound(),
                bound.position(),
                bound.kernel());
    }

    private record BoundRange(
            Reference input,
            Object bound,
            RangeConstraint.Position position,
            RangeConstraint.Kernel kernel) {}
}
