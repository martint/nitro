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

import org.weakref.nitro.data.Stream;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.RangeBoundProvider;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Converts function-owned range metadata into structural physical mask nodes before execution.
 */
public final class RangeConstraintLowerer
{
    private static final boolean FUSE_RANGES =
            Boolean.parseBoolean(System.getProperty("nitro.expression.fuseLongConstantRanges", "true"));

    private final Map<Variable, Assignment> assignments;
    private final PrimitiveRegistry registry;

    private RangeConstraintLowerer(EvaluationPlan plan, PrimitiveRegistry registry)
    {
        this.assignments = plan.assignments().stream()
                .collect(java.util.stream.Collectors.toMap(Assignment::output, Function.identity()));
        this.registry = registry;
    }

    public static MaskExpression lower(EvaluationPlan plan, PrimitiveRegistry registry, MaskExpression expression)
    {
        return new RangeConstraintLowerer(plan, registry).lower(expression);
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
        if (!FUSE_RANGES) {
            return new AndMask(terms);
        }
        for (int lowerIndex = 0; lowerIndex < terms.size(); lowerIndex++) {
            RangeBoundProvider.RangeBound lower = rangeBound(terms.get(lowerIndex));
            if (lower == null || lower.position() != RangeConstraint.Position.LOWER_EXCLUSIVE) {
                continue;
            }
            for (int upperIndex = 0; upperIndex < terms.size(); upperIndex++) {
                RangeBoundProvider.RangeBound upper = rangeBound(terms.get(upperIndex));
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

    private RangeBoundProvider.RangeBound rangeBound(MaskExpression expression)
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
        return registry.capability(call, RangeBoundProvider.class)
                .flatMap(provider -> provider.rangeBound(call.arguments(), this::literal))
                .orElse(null);
    }

    private Optional<Object> literal(Reference reference)
    {
        if (reference.stream() != Stream.VALUES || !(reference.producer() instanceof Variable variable)) {
            return Optional.empty();
        }
        Assignment assignment = assignments.get(variable);
        if (assignment == null || assignment.mask() != AllMask.ALL ||
                !(assignment.operation() instanceof Literal(Object value))) {
            return Optional.empty();
        }
        return Optional.of(value);
    }
}
