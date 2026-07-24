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
package org.weakref.nitro;

import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.AndMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.Copy;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.MaskExpression;
import org.weakref.nitro.operator.evaluator.ir.NotMask;
import org.weakref.nitro.operator.evaluator.ir.OrMask;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.ReferenceMask;
import org.weakref.nitro.operator.evaluator.ir.Stream;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Quarantines name-based logical translation in legacy benchmark builders.
 *
 * <p>Production Nitro consumes explicit structural masks and never recognizes these function names. New harness code
 * must construct {@link AndMask}, {@link OrMask}, and {@link NotMask} directly.
 */
public final class LegacyLogicalMaskAdapter
{
    private LegacyLogicalMaskAdapter() {}

    public static MaskExpression resolve(EvaluationPlan plan, MaskExpression expression)
    {
        return new Resolver(plan).resolveExpression(expression);
    }

    private static final class Resolver
    {
        private final Map<Variable, Assignment> assignments;
        private final Map<Reference, MaskExpression> maskPlans;
        private final Set<Reference> resolvingMaskPlans = new HashSet<>();

        private Resolver(EvaluationPlan plan)
        {
            assignments = plan.assignments().stream()
                    .collect(java.util.stream.Collectors.toMap(Assignment::output, Function.identity()));
            maskPlans = plan.maskPlans();
        }

        private MaskExpression resolve(Reference reference)
        {
            checkArgument(reference.stream() == Stream.VALUES, "Mask references must use VALUES: %s", reference);
            MaskExpression maskPlan = maskPlans.get(reference);
            if (maskPlan != null && resolvingMaskPlans.add(reference)) {
                try {
                    return resolveExpression(maskPlan);
                }
                finally {
                    resolvingMaskPlans.remove(reference);
                }
            }
            return switch (reference.producer()) {
                case Input _ -> new ReferenceMask(reference);
                case Variable variable -> resolveVariable(variable, reference);
            };
        }

        private MaskExpression resolveVariable(Variable variable, Reference reference)
        {
            Assignment assignment = assignments.get(variable);
            if (assignment == null || assignment.mask() != AllMask.ALL) {
                return new ReferenceMask(reference);
            }
            return switch (assignment.operation()) {
                case Call(String name, List<Reference> arguments, _) when name.equals("and") ->
                        new AndMask(arguments.stream().map(this::resolve).toList());
                case Call(String name, List<Reference> arguments, _) when name.equals("or") ->
                        new OrMask(arguments.stream().map(this::resolve).toList());
                case Call(String name, List<Reference> arguments, _) when name.equals("not") -> {
                    checkArgument(arguments.size() == 1, "not requires 1 argument");
                    yield new NotMask(resolve(arguments.getFirst()));
                }
                case Copy(Reference source) -> resolve(source);
                default -> new ReferenceMask(reference);
            };
        }

        private MaskExpression resolveExpression(MaskExpression expression)
        {
            return switch (expression) {
                case AllMask _ -> expression;
                case ReferenceMask(Reference reference) -> resolve(reference);
                case NotMask(MaskExpression source) -> new NotMask(resolveExpression(source));
                case AndMask(List<MaskExpression> terms) -> new AndMask(terms.stream()
                        .map(this::resolveExpression)
                        .toList());
                case OrMask(List<MaskExpression> terms) -> new OrMask(terms.stream()
                        .map(this::resolveExpression)
                        .toList());
            };
        }
    }
}
