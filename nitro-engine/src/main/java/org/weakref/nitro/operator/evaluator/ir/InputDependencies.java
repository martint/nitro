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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/// Finds the leaf input channels transitively needed by selected plan references and masks.
public final class InputDependencies
{
    private InputDependencies() {}

    public static Set<Integer> inputs(EvaluationPlan plan, Collection<Reference> roots)
    {
        Visitor visitor = new Visitor(plan);
        roots.forEach(visitor::reference);
        return Set.copyOf(visitor.inputs);
    }

    public static Set<Integer> inputs(EvaluationPlan plan, MaskExpression root)
    {
        Visitor visitor = new Visitor(plan);
        visitor.mask(root);
        return Set.copyOf(visitor.inputs);
    }

    private static final class Visitor
    {
        private final EvaluationPlan plan;
        private final Map<Variable, Assignment> assignments = new HashMap<>();
        private final Set<Reference> visitedReferences = new HashSet<>();
        private final Set<MaskExpression> visitedMasks = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        private final Set<Integer> inputs = new HashSet<>();

        private Visitor(EvaluationPlan plan)
        {
            this.plan = requireNonNull(plan, "plan is null");
            for (Assignment assignment : plan.assignments()) {
                assignments.put(assignment.output(), assignment);
            }
        }

        private void reference(Reference reference)
        {
            if (!visitedReferences.add(requireNonNull(reference, "reference is null"))) {
                return;
            }
            switch (reference.producer()) {
                case Input(int input) -> inputs.add(input);
                case Variable variable -> {
                    Assignment assignment = assignments.get(variable);
                    if (assignment == null) {
                        throw new IllegalArgumentException("Missing assignment for " + variable);
                    }
                    operation(assignment.operation());
                    mask(assignment.mask());
                }
            }
            MaskExpression referenceMask = plan.maskPlans().get(reference);
            if (referenceMask != null) {
                mask(referenceMask);
            }
        }

        private void operation(Operation operation)
        {
            switch (operation) {
                case Call call -> call.arguments().forEach(this::reference);
                case Coalesce coalesce -> {
                    reference(coalesce.first());
                    reference(coalesce.second());
                }
                case Conditional conditional -> {
                    reference(conditional.condition());
                    reference(conditional.whenTrue());
                    reference(conditional.whenFalse());
                }
                case Construct construct -> construct.arguments().forEach(this::reference);
                case Copy copy -> reference(copy.source());
                case Literal _ -> {}
                case Merge merge -> {
                    mask(merge.condition());
                    reference(merge.whenTrue());
                    reference(merge.whenFalse());
                }
                case Sequence sequence -> {
                    reference(sequence.first());
                    reference(sequence.result());
                }
                case StructField structField -> reference(structField.source());
            }
        }

        private void mask(MaskExpression expression)
        {
            if (!visitedMasks.add(requireNonNull(expression, "expression is null"))) {
                return;
            }
            switch (expression) {
                case AllMask _ -> {}
                case AndMask and -> and.terms().forEach(this::mask);
                case LongDomainMask domain -> reference(domain.input());
                case NotMask not -> mask(not.source());
                case OrMask or -> or.terms().forEach(this::mask);
                case RangeConstrainedAndMask range -> {
                    reference(range.input());
                    range.remainingTerms().forEach(this::mask);
                }
                case ReferenceMask referenceMask -> reference(referenceMask.reference());
            }
        }
    }
}
