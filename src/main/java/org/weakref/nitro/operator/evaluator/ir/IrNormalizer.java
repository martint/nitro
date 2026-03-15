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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class IrNormalizer
{
    private static final IrNormalizer STANDARD = new IrNormalizer(List.of(
            new IfNormalizationRule(),
            new CoalesceNormalizationRule()));

    private final List<IrNormalizationRule> rules;

    public IrNormalizer(List<IrNormalizationRule> rules)
    {
        this.rules = List.copyOf(rules);
    }

    public static EvaluationPlan normalize(EvaluationPlan plan)
    {
        return STANDARD.normalizePlan(plan);
    }

    public EvaluationPlan normalizePlan(EvaluationPlan plan)
    {
        Context context = new Context(nextVariableId(plan.assignments()));
        for (Assignment assignment : plan.assignments()) {
            normalizeAssignment(assignment, context);
        }
        Map<Reference, MaskExpression> normalizedMaskPlans = plan.maskPlans().entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> normalizeMaskExpression(entry.getValue())));
        EvaluationPlan normalizedPlan = new EvaluationPlan(context.assignments(), plan.outputs(), plan.streamPlans(), normalizedMaskPlans);
        return resolveMaskReferences(normalizedPlan);
    }

    private void normalizeAssignment(Assignment assignment, Context context)
    {
        for (IrNormalizationRule rule : rules) {
            if (rule.matches(assignment)) {
                rule.apply(assignment, context);
                return;
            }
        }
        context.emit(assignment);
    }

    public static final class Context
    {
        private final VariableAllocator variableAllocator;
        private final List<Assignment> assignments = new ArrayList<>();

        private Context(int nextVariableId)
        {
            variableAllocator = new VariableAllocator(nextVariableId);
        }

        public void emit(Assignment assignment)
        {
            assignments.add(normalizeMasks(assignment));
        }

        public Variable nextVariable()
        {
            return variableAllocator.next();
        }

        private List<Assignment> assignments()
        {
            return assignments;
        }

        private static Assignment normalizeMasks(Assignment assignment)
        {
            MaskExpression mask = normalizeMaskExpression(assignment.mask());
            Operation operation = switch (assignment.operation()) {
                case Merge merge -> new Merge(normalizeMaskExpression(merge.condition()), merge.whenTrue(), merge.whenFalse());
                default -> assignment.operation();
            };
            return new Assignment(assignment.output(), operation, mask);
        }
    }

    private static int nextVariableId(List<Assignment> assignments)
    {
        return assignments.stream()
                .mapToInt(assignment -> assignment.output().id())
                .max()
                .orElse(-1) + 1;
    }

    private static EvaluationPlan resolveMaskReferences(EvaluationPlan plan)
    {
        List<Assignment> resolvedAssignments = plan.assignments().stream()
                .map(assignment -> {
                    MaskExpression mask = MaskExpressionResolver.resolve(plan, assignment.mask());
                    Operation operation = switch (assignment.operation()) {
                        case Merge merge -> new Merge(MaskExpressionResolver.resolve(plan, merge.condition()), merge.whenTrue(), merge.whenFalse());
                        default -> assignment.operation();
                    };
                    return new Assignment(assignment.output(), operation, mask);
                })
                .toList();
        Map<Reference, MaskExpression> resolvedMaskPlans = plan.maskPlans().entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> MaskExpressionResolver.resolve(plan, entry.getValue())));
        return new EvaluationPlan(resolvedAssignments, plan.outputs(), plan.streamPlans(), resolvedMaskPlans);
    }

    private static MaskExpression normalizeMaskExpression(MaskExpression expression)
    {
        return switch (expression) {
            case AllMask _, ReferenceMask _ -> expression;
            case NotMask(MaskExpression source) -> new NotMask(normalizeMaskExpression(source));
            case AndMask(List<MaskExpression> terms) -> normalizeAnd(terms);
            case OrMask(List<MaskExpression> terms) -> normalizeOr(terms);
        };
    }

    private static MaskExpression normalizeAnd(List<MaskExpression> terms)
    {
        ArrayList<MaskExpression> flattened = new ArrayList<>();
        for (MaskExpression term : terms) {
            MaskExpression normalized = normalizeMaskExpression(term);
            switch (normalized) {
                case AndMask(List<MaskExpression> nested) -> flattened.addAll(nested);
                default -> flattened.add(normalized);
            }
        }
        return flattened.size() == 1 ? flattened.getFirst() : new AndMask(flattened);
    }

    private static MaskExpression normalizeOr(List<MaskExpression> terms)
    {
        ArrayList<MaskExpression> flattened = new ArrayList<>();
        for (MaskExpression term : terms) {
            MaskExpression normalized = normalizeMaskExpression(term);
            switch (normalized) {
                case OrMask(List<MaskExpression> nested) -> flattened.addAll(nested);
                default -> flattened.add(normalized);
            }
        }
        return flattened.size() == 1 ? flattened.getFirst() : new OrMask(flattened);
    }

    private static final class VariableAllocator
    {
        private int nextId;

        private VariableAllocator(int nextId)
        {
            this.nextId = nextId;
        }

        private Variable next()
        {
            return new Variable(nextId++);
        }
    }
}
