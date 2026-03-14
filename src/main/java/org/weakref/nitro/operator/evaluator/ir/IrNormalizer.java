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
        return new EvaluationPlan(context.assignments(), plan.outputs(), plan.streamPlans());
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
            assignments.add(assignment);
        }

        public Variable nextVariable()
        {
            return variableAllocator.next();
        }

        private List<Assignment> assignments()
        {
            return assignments;
        }
    }

    private static int nextVariableId(List<Assignment> assignments)
    {
        return assignments.stream()
                .mapToInt(assignment -> assignment.output().id())
                .max()
                .orElse(-1) + 1;
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
