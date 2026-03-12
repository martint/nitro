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

import static com.google.common.base.Preconditions.checkArgument;

public final class IrNormalizer
{
    private IrNormalizer() {}

    public static EvaluationPlan normalize(EvaluationPlan plan)
    {
        VariableAllocator variableAllocator = new VariableAllocator(nextVariableId(plan.assignments()));
        List<Assignment> assignments = new ArrayList<>();

        for (Assignment assignment : plan.assignments()) {
            normalizeAssignment(assignment, assignments, variableAllocator);
        }

        return new EvaluationPlan(assignments, plan.outputs(), plan.streamPlans());
    }

    private static void normalizeAssignment(Assignment assignment, List<Assignment> output, VariableAllocator variableAllocator)
    {
        switch (assignment.operation()) {
            case Call(String name, List<Reference> arguments) when name.equals("if") -> normalizeIf(assignment, arguments, output, variableAllocator);
            case Call(String name, List<Reference> arguments) when name.equals("coalesce") -> normalizeCoalesce(assignment, arguments, output, variableAllocator);
            default -> output.add(assignment);
        }
    }

    private static void normalizeIf(Assignment assignment, List<Reference> arguments, List<Assignment> output, VariableAllocator variableAllocator)
    {
        checkArgument(arguments.size() == 3, "if requires 3 arguments");

        Reference condition = arguments.get(0);
        Reference whenTrue = arguments.get(1);
        Reference whenFalse = arguments.get(2);

        Variable thenVariable = variableAllocator.next();
        Variable elseVariable = variableAllocator.next();
        MaskExpression conditionMask = new ReferenceMask(condition);

        output.add(new Assignment(thenVariable, new Copy(whenTrue), new AndMask(assignment.mask(), conditionMask)));
        output.add(new Assignment(elseVariable, new Copy(whenFalse), new AndMask(assignment.mask(), new NotMask(conditionMask))));
        output.add(new Assignment(
                assignment.output(),
                new Merge(conditionMask, new Reference(thenVariable, whenTrue.stream()), new Reference(elseVariable, whenFalse.stream())),
                assignment.mask()));
    }

    private static void normalizeCoalesce(Assignment assignment, List<Reference> arguments, List<Assignment> output, VariableAllocator variableAllocator)
    {
        checkArgument(arguments.size() == 2, "coalesce requires 2 arguments");

        Reference first = arguments.get(0);
        Reference second = arguments.get(1);
        Reference firstNulls = new Reference(first.producer(), Stream.NULLS);
        MaskExpression firstIsNull = new ReferenceMask(firstNulls);
        MaskExpression firstIsPresent = new NotMask(firstIsNull);

        Variable firstVariable = variableAllocator.next();
        Variable secondVariable = variableAllocator.next();

        output.add(new Assignment(firstVariable, new Copy(first), new AndMask(assignment.mask(), firstIsPresent)));
        output.add(new Assignment(secondVariable, new Copy(second), new AndMask(assignment.mask(), firstIsNull)));
        output.add(new Assignment(
                assignment.output(),
                new Merge(firstIsPresent, new Reference(firstVariable, first.stream()), new Reference(secondVariable, second.stream())),
                assignment.mask()));
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
