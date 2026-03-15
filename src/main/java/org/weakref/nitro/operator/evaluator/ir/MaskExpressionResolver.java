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

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;

public final class MaskExpressionResolver
{
    private final Map<Variable, Assignment> assignments;

    private MaskExpressionResolver(EvaluationPlan plan)
    {
        this.assignments = plan.assignments().stream()
                .collect(java.util.stream.Collectors.toMap(Assignment::output, Function.identity()));
    }

    public static MaskExpression resolve(EvaluationPlan plan, Reference reference)
    {
        return new MaskExpressionResolver(plan).resolve(reference);
    }

    public static MaskExpression resolve(EvaluationPlan plan, MaskExpression expression)
    {
        return new MaskExpressionResolver(plan).resolveExpression(expression);
    }

    private MaskExpression resolve(Reference reference)
    {
        checkArgument(reference.stream() == Stream.VALUES, "Mask references must use the VALUES stream: %s", reference);

        return switch (reference.producer()) {
            case Input _ -> new ReferenceMask(reference);
            case Variable variable -> resolveVariable(variable, reference);
        };
    }

    private MaskExpression resolveVariable(Variable variable, Reference reference)
    {
        Assignment assignment = assignments.get(variable);
        if (assignment == null) {
            return new ReferenceMask(reference);
        }

        return switch (assignment.operation()) {
            case Call(String name, List<Reference> arguments) when name.equals("and") ->
                    new AndMask(arguments.stream()
                            .map(this::resolve)
                            .toList());
            case Call(String name, List<Reference> arguments) when name.equals("or") ->
                    new OrMask(arguments.stream()
                            .map(this::resolve)
                            .toList());
            case Call(String name, List<Reference> arguments) when name.equals("not") -> {
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
            case ReferenceMask(Reference reference) -> reference.stream() == Stream.VALUES ? resolve(reference) : expression;
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
