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

import static com.google.common.base.Preconditions.checkArgument;

public final class CoalesceNormalizationRule
        implements IrNormalizationRule
{
    @Override
    public boolean matches(Assignment assignment)
    {
        return assignment.operation() instanceof Call(String name, List<Reference> ignored) && name.equals("coalesce");
    }

    @Override
    public void apply(Assignment assignment, IrNormalizer.Context context)
    {
        Call call = (Call) assignment.operation();
        List<Reference> arguments = call.arguments();
        checkArgument(arguments.size() == 2, "coalesce requires 2 arguments");

        Reference first = arguments.get(0);
        Reference second = arguments.get(1);
        Reference firstNulls = new Reference(first.producer(), Stream.NULLS);
        MaskExpression firstIsNull = new ReferenceMask(firstNulls);
        MaskExpression firstIsPresent = new NotMask(firstIsNull);

        Variable firstVariable = context.nextVariable();
        Variable secondVariable = context.nextVariable();

        context.emit(new Assignment(firstVariable, new Copy(first), new AndMask(assignment.mask(), firstIsPresent)));
        context.emit(new Assignment(secondVariable, new Copy(second), new AndMask(assignment.mask(), firstIsNull)));
        context.emit(new Assignment(
                assignment.output(),
                new Merge(firstIsPresent, new Reference(firstVariable, first.stream()), new Reference(secondVariable, second.stream())),
                assignment.mask()));
    }
}
