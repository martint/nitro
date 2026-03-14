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

public final class IfNormalizationRule
        implements IrNormalizationRule
{
    @Override
    public boolean matches(Assignment assignment)
    {
        return assignment.operation() instanceof Call(String name, List<Reference> ignored) && name.equals("if");
    }

    @Override
    public void apply(Assignment assignment, IrNormalizer.Context context)
    {
        Call call = (Call) assignment.operation();
        List<Reference> arguments = call.arguments();
        checkArgument(arguments.size() == 3, "if requires 3 arguments");

        Reference condition = arguments.get(0);
        Reference whenTrue = arguments.get(1);
        Reference whenFalse = arguments.get(2);

        Variable thenVariable = context.nextVariable();
        Variable elseVariable = context.nextVariable();
        MaskExpression conditionMask = new ReferenceMask(condition);

        context.emit(new Assignment(thenVariable, new Copy(whenTrue), new AndMask(assignment.mask(), conditionMask)));
        context.emit(new Assignment(elseVariable, new Copy(whenFalse), new AndMask(assignment.mask(), new NotMask(conditionMask))));
        context.emit(new Assignment(
                assignment.output(),
                new Merge(conditionMask, new Reference(thenVariable, whenTrue.stream()), new Reference(elseVariable, whenFalse.stream())),
                assignment.mask()));
    }
}
