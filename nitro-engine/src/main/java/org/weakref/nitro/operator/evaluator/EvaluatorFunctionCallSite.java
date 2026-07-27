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
package org.weakref.nitro.operator.evaluator;

import org.weakref.nitro.core.function.FunctionCapability;
import org.weakref.nitro.core.function.mask.FunctionCallSite;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Literal;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/// Adapts evaluator IR to the classloader-neutral function call-site contract.
public final class EvaluatorFunctionCallSite
        implements FunctionCallSite
{
    private final Call call;
    private final Map<Variable, Assignment> assignments;
    private final PrimitiveRegistry primitiveRegistry;

    public EvaluatorFunctionCallSite(Call call, EvaluationPlan plan, PrimitiveRegistry primitiveRegistry)
    {
        this(
                call,
                requireNonNull(plan, "plan is null").assignments().stream()
                        .collect(java.util.stream.Collectors.toMap(Assignment::output, Function.identity())),
                primitiveRegistry);
    }

    public EvaluatorFunctionCallSite(Call call, Map<Variable, Assignment> assignments, PrimitiveRegistry primitiveRegistry)
    {
        this.call = requireNonNull(call, "call is null");
        this.assignments = requireNonNull(assignments, "assignments is null");
        this.primitiveRegistry = requireNonNull(primitiveRegistry, "primitiveRegistry is null");
    }

    @Override
    public int argumentCount()
    {
        return call.arguments().size();
    }

    @Override
    public Argument argument(int index)
    {
        Reference reference = call.arguments().get(index);
        Assignment assignment = reference.producer() instanceof Variable variable ? assignments.get(variable) : null;
        return new Argument()
        {
            @Override
            public Optional<Object> literal()
            {
                return assignment != null && assignment.operation() instanceof Literal literal
                        ? Optional.ofNullable(literal.value())
                        : Optional.empty();
            }

            @Override
            public Optional<FunctionCallSite> call()
            {
                return assignment != null && assignment.operation() instanceof Call nestedCall
                        ? Optional.of(new EvaluatorFunctionCallSite(nestedCall, assignments, primitiveRegistry))
                        : Optional.empty();
            }
        };
    }

    @Override
    public <T extends FunctionCapability> Optional<T> capability(Class<T> capabilityType)
    {
        return primitiveRegistry.capability(call, capabilityType);
    }
}
