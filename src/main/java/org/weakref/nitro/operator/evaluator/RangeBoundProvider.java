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
import org.weakref.nitro.operator.evaluator.ir.RangeConstraint;
import org.weakref.nitro.operator.evaluator.ir.Reference;

import java.util.List;
import java.util.Optional;

/**
 * Plan-time lowering supplied by a dynamically registered function.
 *
 * <p>The provider owns call arity, operand roles, and literal interpretation. The evaluator sees only the
 * resulting structural bound and invokes the supplied physical kernel when two compatible bounds can be fused.
 */
public interface RangeBoundProvider
        extends FunctionCapability
{
    Optional<RangeBound> rangeBound(List<Reference> arguments, LiteralResolver literals);

    @FunctionalInterface
    interface LiteralResolver
    {
        Optional<Object> resolve(Reference reference);
    }

    record RangeBound(Reference input, Object bound, RangeConstraint.Position position, RangeConstraint.Kernel kernel) {}
}
