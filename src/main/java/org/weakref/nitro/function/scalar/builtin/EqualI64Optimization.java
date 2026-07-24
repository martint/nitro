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
package org.weakref.nitro.function.scalar.builtin;

import org.weakref.nitro.operator.evaluator.StaticLongEqualityProvider;
import org.weakref.nitro.operator.evaluator.ir.Reference;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

public final class EqualI64Optimization
        implements StaticLongEqualityProvider
{
    @Override
    public Optional<StaticLongEquality> staticLongEquality(List<Reference> arguments, LiteralResolver literals)
    {
        if (arguments.size() != 2) {
            return Optional.empty();
        }
        OptionalLong left = literals.resolve(arguments.get(0));
        OptionalLong right = literals.resolve(arguments.get(1));
        if (left.isPresent() && right.isEmpty()) {
            return Optional.of(new StaticLongEquality(arguments.get(1), left.getAsLong()));
        }
        if (right.isPresent() && left.isEmpty()) {
            return Optional.of(new StaticLongEquality(arguments.get(0), right.getAsLong()));
        }
        return Optional.empty();
    }
}
