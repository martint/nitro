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

import org.weakref.nitro.core.function.mask.FunctionCallSite;
import org.weakref.nitro.core.function.mask.MaskCodeProvider;
import org.weakref.nitro.core.function.mask.StaticLongEqualityProvider;
import org.weakref.nitro.core.function.projection.ProjectionArgument;
import org.weakref.nitro.core.function.projection.ProjectionCodeBuilder;
import org.weakref.nitro.core.function.projection.ProjectionProgram;

import java.util.List;
import java.util.Optional;

public final class EqualI64Optimization
        implements StaticLongEqualityProvider, MaskCodeProvider
{
    @Override
    public Optional<ProjectionProgram> generate(ProjectionCodeBuilder builder, List<ProjectionArgument> arguments)
    {
        if (arguments.size() != 2) {
            return Optional.empty();
        }
        var left = builder.argument(0, ProjectionCodeBuilder.ValueType.I64);
        var right = builder.argument(1, ProjectionCodeBuilder.ValueType.I64);
        return Optional.of(builder.program(
                List.of(ProjectionCodeBuilder.ValueType.I64, ProjectionCodeBuilder.ValueType.I64),
                builder.equal(left, right),
                builder.or(builder.isNull(0), builder.isNull(1))));
    }

    @Override
    public Optional<StaticLongEquality> staticLongEquality(FunctionCallSite callSite)
    {
        if (callSite.argumentCount() != 2) {
            return Optional.empty();
        }
        Optional<Object> left = callSite.argument(0).literal();
        Optional<Object> right = callSite.argument(1).literal();
        if (left.orElse(null) instanceof Long value && right.isEmpty()) {
            return Optional.of(new StaticLongEquality(1, value));
        }
        if (right.orElse(null) instanceof Long value && left.isEmpty()) {
            return Optional.of(new StaticLongEquality(0, value));
        }
        return Optional.empty();
    }
}
