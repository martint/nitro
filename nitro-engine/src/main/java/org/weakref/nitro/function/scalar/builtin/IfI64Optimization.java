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

import org.weakref.nitro.core.function.projection.ProjectionArgument;
import org.weakref.nitro.core.function.projection.ProjectionCodeBuilder;
import org.weakref.nitro.core.function.projection.ProjectionCodeProvider;
import org.weakref.nitro.core.function.projection.ProjectionProgram;

import java.util.List;
import java.util.Optional;

/**
 * Registry-owned projection lowering for an I64 conditional.
 */
public final class IfI64Optimization
        implements ProjectionCodeProvider
{
    @Override
    public Optional<ProjectionProgram> generate(ProjectionCodeBuilder builder, List<ProjectionArgument> arguments)
    {
        if (arguments.size() != 3) {
            return Optional.empty();
        }
        var condition = builder.and(
                builder.not(builder.isNull(0)),
                builder.argument(0, ProjectionCodeBuilder.ValueType.BOOLEAN));
        return Optional.of(builder.program(
                List.of(
                        ProjectionCodeBuilder.ValueType.BOOLEAN,
                        ProjectionCodeBuilder.ValueType.I64,
                        ProjectionCodeBuilder.ValueType.I64),
                builder.conditional(
                        condition,
                        builder.argument(1, ProjectionCodeBuilder.ValueType.I64),
                        builder.argument(2, ProjectionCodeBuilder.ValueType.I64)),
                builder.conditional(condition, builder.isNull(1), builder.isNull(2))));
    }
}
