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
 * Registry-owned projection lowering for three-valued boolean conjunction.
 */
public final class AndBooleanOptimization
        implements ProjectionCodeProvider
{
    @Override
    public Optional<ProjectionProgram> generate(ProjectionCodeBuilder builder, List<ProjectionArgument> arguments)
    {
        if (arguments.size() != 2) {
            return Optional.empty();
        }
        var left = builder.argument(0, ProjectionCodeBuilder.ValueType.BOOLEAN);
        var right = builder.argument(1, ProjectionCodeBuilder.ValueType.BOOLEAN);
        var leftNull = builder.isNull(0);
        var rightNull = builder.isNull(1);
        var knownFalse = builder.or(
                builder.and(builder.not(leftNull), builder.not(left)),
                builder.and(builder.not(rightNull), builder.not(right)));
        return Optional.of(builder.program(
                List.of(ProjectionCodeBuilder.ValueType.BOOLEAN, ProjectionCodeBuilder.ValueType.BOOLEAN),
                builder.and(left, right),
                builder.and(builder.not(knownFalse), builder.or(leftNull, rightNull))));
    }
}
