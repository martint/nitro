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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Registry-owned projection lowering for UTF-8 set membership.
 */
public final class InUtf8ProjectionOptimization
        implements ProjectionCodeProvider
{
    @Override
    public Optional<ProjectionProgram> generate(ProjectionCodeBuilder builder, List<ProjectionArgument> arguments)
    {
        if (arguments.size() < 2 ||
                arguments.getFirst().kind() != ProjectionArgument.Kind.INPUT) {
            return Optional.empty();
        }
        for (int index = 1; index < arguments.size(); index++) {
            if (arguments.get(index).kind() != ProjectionArgument.Kind.LITERAL ||
                    !(arguments.get(index).literal() instanceof String)) {
                return Optional.empty();
            }
        }

        List<ProjectionCodeBuilder.ValueType> argumentTypes = new ArrayList<>(arguments.size());
        var input = builder.argument(0, ProjectionCodeBuilder.ValueType.UTF8);
        var matches = builder.utf8Equal(input, builder.argument(1, ProjectionCodeBuilder.ValueType.UTF8));
        argumentTypes.add(ProjectionCodeBuilder.ValueType.UTF8);
        argumentTypes.add(ProjectionCodeBuilder.ValueType.UTF8);
        for (int index = 2; index < arguments.size(); index++) {
            argumentTypes.add(ProjectionCodeBuilder.ValueType.UTF8);
            matches = builder.or(
                    matches,
                    builder.utf8Equal(input, builder.argument(index, ProjectionCodeBuilder.ValueType.UTF8)));
        }
        return Optional.of(builder.program(argumentTypes, matches, builder.isNull(0)));
    }
}
