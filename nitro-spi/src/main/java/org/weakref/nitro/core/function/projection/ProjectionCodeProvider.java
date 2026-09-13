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
package org.weakref.nitro.core.function.projection;

import org.weakref.nitro.core.function.FunctionCapability;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Optional projection lowering supplied by a dynamically loaded function provider.
 *
 * <p>The provider owns signature, arity, shape admission, value semantics, and null semantics. The engine supplies
 * only a low-level expression builder and may fall back to generic invocation when this capability declines a shape.
 */
public interface ProjectionCodeProvider
        extends FunctionCapability
{
    /// Retains this provider's admission, value, null and fallback program after partial application.
    /// Unsupported literal representations decline generation, leaving ordinary invocation available.
    default ProjectionCodeProvider bindArguments(int argumentCount, Map<Integer, Object> literals)
    {
        Map<Integer, Object> bound = Map.copyOf(literals);
        if (argumentCount < 0 || bound.keySet().stream().anyMatch(index -> index < 0 || index >= argumentCount)) {
            throw new IllegalArgumentException("bound argument is outside the provider signature");
        }
        return (builder, arguments) -> {
            if (arguments.size() != argumentCount - bound.size()) {
                throw new IllegalArgumentException("runtime arguments do not match the partially applied signature");
            }
            List<ProjectionArgument> original = new ArrayList<>(argumentCount);
            int runtimeIndex = 0;
            for (int index = 0; index < argumentCount; index++) {
                original.add(bound.containsKey(index)
                        ? ProjectionArgument.literal(bound.get(index))
                        : arguments.get(runtimeIndex++));
            }
            return builder.bindArguments(argumentCount, bound)
                    .flatMap(view -> generate(view, List.copyOf(original)));
        };
    }

    Optional<ProjectionProgram> generate(
            ProjectionCodeBuilder builder,
            List<ProjectionArgument> arguments);
}
