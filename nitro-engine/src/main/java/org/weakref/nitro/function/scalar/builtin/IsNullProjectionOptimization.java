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
 * Registry-owned projection lowering for a null test.
 *
 * <p>The program declares a nulls-only input shape, so it is independent of the argument's logical type and physical
 * value carrier. The backend can merge that access with a value-consuming use of the same column, but does not load or
 * materialize the value stream when nullness is the only use.
 */
public final class IsNullProjectionOptimization
        implements ProjectionCodeProvider
{
    @Override
    public Optional<ProjectionProgram> generate(ProjectionCodeBuilder builder, List<ProjectionArgument> arguments)
    {
        if (arguments.size() != 1) {
            return Optional.empty();
        }
        return Optional.of(builder.program(
                List.of(ProjectionCodeBuilder.ValueType.NULLS_ONLY),
                builder.isNull(0),
                builder.constant(false)));
    }
}
