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

import java.util.List;
import java.util.Optional;

/**
 * Optional projection lowering supplied by a dynamically loaded function provider.
 *
 * <p>The provider owns signature, arity, shape admission, value semantics, and null semantics. The engine supplies
 * only a low-level expression builder and may fall back to generic invocation when this capability declines a shape.
 */
public interface ProjectionCodeProvider
{
    Optional<ProjectionProgram> generate(
            ProjectionCodeBuilder builder,
            List<ProjectionArgument> arguments);
}
