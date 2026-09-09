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
import org.weakref.nitro.core.function.mask.StaticBinaryEqualityProvider;
import org.weakref.nitro.core.function.projection.ProjectionArgument;
import org.weakref.nitro.core.function.projection.ProjectionCodeBuilder;
import org.weakref.nitro.core.function.projection.ProjectionProgram;

import java.util.List;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Registry-owned projection lowering for UTF-8 equality.
 */
public final class EqualUtf8ProjectionOptimization
        implements MaskCodeProvider, StaticBinaryEqualityProvider
{
    @Override
    public Optional<ProjectionProgram> generate(ProjectionCodeBuilder builder, List<ProjectionArgument> arguments)
    {
        if (arguments.size() != 2) {
            return Optional.empty();
        }
        return Optional.of(builder.program(
                List.of(ProjectionCodeBuilder.ValueType.UTF8, ProjectionCodeBuilder.ValueType.UTF8),
                builder.utf8Equal(
                        builder.argument(0, ProjectionCodeBuilder.ValueType.UTF8),
                        builder.argument(1, ProjectionCodeBuilder.ValueType.UTF8)),
                builder.or(builder.isNull(0), builder.isNull(1))));
    }

    @Override
    public Optional<StaticBinaryEquality> staticBinaryEquality(FunctionCallSite callSite)
    {
        if (callSite.argumentCount() != 2) {
            return Optional.empty();
        }
        Optional<byte[]> left = literalBytes(callSite.argument(0).literal());
        Optional<byte[]> right = literalBytes(callSite.argument(1).literal());
        if (left.isPresent() && callSite.argument(1).literal().isEmpty()) {
            return Optional.of(new StaticBinaryEquality(1, left.orElseThrow()));
        }
        if (right.isPresent() && callSite.argument(0).literal().isEmpty()) {
            return Optional.of(new StaticBinaryEquality(0, right.orElseThrow()));
        }
        return Optional.empty();
    }

    private static Optional<byte[]> literalBytes(Optional<Object> literal)
    {
        Object value = literal.orElse(null);
        if (value instanceof String string) {
            return Optional.of(string.getBytes(UTF_8));
        }
        if (value instanceof byte[] bytes) {
            return Optional.of(bytes.clone());
        }
        return Optional.empty();
    }
}
