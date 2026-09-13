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
package org.weakref.nitro.jit;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.core.function.projection.ProjectionArgument;
import org.weakref.nitro.core.function.projection.ProjectionCodeProvider;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.weakref.nitro.core.function.projection.ProjectionCodeBuilder.ValueType.BOOLEAN;
import static org.weakref.nitro.core.function.projection.ProjectionCodeBuilder.ValueType.I64;

class TestPartiallyAppliedProjection
{
    @Test
    void testOriginalShapesAndReducedGuardedProgram()
    {
        ProjectionCodeProvider provider = (builder, arguments) -> {
            assertThat(arguments).containsExactly(ProjectionArgument.literal(11L), ProjectionArgument.computed(), ProjectionArgument.literal(true), ProjectionArgument.input());
            return Optional.of(builder.guardedProgram(
                    List.of(I64, I64, BOOLEAN, I64),
                    builder.add(builder.argument(0, I64), builder.argument(1, I64)),
                    builder.or(builder.isNull(0), builder.isNull(3)),
                    builder.argument(2, BOOLEAN)));
        };
        ProjectionProgramBuilder builder = new ProjectionProgramBuilder();
        var program = builder.requireProgram(provider.bindArguments(4, Map.of(0, 11L, 2, true))
                .generate(builder, List.of(ProjectionArgument.computed(), ProjectionArgument.input())).orElseThrow());
        assertThat(program.argumentTypes()).containsExactly(I64, I64);
        assertThat(program.value()).isEqualTo(builder.add(builder.constant(11L), builder.argument(0, I64)));
        assertThat(program.isNull()).isEqualTo(builder.or(builder.constant(false), builder.isNull(1)));
        assertThat(program.fallback()).isEqualTo(builder.constant(true));
    }

    @Test
    void testNestedAndAllConstantBindings()
    {
        ProjectionCodeProvider provider = (builder, arguments) -> Optional.of(builder.program(
                List.of(I64, I64, I64),
                builder.add(builder.argument(0, I64), builder.subtract(builder.argument(1, I64), builder.argument(2, I64))),
                builder.isNull(2)));
        ProjectionProgramBuilder builder = new ProjectionProgramBuilder();
        var program = builder.requireProgram(provider.bindArguments(3, Map.of(1, 9L))
                .bindArguments(2, Map.of(0, 4L, 1, 2L)).generate(builder, List.of()).orElseThrow());
        assertThat(program.argumentTypes()).isEmpty();
        assertThat(program.value()).isEqualTo(builder.add(builder.constant(4L), builder.subtract(builder.constant(9L), builder.constant(2L))));
        assertThat(program.isNull()).isEqualTo(builder.constant(false));
    }

    @Test
    void testUnsupportedRepresentationsAndProviderDecline()
    {
        ProjectionCodeProvider provider = (builder, arguments) -> Optional.empty();
        ProjectionProgramBuilder builder = new ProjectionProgramBuilder();
        assertThat(provider.bindArguments(1, Map.of(0, 3L)).generate(builder, List.of())).isEmpty();
        assertThat(builder.bindArguments(1, Map.of(0, 3.0))).isEmpty();
        assertThat(builder.bindArguments(1, Map.of(0, "3"))).isEmpty();
    }

    @Test
    void testInvalidSignaturesAndLiteralTypes()
    {
        ProjectionProgramBuilder builder = new ProjectionProgramBuilder();
        assertThatThrownBy(() -> builder.bindArguments(1, Map.of(1, 3L))).isInstanceOf(IllegalArgumentException.class);
        var bound = builder.bindArguments(2, Map.of(0, 3L)).orElseThrow();
        assertThatThrownBy(() -> bound.argument(0, BOOLEAN)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> bound.isNull(2)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> bound.program(List.of(BOOLEAN, I64), bound.constant(1L), bound.constant(false))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> bound.program(List.of(I64), bound.constant(1L), bound.constant(false))).isInstanceOf(IllegalArgumentException.class);
        ProjectionCodeProvider provider = (view, arguments) -> Optional.empty();
        assertThatThrownBy(() -> provider.bindArguments(2, Map.of(0, 1L)).generate(builder, List.of())).isInstanceOf(IllegalArgumentException.class);
    }
}
