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
import org.weakref.nitro.function.scalar.builtin.EqualF64Optimization;
import org.weakref.nitro.function.scalar.builtin.GreaterThanF64Optimization;
import org.weakref.nitro.function.scalar.builtin.GreaterThanOrEqualF64Optimization;
import org.weakref.nitro.function.scalar.builtin.LessThanF64Optimization;
import org.weakref.nitro.function.scalar.builtin.LessThanOrEqualF64Optimization;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TestProjectionMaskCompiler
{
    private static final List<ProjectionArgument> ARGUMENTS =
            List.of(ProjectionArgument.input(), ProjectionArgument.literal(1.0));

    @Test
    void testCompilesProviderAuthoredDoubleComparisons()
    {
        assertThat(ProjectionMaskCompiler.tryCompile(new EqualF64Optimization(), ARGUMENTS)).isPresent();
        assertThat(ProjectionMaskCompiler.tryCompile(new LessThanF64Optimization(), ARGUMENTS)).isPresent();
        assertThat(ProjectionMaskCompiler.tryCompile(new LessThanOrEqualF64Optimization(), ARGUMENTS)).isPresent();
        assertThat(ProjectionMaskCompiler.tryCompile(new GreaterThanF64Optimization(), ARGUMENTS)).isPresent();
        assertThat(ProjectionMaskCompiler.tryCompile(new GreaterThanOrEqualF64Optimization(), ARGUMENTS)).isPresent();
        assertThat(ProjectionMaskCompiler.tryCompile(new EqualF64Optimization(), ARGUMENTS).orElseThrow().argumentCount())
                .isEqualTo(2);
    }

    @Test
    void testRejectsUnsupportedArity()
    {
        assertThat(ProjectionMaskCompiler.tryCompile(
                new EqualF64Optimization(), List.of(ProjectionArgument.input())))
                .isEmpty();
    }
}
