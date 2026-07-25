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
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Utf8Traits;
import org.weakref.nitro.function.scalar.builtin.EqualF64Optimization;
import org.weakref.nitro.function.scalar.builtin.EqualI64Optimization;
import org.weakref.nitro.function.scalar.builtin.EqualUtf8ProjectionOptimization;
import org.weakref.nitro.function.scalar.builtin.GreaterThanF64Optimization;
import org.weakref.nitro.function.scalar.builtin.GreaterThanOrEqualF64Optimization;
import org.weakref.nitro.function.scalar.builtin.LessThanF64Optimization;
import org.weakref.nitro.function.scalar.builtin.LessThanI64RangeOptimization;
import org.weakref.nitro.function.scalar.builtin.LessThanOrEqualF64Optimization;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class TestProjectionMaskCompiler
{
    private final ProjectionMaskCompiler compiler = new ProjectionMaskCompiler();

    private static final List<ProjectionArgument> F64_ARGUMENTS =
            List.of(ProjectionArgument.input(), ProjectionArgument.literal(1.0));
    private static final List<ProjectionArgument> I64_ARGUMENTS =
            List.of(ProjectionArgument.input(), ProjectionArgument.literal(1L));

    @Test
    void testCompilesProviderAuthoredDoubleComparisons()
    {
        assertThat(compiler.tryCompile(new EqualF64Optimization(), F64_ARGUMENTS)).isPresent();
        assertThat(compiler.tryCompile(new LessThanF64Optimization(), F64_ARGUMENTS)).isPresent();
        assertThat(compiler.tryCompile(new LessThanOrEqualF64Optimization(), F64_ARGUMENTS)).isPresent();
        assertThat(compiler.tryCompile(new GreaterThanF64Optimization(), F64_ARGUMENTS)).isPresent();
        assertThat(compiler.tryCompile(new GreaterThanOrEqualF64Optimization(), F64_ARGUMENTS)).isPresent();
        assertThat(compiler.tryCompile(new EqualF64Optimization(), F64_ARGUMENTS).orElseThrow().argumentCount())
                .isEqualTo(2);
    }

    @Test
    void testRejectsUnsupportedArity()
    {
        assertThat(compiler.tryCompile(
                new EqualF64Optimization(), List.of(ProjectionArgument.input())))
                .isEmpty();
    }

    @Test
    void testCompilesProviderAuthoredLongComparisons()
    {
        assertThat(compiler.tryCompile(new EqualI64Optimization(), I64_ARGUMENTS)).isPresent();
        assertThat(compiler.tryCompile(new LessThanI64RangeOptimization(), I64_ARGUMENTS)).isPresent();
    }

    @Test
    void testCompilesProviderAuthoredUtf8LiteralEquality()
    {
        ProjectionMaskCompiler.CompiledMask compiled = compiler.tryCompile(
                new EqualUtf8ProjectionOptimization(),
                List.of(ProjectionArgument.input(), ProjectionArgument.literal("BUILDING")))
                .orElseThrow();

        BinaryVector values = utf8("BUILDING", "AUTOMOBILE", "BUILDING");
        Mask trueMask = Mask.all(3);
        assertThat(compiled.evaluate(
                List.of(Streams.ofValues(values), Streams.empty()),
                trueMask,
                true)).isTrue();
        assertThat(trueMask).containsExactly(0, 2);

        Mask falseMask = Mask.all(3);
        assertThat(compiled.evaluate(
                List.of(Streams.ofValues(values), Streams.empty()),
                falseMask,
                false)).isTrue();
        assertThat(falseMask).containsExactly(1);
    }

    @Test
    void testCompiledUtf8LiteralEqualityUsesDictionaryEncoding()
    {
        ProjectionMaskCompiler.CompiledMask compiled = compiler.tryCompile(
                new EqualUtf8ProjectionOptimization(),
                List.of(ProjectionArgument.literal("Brand#45"), ProjectionArgument.input()))
                .orElseThrow();

        DictionaryVector values = new DictionaryVector(
                new int[] {1, 0, 1, 2},
                utf8("Brand#12", "Brand#45", "Brand#23"));
        Mask mask = Mask.all(4);
        assertThat(compiled.evaluate(
                List.of(Streams.empty(), Streams.ofValues(values)),
                mask,
                true)).isTrue();
        assertThat(mask).containsExactly(0, 2);
    }

    @Test
    void testCompilesProviderAuthoredUtf8InputEquality()
    {
        ProjectionMaskCompiler.CompiledMask compiled = compiler.tryCompile(
                new EqualUtf8ProjectionOptimization(),
                List.of(ProjectionArgument.input(), ProjectionArgument.input()))
                .orElseThrow();

        DictionaryVector left = new DictionaryVector(
                new int[] {0, 1, 2, 1},
                utf8("A", "B", "C"));
        DictionaryVector right = new DictionaryVector(
                new int[] {1, 1, 0, 2},
                utf8("C", "A", "B"));
        Mask trueMask = Mask.all(4);
        assertThat(compiled.evaluate(
                List.of(Streams.ofValues(left), Streams.ofValues(right)),
                trueMask,
                true)).isTrue();
        assertThat(trueMask).containsExactly(0, 2, 3);

        Mask falseMask = Mask.all(4);
        assertThat(compiled.evaluate(
                List.of(Streams.ofValues(left), Streams.ofValues(right)),
                falseMask,
                false)).isTrue();
        assertThat(falseMask).containsExactly(1);
    }

    @Test
    void testReusesDynamicKernelWithinCompilerLifetime()
    {
        AtomicInteger generatedKernels = new AtomicInteger();
        ProjectionMaskCompiler compiler = new ProjectionMaskCompiler(() -> {
            generatedKernels.incrementAndGet();
            return Utf8DynamicMaskKernelGenerator.generate();
        });

        List<ProjectionArgument> arguments = List.of(ProjectionArgument.input(), ProjectionArgument.input());
        assertThat(compiler.tryCompile(new EqualUtf8ProjectionOptimization(), arguments)).isPresent();
        assertThat(compiler.tryCompile(new EqualUtf8ProjectionOptimization(), arguments)).isPresent();
        assertThat(generatedKernels).hasValue(1);
    }

    @Test
    void testCompiledUtf8InputEqualityFusesNulls()
    {
        ProjectionMaskCompiler.CompiledMask compiled = compiler.tryCompile(
                new EqualUtf8ProjectionOptimization(),
                List.of(ProjectionArgument.input(), ProjectionArgument.input()))
                .orElseThrow();

        Mask mask = Mask.all(3);
        assertThat(compiled.evaluate(
                List.of(
                        Streams.ofValuesAndNulls(utf8("A", "B", "C"), new BooleanVector(new boolean[] {false, true, false})),
                        Streams.ofValuesAndNulls(utf8("A", "B", "X"), new BooleanVector(new boolean[] {false, false, true}))),
                mask,
                true)).isTrue();
        assertThat(mask).containsExactly(0);
    }

    @Test
    void testCompiledUtf8InputEqualityTraversesNestedDictionariesAndEncodedNulls()
    {
        ProjectionMaskCompiler.CompiledMask compiled = compiler.tryCompile(
                new EqualUtf8ProjectionOptimization(),
                List.of(ProjectionArgument.input(), ProjectionArgument.input()))
                .orElseThrow();

        DictionaryVector left = new DictionaryVector(
                new int[] {5, 2, 0, 3, 1, 4},
                new DictionaryVector(
                        new int[] {3, 1, 0, 2, 1, 3},
                        new DictionaryVector(
                                new int[] {2, 0, 3, 1},
                                utf8("A", "B", "C", "D"))));
        DictionaryVector right = new DictionaryVector(
                new int[] {0, 1, 3, 2, 0, 2},
                new DictionaryVector(
                        new int[] {1, 2, 0, 3},
                        utf8("A", "B", "C", "X")));
        DictionaryVector leftNulls = new DictionaryVector(
                new int[] {0, 1, 0, 0, 0, 0},
                new BooleanVector(new boolean[] {false, true}));
        DictionaryVector rightNulls = new DictionaryVector(
                new int[] {0, 0, 0, 0, 0, 1},
                new BooleanVector(new boolean[] {false, true}));

        Mask trueMask = Mask.all(6);
        assertThat(compiled.evaluate(
                List.of(
                        Streams.of(left, leftNulls, null),
                        Streams.of(right, rightNulls, null)),
                trueMask,
                true)).isTrue();
        assertThat(trueMask).containsExactly(0);

        Mask falseMask = Mask.sparse(new int[] {0, 2, 3, 5}, 6);
        assertThat(compiled.evaluate(
                List.of(
                        Streams.of(left, leftNulls, null),
                        Streams.of(right, rightNulls, null)),
                falseMask,
                false)).isTrue();
        assertThat(falseMask).containsExactly(2, 3);
    }

    private static BinaryVector utf8(String... values)
    {
        int[] offsets = new int[values.length + 1];
        int length = 0;
        for (int index = 0; index < values.length; index++) {
            length += values[index].getBytes(UTF_8).length;
            offsets[index + 1] = length;
        }
        byte[] data = new byte[length];
        int offset = 0;
        for (String value : values) {
            byte[] bytes = value.getBytes(UTF_8);
            System.arraycopy(bytes, 0, data, offset, bytes.length);
            offset += bytes.length;
        }
        BinaryVector vector = new BinaryVector(values.length, offsets, data);
        vector.addTrait(Utf8Traits.UTF8_STRING);
        return vector;
    }
}
