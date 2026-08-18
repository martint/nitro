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

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.ErrorValue;
import org.weakref.nitro.data.ErrorVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;

import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TestBoundLikeUtf8
{
    @Test
    void testLiteralContainsAcrossFlatDictionaryAndRleInputs()
    {
        assertMatches(
                utf8("goog", "le", "xgoogley", "Google", "", "google"),
                Mask.all(6),
                false, false, true, false, false, true);

        assertMatches(
                new DictionaryVector(new int[] {0, 1, 2, 1, 0}, utf8("google", "other", "xgoogley")),
                Mask.all(5),
                true, false, true, false, true);

        assertMatches(
                new RleVector(new int[] {2, 1, 2}, utf8("google", "other", "xgoogley")),
                Mask.all(5),
                true, true, false, true, true);
    }

    @Test
    void testCleanInputUsesCompactImmutableErrorCarrier()
    {
        Streams result = evaluate(
                new BoundLikeUtf8("%google%"),
                Streams.ofValues(utf8("google", "other")),
                Mask.all(2));

        assertThat(result.get(Stream.ERRORS))
                .isInstanceOf(BooleanVector.class)
                .isNotInstanceOf(ErrorVector.class);
        assertThat(((BooleanVector) result.get(Stream.ERRORS)).isAllFalse()).isTrue();
    }

    @Test
    void testBooleanInputErrorsRemainCompact()
    {
        Streams result = evaluate(
                new BoundLikeUtf8("%google%"),
                Streams.ofValues(utf8("google", "other")).with(Stream.ERRORS, new BooleanVector(new boolean[] {false, true})),
                Mask.all(2));

        assertThat(result.get(Stream.ERRORS))
                .isInstanceOf(BooleanVector.class)
                .isNotInstanceOf(ErrorVector.class);
        assertThat(((BooleanVector) result.get(Stream.ERRORS)).values()).containsExactly(false, true);
    }

    @Test
    void testLiteralContainsHonorsMaskNullsAndErrors()
    {
        BinaryVector input = utf8("google", "google", "google", "other", "xgoogley");
        BooleanVector nulls = new BooleanVector(new boolean[] {false, true, false, false, false});
        ErrorVector errors = new ErrorVector(5);
        ErrorValue failure = new ErrorValue("test", 1, "failure", "TEST", "failure");
        errors.setError(2, failure);

        Streams result = evaluate(
                new BoundLikeUtf8("%google%"),
                Streams.ofValues(input).with(Stream.NULLS, nulls).with(Stream.ERRORS, errors),
                Mask.sparse(new int[] {0, 1, 2, 3}, 5));

        assertThat(((BooleanVector) result.get(Stream.VALUES)).values())
                .containsExactly(true, false, false, false, false);
        assertThat(((BooleanVector) result.get(Stream.NULLS)).values())
                .containsExactly(false, true, false, false, false);
        ErrorVector resultErrors = (ErrorVector) result.get(Stream.ERRORS);
        assertThat(resultErrors.values()).containsExactly(false, false, true, false, false);
        assertThat(resultErrors.error(2)).isEqualTo(failure);
    }

    @Test
    void testMultipleLiteralSegmentsRetainLikeSemantics()
    {
        BoundLikeUtf8 function = new BoundLikeUtf8("%special%requests%", new LikeUtf8Policy(false));
        Streams result = evaluate(
                function,
                Streams.ofValues(utf8("special requests", "requests special", "special handling requests", "special")),
                Mask.all(4));

        assertThat(((BooleanVector) result.get(Stream.VALUES)).values())
                .containsExactly(true, false, true, false);
    }

    @Test
    void testDictionaryDirectMaskHonorsNullsAndErrors()
    {
        BoundLikeUtf8 function = new BoundLikeUtf8("%google%");
        Streams input = Streams.ofValues(new DictionaryVector(
                        new int[] {0, 1, 2, 0, 1},
                        utf8("google", "other", "xgoogley")))
                .with(Stream.NULLS, new BooleanVector(new boolean[] {false, false, false, true, false}))
                .with(Stream.ERRORS, new BooleanVector(new boolean[] {false, false, true, false, false}));
        Mask mask = Mask.all(5);

        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            assertThat(function.tryEvaluateTrueMaskInPlace(
                    List.of(input), mask, new PrimitiveExecutionContext(allocator))).isTrue();
        }

        assertThat(mask.selectedCount()).isEqualTo(1);
        assertThat(mask.position(0)).isZero();
    }

    @Test
    void testFlatDirectMaskDeclinesToPreserveVectorizedSweep()
    {
        BoundLikeUtf8 function = new BoundLikeUtf8("%google%");
        Mask mask = Mask.all(2);

        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            assertThat(function.tryEvaluateTrueMaskInPlace(
                    List.of(Streams.ofValues(utf8("google", "other"))),
                    mask,
                    new PrimitiveExecutionContext(allocator))).isFalse();
        }

        assertThat(mask.all()).isTrue();
    }

    private static void assertMatches(Vector input, Mask mask, boolean... expected)
    {
        Streams result = evaluate(new BoundLikeUtf8("%google%"), Streams.ofValues(input), mask);
        assertThat(((BooleanVector) result.get(Stream.VALUES)).values()).containsExactly(expected);
    }

    private static Streams evaluate(BoundLikeUtf8 function, Streams input, Mask mask)
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            return function.apply(
                    List.of(input),
                    mask,
                    EnumSet.allOf(Stream.class),
                    Streams.empty(),
                    new PrimitiveExecutionContext(allocator));
        }
    }

    private static BinaryVector utf8(String... values)
    {
        int bytes = 0;
        for (String value : values) {
            bytes += value.getBytes(StandardCharsets.UTF_8).length;
        }
        BinaryVector result = new BinaryVector(values.length, bytes);
        for (int index = 0; index < values.length; index++) {
            result.setBytes(index, values[index].getBytes(StandardCharsets.UTF_8));
        }
        return result;
    }
}
