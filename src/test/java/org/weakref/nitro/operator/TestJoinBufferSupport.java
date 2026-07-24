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
package org.weakref.nitro.operator;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.ConcatenatedBooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.SelectedPositions;
import org.weakref.nitro.data.Utf8Traits;
import org.weakref.nitro.data.Vector;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

public class TestJoinBufferSupport
{
    @Test
    void testPrimitivePositionCopiesHonorInputAndOutputOffsets()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("JoinBufferSupportTest");
        JoinBufferSupport buffers = new JoinBufferSupport(allocator, context);

        int[] positions = {19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
        double[] doubles = new double[20];
        long[] longs = new long[20];
        for (int index = 0; index < doubles.length; index++) {
            doubles[index] = index + 0.25;
            longs[index] = 1_000 + index;
        }

        Streams copiedDoubles = buffers.copyPositionsFresh(
                (Streams) null,
                Streams.ofValues(new F64Vector(doubles)),
                positions,
                2,
                13,
                3,
                20);
        assertThat(((F64Vector) copiedDoubles.values()).values())
                .containsExactly(0, 0, 0, 17.25, 16.25, 15.25, 14.25, 13.25, 12.25, 11.25, 10.25, 9.25, 8.25, 7.25, 6.25, 5.25, 0, 0, 0, 0);

        Streams copiedLongs = buffers.copyPositions(
                null,
                Streams.ofValues(new I64Vector(longs)),
                SelectedPositions.positions(positions, 2, 13),
                3,
                20);
        assertThat(((I64Vector) copiedLongs.values()).values())
                .containsExactly(0, 0, 0, 1_017, 1_016, 1_015, 1_014, 1_013, 1_012, 1_011, 1_010, 1_009, 1_008, 1_007, 1_006, 1_005, 0, 0, 0, 0);

        allocator.release(context);
    }

    @Test
    void testAllFalseCopyStaysCompactUntilTrueValueArrives()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("JoinBufferSupportTest");
        JoinBufferSupport buffers = new JoinBufferSupport(allocator, context);

        Streams copied = buffers.copyPositionsFresh(
                (Streams) null,
                Streams.ofValues(new BooleanVector(new boolean[] {false, false})),
                new int[] {0, 1},
                0,
                2,
                0,
                4);

        assertThat(copied.values()).isInstanceOf(RleVector.class);

        copied = buffers.copyPositionsFresh(
                copied,
                Streams.ofValues(new BooleanVector(new boolean[] {true, false})),
                new int[] {0, 1},
                0,
                2,
                2,
                4);

        assertThat(copied.values()).isInstanceOf(BooleanVector.class);
        assertThat(((BooleanVector) copied.values()).values()).containsExactly(false, false, true, false);

        allocator.release(context);
    }

    @Test
    void testDictionaryBinaryPositionCopyKeepsCompactDictionaryWhenRepeated()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("JoinBufferSupportTest");
        JoinBufferSupport buffers = new JoinBufferSupport(allocator, context);

        BinaryVector dictionaryValues = utf8("alpha", "beta", "unused");
        DictionaryVector source = DictionaryVector.wrap(new int[] {0, 1, 0, 1, 0}, dictionaryValues);

        Streams copied = buffers.copyPositionsFresh(
                (Streams) null,
                Streams.ofValues(source),
                new int[] {0, 1, 2, 3, 4},
                0,
                5,
                0,
                5);

        Vector values = copied.values();
        assertThat(values).isInstanceOf(DictionaryVector.class);
        DictionaryVector dictionary = (DictionaryVector) values;
        assertThat(dictionary.ids()).containsExactly(0, 1, 0, 1, 0);
        assertThat(dictionary.values()).isInstanceOf(BinaryVector.class);

        BinaryVector compactValues = (BinaryVector) dictionary.values();
        assertThat(compactValues.length()).isEqualTo(2);
        assertThat(utf8(compactValues, 0)).isEqualTo("alpha");
        assertThat(utf8(compactValues, 1)).isEqualTo("beta");

        allocator.release(context);
    }

    @Test
    void testDictionaryBinaryPositionCopyFallsBackWhenOutputIsPartial()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("JoinBufferSupportTest");
        JoinBufferSupport buffers = new JoinBufferSupport(allocator, context);

        BinaryVector dictionaryValues = utf8("alpha", "beta");
        DictionaryVector source = DictionaryVector.wrap(new int[] {0, 1}, dictionaryValues);

        Streams copied = buffers.copyPositionsFresh(
                (Streams) null,
                Streams.ofValues(source),
                new int[] {0, 1},
                0,
                2,
                0,
                4);

        assertThat(copied.values()).isInstanceOf(BinaryVector.class);
        assertThat(utf8(copied.values(), 0)).isEqualTo("alpha");
        assertThat(utf8(copied.values(), 1)).isEqualTo("beta");

        allocator.release(context);
    }

    @Test
    void testConcatenatedRleBooleanPositionCopySupportsForwardAndBackwardPositions()
    {
        RleVector first = new RleVector(
                new int[] {2, 2},
                new BooleanVector(new boolean[] {false, true}));
        RleVector second = new RleVector(
                new int[] {1, 3},
                new BooleanVector(new boolean[] {true, false}));
        ConcatenatedBooleanVector source = new ConcatenatedBooleanVector(new Vector[] {first, second});

        assertCopiedBooleans(source, new int[] {0, 2, 3, 4, 6, 7}, false, true, true, true, false, false);
        assertCopiedBooleans(source, new int[] {0, 1, 2, 3, 0, 4, 6, 7}, false, false, true, true, false, true, false, false);
        assertCopiedBooleans(source, new int[] {7, 0, 4, 3, 6, 2}, false, false, true, true, false, true);
    }

    private static void assertCopiedBooleans(Vector source, int[] positions, boolean... expected)
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("JoinBufferSupportTest");
        JoinBufferSupport buffers = new JoinBufferSupport(allocator, context);

        Streams copied = buffers.copyPositionsFresh(
                (Streams) null,
                Streams.ofValues(source),
                positions,
                0,
                positions.length,
                0,
                positions.length);

        assertThat(copied.values()).isInstanceOf(BooleanVector.class);
        assertThat(((BooleanVector) copied.values()).values()).containsExactly(expected);
        allocator.release(context);
    }

    private static BinaryVector utf8(String... values)
    {
        int totalBytes = 0;
        for (String value : values) {
            totalBytes += value.length();
        }

        BinaryVector vector = new BinaryVector(values.length, totalBytes);
        vector.addTrait(Utf8Traits.UTF8_STRING);
        vector.addTrait(Utf8Traits.ASCII_ONLY);
        for (int index = 0; index < values.length; index++) {
            vector.setBytes(index, values[index].getBytes(StandardCharsets.UTF_8));
        }
        return vector;
    }

    private static String utf8(Vector vector, int position)
    {
        return switch (vector) {
            case BinaryVector binary -> new String(binary.copyBytes(position), StandardCharsets.UTF_8);
            case DictionaryVector dictionary -> utf8(dictionary.values(), dictionary.ids()[position]);
            default -> throw new IllegalArgumentException("Expected binary vector but found " + vector.getClass().getSimpleName());
        };
    }
}
