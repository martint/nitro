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
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.Utf8Traits;
import org.weakref.nitro.data.Vector;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

public class TestJoinBufferSupport
{
    @Test
    void testDictionaryBinaryPositionCopyKeepsCompactDictionaryWhenRepeated()
    {
        Allocator allocator = new Allocator();
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
        Allocator allocator = new Allocator();
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
