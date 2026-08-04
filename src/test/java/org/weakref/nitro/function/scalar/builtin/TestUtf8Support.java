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

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestUtf8Support
{
    @Test
    void testPositiveAsciiSubstringSlice()
    {
        assertSlice("abcdef", 2, 3, "bcd");
        assertSlice("abcdef", 5, 20, "ef");
        assertSlice("abcdef", 20, 3, "");
        assertSlice("猫尾x", 2, 2, "尾x");
    }

    @Test
    void testRejectsNonPositiveStart()
    {
        assertThatThrownBy(() -> Utf8Support.substringSlicePositiveAsciiFast(new byte[0], 0, 0, 0, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("start must be positive");
    }

    private static void assertSlice(String value, long start, long count, String expected)
    {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        long slice = Utf8Support.substringSlicePositiveAsciiFast(bytes, 0, bytes.length, start, count);
        int offset = (int) (slice >>> 32);
        int length = (int) slice;
        assertThat(new String(bytes, offset, length, StandardCharsets.UTF_8)).isEqualTo(expected);
        assertThat(slice).isEqualTo(Utf8Support.substringSlice(bytes, 0, bytes.length, start, count));
    }
}
