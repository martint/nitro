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

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers {@link OperatorVectorSupport#binaryEquals(byte[], int, byte[], int, int)} across the word-at-a-time
 * fast path, the masked sub-word tail, the array-end byte-loop fallback, and the {@code Arrays.mismatch} path,
 * including differences isolated to the first, an interior, or the final byte. The kernel is shared by binary
 * grouping and joins, so a wrong-length or wrong-tail compare would silently mis-group or mis-join.
 */
class TestBinaryEquals
{
    @Test
    void equalAcrossAllLengths()
    {
        for (int length = 0; length <= 40; length++) {
            byte[] left = sequence(length, 0);
            byte[] right = sequence(length, 0);
            // Pad so 8-byte reads stay in bounds (exercises the masked-word tail path).
            assertThat(OperatorVectorSupport.binaryEquals(pad(left), 0, pad(right), 0, length))
                    .as("equal length %d (padded)", length)
                    .isTrue();
            // Exact-size arrays force the byte-loop fallback for the sub-word tail at the array end.
            assertThat(OperatorVectorSupport.binaryEquals(left, 0, right, 0, length))
                    .as("equal length %d (exact, end-of-array)", length)
                    .isTrue();
        }
    }

    @Test
    void differsAtEveryBytePosition()
    {
        for (int length = 1; length <= 40; length++) {
            for (int differingIndex = 0; differingIndex < length; differingIndex++) {
                byte[] left = sequence(length, 0);
                byte[] right = sequence(length, 0);
                right[differingIndex] ^= 0xFF;
                assertThat(OperatorVectorSupport.binaryEquals(pad(left), 0, pad(right), 0, length))
                        .as("length %d differing at %d (padded)", length, differingIndex)
                        .isFalse();
                assertThat(OperatorVectorSupport.binaryEquals(left, 0, right, 0, length))
                        .as("length %d differing at %d (exact, end-of-array)", length, differingIndex)
                        .isFalse();
            }
        }
    }

    @Test
    void honorsOffsetsAndIgnoresNeighbouringBytes()
    {
        // The over-read in the word/masked path must never let neighbouring bytes affect the result: equal
        // windows surrounded by different bytes must still compare equal, at non-zero offsets.
        for (int length = 1; length <= 12; length++) {
            byte[] left = new byte[length + 16];
            byte[] right = new byte[length + 16];
            Arrays.fill(left, (byte) 0xAA);
            Arrays.fill(right, (byte) 0x55);
            for (int index = 0; index < length; index++) {
                left[4 + index] = (byte) (index + 1);
                right[4 + index] = (byte) (index + 1);
            }
            assertThat(OperatorVectorSupport.binaryEquals(left, 4, right, 4, length))
                    .as("offset window length %d", length)
                    .isTrue();
        }
    }

    @Test
    void binaryHashMaskedTailMatchesByteLoop()
    {
        // The masked-word tail and the array-end byte-loop fallback must produce the SAME hash; otherwise the
        // same value would hash differently depending on where it sits in its backing array, splitting groups.
        for (int length = 0; length <= 40; length++) {
            byte[] exact = sequence(length, 11);          // 8-byte tail read would run off the end -> byte loop
            byte[] padded = pad(exact);                   // 8 bytes readable past the tail -> masked-word path
            assertThat(OperatorVectorSupport.binaryHash(padded, 0, length))
                    .as("hash length %d: masked tail == byte-loop tail", length)
                    .isEqualTo(OperatorVectorSupport.binaryHash(exact, 0, length));
        }
    }

    @Test
    void binaryHashHonorsOffsetAndContent()
    {
        byte[] base = sequence(7, 3);
        byte[] shifted = new byte[7 + 5];
        Arrays.fill(shifted, (byte) 0x99);
        System.arraycopy(base, 0, shifted, 5, 7);
        assertThat(OperatorVectorSupport.binaryHash(shifted, 5, 7))
                .as("hash is offset-relative, independent of surrounding bytes")
                .isEqualTo(OperatorVectorSupport.binaryHash(pad(base), 0, 7));

        byte[] other = sequence(7, 3);
        other[6] ^= 0xFF;
        assertThat(OperatorVectorSupport.binaryHash(pad(other), 0, 7))
                .as("differing tail byte changes the hash")
                .isNotEqualTo(OperatorVectorSupport.binaryHash(pad(base), 0, 7));
    }

    private static byte[] sequence(int length, int seed)
    {
        byte[] bytes = new byte[length];
        for (int index = 0; index < length; index++) {
            bytes[index] = (byte) (seed + index * 31 + 7);
        }
        return bytes;
    }

    private static byte[] pad(byte[] bytes)
    {
        return Arrays.copyOf(bytes, bytes.length + 8);
    }
}
