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

import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Utf8Traits;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Streams;

/**
 * Physical execution support for a compiled {@code UTF8 input == literal} expression.
 *
 * <p>This class knows vector encodings, not function identity. Its dictionary scratch belongs to one compiled
 * evaluator instance and is reused across batches; there is no ambient cache or static service.
 */
final class Utf8LiteralMaskSupport
{
    private final Utf8LiteralMatcher matcher;
    private boolean[] dictionaryMatches = new boolean[0];

    Utf8LiteralMaskSupport(byte[] literal)
    {
        matcher = Utf8LiteralMatcherGenerator.generate(literal);
    }

    boolean evaluate(Streams input, Mask mask, boolean selectMatches)
    {
        Vector values = input.values();
        if (!hasUtf8Trait(values)) {
            return false;
        }
        return switch (values) {
            case BinaryVector flat -> {
                retainFlat(flat, mask, selectMatches);
                yield true;
            }
            case DictionaryVector dictionary when dictionary.baseValues() instanceof BinaryVector base -> {
                retainDictionary(dictionary, base, mask, selectMatches);
                yield true;
            }
            case RleVector rle when rle.values() instanceof BinaryVector runs -> {
                retainRle(rle, runs, mask, selectMatches);
                yield true;
            }
            default -> false;
        };
    }

    private void retainDictionary(DictionaryVector dictionary, BinaryVector values, Mask mask, boolean selectMatches)
    {
        int dictionarySize = values.length();
        if (dictionaryMatches.length < dictionarySize) {
            dictionaryMatches = new boolean[dictionarySize];
        }
        byte[] data = values.data();
        int[] offsets = values.offsets();
        for (int position = 0; position < dictionarySize; position++) {
            dictionaryMatches[position] = matcher.matches(
                    data,
                    offsets[position],
                    offsets[position + 1] - offsets[position]);
        }
        if (dictionary.dictionaryDepth() == 1) {
            int matchingId = singleMatchingId(dictionaryMatches, dictionarySize);
            if (matchingId >= 0) {
                mask.retainDictionaryIdComparison(dictionary.ids(), matchingId, null, selectMatches);
            }
            else {
                mask.retainDictionaryComparison(dictionary.ids(), dictionaryMatches, null, selectMatches);
            }
            return;
        }
        retainMapped(mask, selectMatches, position -> dictionaryMatches[dictionary.basePosition(position)]);
    }

    private static int singleMatchingId(boolean[] matches, int length)
    {
        int matchingId = -1;
        for (int index = 0; index < length; index++) {
            if (!matches[index]) {
                continue;
            }
            if (matchingId >= 0) {
                return -1;
            }
            matchingId = index;
        }
        return matchingId;
    }

    private void retainFlat(BinaryVector values, Mask mask, boolean selectMatches)
    {
        byte[] data = values.data();
        int[] offsets = values.offsets();
        retainMapped(mask, selectMatches, position ->
                matcher.matches(data, offsets[position], offsets[position + 1] - offsets[position]));
    }

    private void retainRle(RleVector rle, BinaryVector values, Mask mask, boolean selectMatches)
    {
        byte[] data = values.data();
        int[] offsets = values.offsets();
        retainMapped(mask, selectMatches, position -> {
            int run = rle.runIndex(position);
            return matcher.matches(data, offsets[run], offsets[run + 1] - offsets[run]);
        });
    }

    private static void retainMapped(Mask mask, boolean selectMatches, PositionMatcher matcher)
    {
        if (mask.none()) {
            return;
        }
        boolean dense = mask.all();
        int iterations = mask.selectedCount();
        int[] positions = dense ? mask.positionsArrayForOverwrite(iterations) : mask.selectedPositions();
        int retained = 0;
        for (int index = 0; index < iterations; index++) {
            int position = dense ? index : positions[index];
            if (matcher.matches(position) == selectMatches) {
                positions[retained++] = position;
            }
        }
        mask.finishRetain(retained);
    }

    private static boolean hasUtf8Trait(Vector vector)
    {
        return switch (vector) {
            case BinaryVector binary -> binary.hasTrait(Utf8Traits.UTF8_STRING);
            case DictionaryVector dictionary -> hasUtf8Trait(dictionary.values());
            case RleVector rle -> hasUtf8Trait(rle.values());
            default -> false;
        };
    }

    @FunctionalInterface
    private interface PositionMatcher
    {
        boolean matches(int position);
    }
}
