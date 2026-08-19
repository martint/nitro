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
package org.weakref.nitro.data;

public final class ErrorVectors
{
    private ErrorVectors() {}

    /**
     * Returns a diagnostic for the logical position when the error carrier has
     * one. Legacy boolean error vectors return {@code null}.
     */
    public static ErrorValue errorAt(Vector vector, int position)
    {
        return switch (vector) {
            case ErrorVector errors -> errors.error(position);
            case DictionaryVector dictionary -> errorAt(dictionary.values(), dictionary.ids()[position]);
            case RleVector rle -> errorAt(rle.values(), rle.runIndex(position));
            case ConcatenatedBooleanVector concatenated -> concatenatedErrorAt(concatenated, position);
            case null, default -> null;
        };
    }

    public static boolean hasDiagnostics(Vector vector)
    {
        return switch (vector) {
            case ErrorVector _ -> true;
            case DictionaryVector dictionary -> hasDiagnostics(dictionary.values());
            case RleVector rle -> hasDiagnostics(rle.values());
            case ConcatenatedBooleanVector concatenated -> {
                boolean diagnostics = false;
                for (int index = 0; index < concatenated.segmentCount(); index++) {
                    diagnostics |= hasDiagnostics(concatenated.segment(index));
                }
                yield diagnostics;
            }
            case null, default -> false;
        };
    }

    private static ErrorValue concatenatedErrorAt(ConcatenatedBooleanVector concatenated, int position)
    {
        int localPosition = position;
        for (int index = 0; index < concatenated.segmentCount(); index++) {
            Vector segment = concatenated.segment(index);
            if (localPosition < segment.length()) {
                return errorAt(segment, localPosition);
            }
            localPosition -= segment.length();
        }
        throw new IndexOutOfBoundsException(position);
    }
}
