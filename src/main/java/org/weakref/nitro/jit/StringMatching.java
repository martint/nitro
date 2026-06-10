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

import java.util.regex.Pattern;

/**
 * Runtime helpers for string predicates a generated pipeline evaluates once per dictionary entry. Kept off the
 * per-row hot path: a {@code LIKE} pattern compiles to a {@link Pattern} a single time, then matches each
 * dictionary entry.
 */
public final class StringMatching
{
    private StringMatching() {}

    /**
     * Translate a SQL {@code LIKE} pattern into a {@link Pattern}: {@code %} -> {@code .*}, {@code _} -> {@code .},
     * with all other characters quoted. {@code DOTALL} so {@code %} also spans newlines, as SQL requires.
     */
    /** Byte-level substring containment (the {@code LIKE '%literal%'} fast path; UTF-8 substrings align on bytes). */
    public static boolean containsBytes(byte[] value, byte[] target)
    {
        if (target.length == 0) {
            return true;
        }
        outer:
        for (int from = 0; from <= value.length - target.length; from++) {
            for (int i = 0; i < target.length; i++) {
                if (value[from + i] != target[i]) {
                    continue outer;
                }
            }
            return true;
        }
        return false;
    }

    /** The number of code points in UTF-8 bytes (continuation bytes don't start a code point). */
    public static int codePointCount(byte[] utf8)
    {
        int count = 0;
        for (byte b : utf8) {
            if ((b & 0xC0) != 0x80) {
                count++;
            }
        }
        return count;
    }

    public static Pattern likePattern(String like)
    {
        StringBuilder regex = new StringBuilder();
        StringBuilder literal = new StringBuilder();
        for (int i = 0; i < like.length(); i++) {
            char c = like.charAt(i);
            if (c == '%' || c == '_') {
                if (literal.length() > 0) {
                    regex.append(Pattern.quote(literal.toString()));
                    literal.setLength(0);
                }
                regex.append(c == '%' ? ".*" : ".");
            }
            else {
                literal.append(c);
            }
        }
        if (literal.length() > 0) {
            regex.append(Pattern.quote(literal.toString()));
        }
        return Pattern.compile(regex.toString(), Pattern.DOTALL);
    }
}
