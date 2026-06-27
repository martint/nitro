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

import io.airlift.joni.Regex;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.operator.scalar.JoniRegexpFunctions;
import io.trino.type.JoniRegexp;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies {@link JoniRegexpSupport#replace} is byte-for-byte identical to Trino's
 * {@code JoniRegexpFunctions.regexpReplace}, the engine the Trino comparison harness uses for q29.
 */
public class TestJoniRegexpParity
{
    private static final List<String> PATTERNS = List.of(
            "^https?://(?:www\\.)?([^/]+)/.*$",   // ClickBench q29
            "([a-z]+)",
            "(\\d+)-(\\d+)",
            "a",
            "",
            "(?<host>[^.]+)\\.com",
            "x*",
            "\\s+");

    private static final List<String> REPLACEMENTS = List.of(
            "$1", "\\1", "[$1]", "$2-$1", "X", "", "${host}", "$0", "a$1b", "\\$literal", "\\\\");

    private static final List<String> SOURCES = List.of(
            "http://www.example.com/path/page.html",
            "https://sub.domain.org/x",
            "ftp://no-match",
            "",
            "abc123-456def",
            "hello.com and world.com",
            "  spaced   out  ",
            "xxxxyyyy",
            "a",
            "AaBbCc");

    @Test
    void matchesTrino()
    {
        for (String patternString : PATTERNS) {
            Slice patternSlice = Slices.utf8Slice(patternString);
            Regex nitroPattern = JoniRegexpSupport.compile(patternSlice);
            JoniRegexp trinoPattern = new JoniRegexp(patternSlice, JoniRegexpSupport.compile(patternSlice));
            for (String replacementString : REPLACEMENTS) {
                Slice replacement = Slices.utf8Slice(replacementString);
                for (String sourceString : SOURCES) {
                    Slice source = Slices.utf8Slice(sourceString);
                    String trino;
                    try {
                        trino = JoniRegexpFunctions.regexpReplace(source, trinoPattern, replacement).toStringUtf8();
                    }
                    catch (RuntimeException e) {
                        // Trino rejects this (pattern, replacement) pair; Nitro must reject it too.
                        try {
                            JoniRegexpSupport.replace(source, nitroPattern, replacement);
                        }
                        catch (RuntimeException expected) {
                            continue;
                        }
                        throw new AssertionError("Nitro accepted what Trino rejected: pattern=[" + patternString
                                + "] replacement=[" + replacementString + "] source=[" + sourceString + "]");
                    }
                    String nitro = JoniRegexpSupport.replace(source, nitroPattern, replacement).toStringUtf8();
                    assertThat(nitro)
                            .withFailMessage("pattern=[%s] replacement=[%s] source=[%s]: nitro=[%s] trino=[%s]",
                                    patternString, replacementString, sourceString, nitro, trino)
                            .isEqualTo(trino);
                }
            }
        }
    }
}
