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

import io.airlift.jcodings.specific.NonStrictUTF8Encoding;
import io.airlift.joni.Matcher;
import io.airlift.joni.Option;
import io.airlift.joni.Regex;
import io.airlift.joni.Region;
import io.airlift.joni.Syntax;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.SliceUtf8;

/**
 * {@code regexp_replace} on the Joni (Oniguruma) engine, byte-for-byte equivalent to Trino's
 * {@code JoniRegexpFunctions.regexpReplace}. Joni is the engine Trino's SQL {@code regexp_replace} uses by
 * default; for typical anchored patterns it is markedly faster than RE2J's linear NFA simulation (RE2J pays a
 * heavy submatch cost whenever the pattern has a capture group).
 *
 * <p>The replacement uses {@code $N} (and {@code ${name}}) group references with {@code \} escaping, matching
 * Joni/Trino semantics. Callers that accept the SQL-canonical {@code \N} backslash syntax translate it first via
 * {@link RegexpReplaceUtf8#translateReplacement}.
 */
public final class JoniRegexpSupport
{
    private JoniRegexpSupport() {}

    /** Compile a UTF-8 pattern with the same encoding/syntax/options Trino uses for its Joni regexp type. */
    public static Regex compile(byte[] pattern)
    {
        return new Regex(pattern, 0, pattern.length, Option.DEFAULT, NonStrictUTF8Encoding.INSTANCE, Syntax.Java);
    }

    public static Regex compile(Slice pattern)
    {
        return compile(pattern.getBytes());
    }

    /** Replace every match of {@code pattern} in {@code source} with {@code replacement}. */
    public static Slice replace(Slice source, Regex pattern, Slice replacement, JoniRegexpPolicy policy)
    {
        byte[] sourceBytes = policy.zeroCopyMatcher() ? source.byteArray() : null;
        int sourceStart;
        if (sourceBytes == null) {
            sourceBytes = source.getBytes();
            sourceStart = 0;
        }
        else {
            sourceStart = source.byteArrayOffset();
        }
        int sourceEnd = sourceStart + source.length();
        Matcher matcher = pattern.matcher(sourceBytes, sourceStart, sourceEnd);
        SliceOutput output = new DynamicSliceOutput(source.length() + replacement.length() * 5);

        int lastEnd = 0;
        int nextStart = sourceStart;
        while (true) {
            int offset = matcher.search(nextStart, sourceEnd, Option.NONE);
            if (offset == -1) {
                break;
            }
            nextStart = sourceStart + nextStart(source, matcher);
            Slice unmatched = source.slice(lastEnd, matcher.getBegin() - lastEnd);
            lastEnd = matcher.getEnd();
            output.appendBytes(unmatched);
            appendReplacement(output, source, pattern, matcher.getEagerRegion(), replacement);
        }
        output.appendBytes(source.slice(lastEnd, source.length() - lastEnd));
        return output.slice();
    }

    /** Advance past a zero-width match by one code point so the search loop makes progress. */
    private static int nextStart(Slice source, Matcher matcher)
    {
        if (matcher.getEnd() == matcher.getBegin()) {
            if (matcher.getBegin() < source.length()) {
                return matcher.getEnd() + SliceUtf8.lengthOfCodePointFromStartByte(source.getByte(matcher.getBegin()));
            }
            return matcher.getEnd() + 1;
        }
        return matcher.getEnd();
    }

    private static void appendReplacement(SliceOutput result, Slice source, Regex pattern, Region region, Slice replacement)
    {
        int index = 0;
        while (index < replacement.length()) {
            byte current = replacement.getByte(index);
            if (current == '$') {
                index++;
                if (index == replacement.length()) {
                    throw new IllegalArgumentException("Illegal replacement sequence: " + replacement.toStringUtf8());
                }
                current = replacement.getByte(index);
                int backref;
                if (current == '{') {
                    index++;
                    int nameStart = index;
                    while (index < replacement.length()) {
                        current = replacement.getByte(index);
                        if (current == '}') {
                            break;
                        }
                        index++;
                    }
                    byte[] name = replacement.getBytes(nameStart, index - nameStart);
                    try {
                        backref = pattern.nameToBackrefNumber(name, 0, name.length, region);
                    }
                    catch (RuntimeException e) {
                        throw new IllegalArgumentException("invalid group name: " + new String(name, java.nio.charset.StandardCharsets.UTF_8), e);
                    }
                    index++;
                }
                else {
                    backref = current - '0';
                    if (backref < 0 || backref > 9) {
                        throw new IllegalArgumentException("Illegal replacement sequence: " + replacement.toStringUtf8());
                    }
                    if (region.numRegs <= backref) {
                        throw new IllegalArgumentException("Illegal back reference: " + backref);
                    }
                    index++;
                    while (index < replacement.length()) {
                        int nextDigit = replacement.getByte(index) - '0';
                        if (nextDigit < 0 || nextDigit > 9) {
                            break;
                        }
                        int candidate = backref * 10 + nextDigit;
                        if (region.numRegs <= candidate) {
                            break;
                        }
                        backref = candidate;
                        index++;
                    }
                }
                int begin = region.beg[backref];
                int end = region.end[backref];
                if (begin != -1 && end != -1) {
                    result.appendBytes(source.slice(begin, end - begin));
                }
            }
            else if (current == '\\') {
                index++;
                if (index == replacement.length()) {
                    throw new IllegalArgumentException("Illegal replacement sequence: " + replacement.toStringUtf8());
                }
                current = replacement.getByte(index);
                result.appendByte(current);
                index++;
            }
            else {
                result.appendByte(current);
                index++;
            }
        }
    }
}
