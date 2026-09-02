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
package org.weakref.nitro.operator.pattern;

import static java.util.Objects.requireNonNull;

/// Resettable, allocation-free cursor over the non-excluded rows of an accepted match.
public final class PatternMatchRows
{
    private PatternMatcher.Session match;
    private int patternStart;
    private int relativePosition;
    private int exclusionIndex;
    private boolean positioned;

    public void reset(PatternMatcher.Session match, int patternStart)
    {
        this.match = requireNonNull(match, "match is null");
        if (!match.matched()) {
            throw new IllegalArgumentException("pattern did not match");
        }
        if (patternStart < 0) {
            throw new IllegalArgumentException("patternStart is negative");
        }
        if ((match.exclusionCount() & 1) != 0) {
            throw new IllegalArgumentException("match has an incomplete exclusion range");
        }
        this.patternStart = patternStart;
        relativePosition = -1;
        exclusionIndex = 0;
        positioned = false;
    }

    public boolean advance()
    {
        requireReset();
        relativePosition++;
        while (exclusionIndex < match.exclusionCount() && relativePosition >= match.exclusionAt(exclusionIndex)) {
            int start = match.exclusionAt(exclusionIndex);
            int end = match.exclusionAt(exclusionIndex + 1);
            if (start < 0 || start > end || end > match.size()) {
                throw new IllegalArgumentException("match has an invalid exclusion range");
            }
            if (relativePosition < end) {
                relativePosition = end;
            }
            exclusionIndex += 2;
        }
        positioned = relativePosition < match.size();
        return positioned;
    }

    public int position()
    {
        requirePositioned();
        return patternStart + relativePosition;
    }

    public int relativePosition()
    {
        requirePositioned();
        return relativePosition;
    }

    public int labelOrdinal()
    {
        requirePositioned();
        return match.labelAt(relativePosition);
    }

    private void requireReset()
    {
        if (match == null) {
            throw new IllegalStateException("match rows are not initialized");
        }
    }

    private void requirePositioned()
    {
        requireReset();
        if (!positioned) {
            throw new IllegalStateException("match rows are not positioned");
        }
    }
}
