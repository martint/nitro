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

import org.weakref.nitro.core.execution.ExecutionContext;

import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

/// Converts one partition's match attempts into SQL row-pattern output rows.
///
/// The cursor keeps accepted matcher state alive while ALL ROWS PER MATCH emits its rows. It exposes row identity
/// and a positioned measure context separately: an unmatched row copies ordinary input values but produces NULL for
/// every measure, while an empty or non-empty match evaluates measures through the registry-bound value program.
public final class PatternPartitionOutput
        implements AutoCloseable
{
    public enum Kind
    {
        MATCH,
        EMPTY_MATCH,
        UNMATCHED
    }

    private final PatternPartitionCursor matches;
    private final PatternOutputMode mode;

    private int lastMatchedPosition = -1;
    private int nextMatchRow;
    private int matchRows;
    private int sourcePosition;
    private int measurePosition;
    private Kind kind;
    private boolean positioned;
    private boolean closed;

    public PatternPartitionOutput(PatternPartitionCursor matches, PatternOutputMode mode)
    {
        this.matches = requireNonNull(matches, "matches is null");
        this.mode = requireNonNull(mode, "mode is null");
        if (mode == PatternOutputMode.WINDOW) {
            throw new IllegalArgumentException("WINDOW pattern output requires frame-aware execution");
        }
    }

    /// Advances to the next visible result row. Matcher suspension leaves both cursors restartable.
    public boolean advance(ExecutionContext context)
    {
        requireNonNull(context, "context is null");
        checkOpen();
        positioned = false;

        while (true) {
            if (nextMatchRow < matchRows && positionNextMatchRow()) {
                return true;
            }
            if (!matches.advance(context)) {
                return false;
            }

            if (!matches.matched()) {
                if (mode.outputsUnmatchedRows() && matches.inputStart() > lastMatchedPosition) {
                    position(Kind.UNMATCHED, matches.inputStart(), -1);
                    return true;
                }
                continue;
            }

            PatternMatcher.Session match = matches.match();
            int size = match.size();
            if (size == 0) {
                if (mode.outputsEmptyMatches()) {
                    position(Kind.EMPTY_MATCH, matches.inputStart(), matches.patternStart());
                    return true;
                }
                continue;
            }

            lastMatchedPosition = max(lastMatchedPosition, matches.patternStart() + size - 1);
            if (mode.isOneRow()) {
                position(Kind.MATCH, matches.inputStart(), matches.patternStart() + size - 1);
                return true;
            }

            nextMatchRow = 0;
            matchRows = size;
        }
    }

    public Kind kind()
    {
        checkPositioned();
        return kind;
    }

    /// The buffered input row whose pass-through values form this output row.
    public int sourcePosition()
    {
        checkPositioned();
        return sourcePosition;
    }

    /// Returns a positioned context for measure evaluation, or {@code null} for an unmatched row.
    public PatternEvaluationContext measureContext()
    {
        checkPositioned();
        if (kind == Kind.UNMATCHED) {
            return null;
        }
        return matches.positionMatch(measurePosition);
    }

    private boolean positionNextMatchRow()
    {
        PatternMatcher.Session match = matches.match();
        while (nextMatchRow < matchRows) {
            int relativePosition = nextMatchRow++;
            if (isExcluded(match, relativePosition)) {
                continue;
            }
            int position = matches.patternStart() + relativePosition;
            position(Kind.MATCH, position, position);
            return true;
        }
        return false;
    }

    private static boolean isExcluded(PatternMatcher.Session match, int position)
    {
        for (int index = 0; index < match.exclusionCount(); index += 2) {
            if (position >= match.exclusionAt(index) && position < match.exclusionAt(index + 1)) {
                return true;
            }
        }
        return false;
    }

    private void position(Kind kind, int sourcePosition, int measurePosition)
    {
        this.kind = requireNonNull(kind, "kind is null");
        this.sourcePosition = sourcePosition;
        this.measurePosition = measurePosition;
        positioned = true;
    }

    private void checkPositioned()
    {
        checkOpen();
        if (!positioned) {
            throw new IllegalStateException("pattern output is not positioned");
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("pattern output is closed");
        }
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        matches.close();
    }
}
