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

import static java.util.Objects.requireNonNull;

/// Restartable match cursor for a non-window row-pattern partition.
public final class PatternPartitionCursor
        implements AutoCloseable
{
    private final PatternSearch search;
    private final PatternSkipPolicy skip;
    private final int partitionStart;
    private final int partitionEnd;
    private final boolean initial;
    private int nextInputStart;
    private int inputStart;
    private long nextMatchNumber = 1;
    private long matchNumber;
    private PatternSearch.Session current;
    private boolean positioned;
    private boolean closed;

    public PatternPartitionCursor(
            PatternSearch search,
            PatternSkipPolicy skip,
            int partitionStart,
            int partitionEnd)
    {
        this(search, skip, partitionStart, partitionEnd, true);
    }

    public PatternPartitionCursor(
            PatternSearch search,
            PatternSkipPolicy skip,
            int partitionStart,
            int partitionEnd,
            boolean initial)
    {
        this.search = requireNonNull(search, "search is null");
        this.skip = requireNonNull(skip, "skip is null");
        if (partitionStart < 0 || partitionStart > partitionEnd) {
            throw new IllegalArgumentException("invalid partition bounds");
        }
        this.partitionStart = partitionStart;
        this.partitionEnd = partitionEnd;
        this.initial = initial;
        nextInputStart = partitionStart;
    }

    /// Advances to the next matched or unmatched input row. Suspension leaves the active search intact.
    public boolean advance(ExecutionContext context)
    {
        requireNonNull(context, "context is null");
        checkOpen();
        if (positioned) {
            closeCurrent();
            positioned = false;
        }
        if (current == null) {
            if (nextInputStart >= partitionEnd) {
                return false;
            }
            inputStart = nextInputStart;
            current = search.start(partitionStart, partitionEnd, inputStart, initial, nextMatchNumber);
        }

        boolean matched = current.run(context);
        positioned = true;
        if (!matched) {
            nextInputStart = inputStart + 1;
            return true;
        }

        matchNumber = nextMatchNumber++;
        PatternMatcher.Session match = current.match();
        nextInputStart = match.size() == 0
                ? inputStart + 1
                : skip.nextStart(inputStart, partitionStart, partitionEnd, current.patternStart(), match);
        return true;
    }

    public boolean matched()
    {
        checkPositioned();
        return current.matched();
    }

    public int inputStart()
    {
        checkPositioned();
        return inputStart;
    }

    public int patternStart()
    {
        checkPositioned();
        return current.patternStart();
    }

    public long matchNumber()
    {
        checkMatched();
        return matchNumber;
    }

    public PatternMatcher.Session match()
    {
        checkMatched();
        return current.match();
    }

    /// Positions the match evaluator for a measure emitted from the current accepted match.
    public PatternEvaluationContext positionMatch(int currentRow)
    {
        checkMatched();
        return search.positionMatch(
                partitionStart,
                partitionEnd,
                current.patternStart(),
                matchNumber,
                currentRow,
                current.match());
    }

    private void checkMatched()
    {
        checkPositioned();
        if (!current.matched()) {
            throw new IllegalStateException("cursor is positioned at an unmatched row");
        }
    }

    private void checkPositioned()
    {
        checkOpen();
        if (!positioned) {
            throw new IllegalStateException("cursor is not positioned");
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("cursor is closed");
        }
    }

    private void closeCurrent()
    {
        if (current != null) {
            current.close();
            current = null;
        }
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        closeCurrent();
    }
}
