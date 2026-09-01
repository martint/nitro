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

import org.weakref.nitro.operator.RowPositionIndex;

import static java.util.Objects.requireNonNull;

/// Borrowed row and match state visible to one registry-bound pattern definition invocation.
public final class PatternEvaluationContext
{
    private final RowPositionIndex rows;
    private int partitionStart;
    private int partitionEnd;
    private int patternStart;
    private long matchNumber;
    private int currentRow;
    private PatternLabelEvaluator.LabelHistory labels;

    PatternEvaluationContext(RowPositionIndex rows)
    {
        this.rows = requireNonNull(rows, "rows is null");
    }

    void resetMatch(int partitionStart, int partitionEnd, int patternStart, long matchNumber)
    {
        if (partitionStart < 0 || partitionStart > patternStart || patternStart > partitionEnd || partitionEnd > rows.size()) {
            throw new IllegalArgumentException("invalid pattern bounds");
        }
        if (matchNumber <= 0) {
            throw new IllegalArgumentException("matchNumber is not positive");
        }
        this.partitionStart = partitionStart;
        this.partitionEnd = partitionEnd;
        this.patternStart = patternStart;
        this.matchNumber = matchNumber;
    }

    void resetRow(int currentRow, PatternLabelEvaluator.LabelHistory labels)
    {
        if (currentRow < patternStart || currentRow >= partitionEnd) {
            throw new IllegalArgumentException("currentRow is outside pattern search bounds");
        }
        this.currentRow = currentRow;
        this.labels = requireNonNull(labels, "labels is null");
    }

    public RowPositionIndex rows()
    {
        return rows;
    }

    public int currentRow()
    {
        return currentRow;
    }

    public int partitionStart()
    {
        return partitionStart;
    }

    public int partitionEnd()
    {
        return partitionEnd;
    }

    public int patternStart()
    {
        return patternStart;
    }

    public long matchNumber()
    {
        return matchNumber;
    }

    public PatternLabelEvaluator.LabelHistory labels()
    {
        return labels;
    }

    /// Resolves a navigation descriptor to an absolute row in {@link #rows()}, or {@code -1} when it has no value.
    public int resolvePosition(PatternNavigation navigation)
    {
        return requireNonNull(navigation, "navigation is null")
                .resolvePosition(currentRow, labels, partitionStart, partitionEnd, patternStart);
    }

    /// Returns the label at the resolved match position, or {@code -1} when the navigation has no classifier.
    public int classifier(PatternNavigation navigation)
    {
        int position = resolvePosition(navigation);
        int relativePosition = position - patternStart;
        if (position < 0 || relativePosition < 0 || relativePosition >= labels.size()) {
            return -1;
        }
        return labels.labelAt(relativePosition);
    }
}
