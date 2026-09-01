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

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/// Engine-neutral navigation within a row-pattern match.
///
/// An empty label set denotes the universal pattern variable. A logical offset advances among matching labels;
/// the physical offset then moves across ordinary partition rows, independently of their labels.
public final class PatternNavigation
{
    private final int[] labelOrdinals;
    private final Origin origin;
    private final Scope scope;
    private final int logicalOffset;
    private final int physicalOffset;

    public PatternNavigation(int[] labelOrdinals, Origin origin, Scope scope, int logicalOffset, int physicalOffset)
    {
        requireNonNull(labelOrdinals, "labelOrdinals is null");
        this.origin = requireNonNull(origin, "origin is null");
        this.scope = requireNonNull(scope, "scope is null");
        if (logicalOffset < 0) {
            throw new IllegalArgumentException("logicalOffset is negative");
        }
        this.logicalOffset = logicalOffset;
        this.physicalOffset = physicalOffset;
        this.labelOrdinals = normalizedOrdinals(labelOrdinals);
    }

    public int[] labelOrdinals()
    {
        return labelOrdinals.clone();
    }

    public Origin origin()
    {
        return origin;
    }

    public Scope scope()
    {
        return scope;
    }

    public int logicalOffset()
    {
        return logicalOffset;
    }

    public int physicalOffset()
    {
        return physicalOffset;
    }

    /// Resolves to an absolute position within the partition, or {@code -1} when no position is visible.
    public int resolvePosition(
            int currentRow,
            PatternLabelEvaluator.LabelHistory matchedLabels,
            int searchStart,
            int searchEnd,
            int patternStart)
    {
        requireNonNull(matchedLabels, "matchedLabels is null");
        if (searchStart < 0 || searchEnd < searchStart) {
            throw new IllegalArgumentException("invalid search range");
        }
        if (patternStart < searchStart || patternStart > searchEnd - matchedLabels.size()) {
            throw new IllegalArgumentException("match is outside the search range");
        }
        if (currentRow < patternStart || currentRow >= patternStart + matchedLabels.size()) {
            throw new IllegalArgumentException("currentRow is outside the match");
        }

        int visibleEnd = scope == Scope.RUNNING
                ? currentRow - patternStart
                : matchedLabels.size() - 1;
        int relativePosition = origin == Origin.LAST
                ? findLast(matchedLabels, visibleEnd)
                : findFirst(matchedLabels, visibleEnd);
        if (relativePosition < 0) {
            return -1;
        }
        long target = (long) patternStart + relativePosition + physicalOffset;
        if (target < searchStart || target >= searchEnd) {
            return -1;
        }
        return (int) target;
    }

    private int findLast(PatternLabelEvaluator.LabelHistory labels, int visibleEnd)
    {
        int remaining = logicalOffset;
        for (int position = visibleEnd; position >= 0; position--) {
            if (includes(labels.labelAt(position)) && remaining-- == 0) {
                return position;
            }
        }
        return -1;
    }

    private int findFirst(PatternLabelEvaluator.LabelHistory labels, int visibleEnd)
    {
        int remaining = logicalOffset;
        for (int position = 0; position <= visibleEnd; position++) {
            if (includes(labels.labelAt(position)) && remaining-- == 0) {
                return position;
            }
        }
        return -1;
    }

    private boolean includes(int labelOrdinal)
    {
        return labelOrdinals.length == 0 || Arrays.binarySearch(labelOrdinals, labelOrdinal) >= 0;
    }

    private static int[] normalizedOrdinals(int[] ordinals)
    {
        int[] normalized = ordinals.clone();
        Arrays.sort(normalized);
        int unique = 0;
        for (int ordinal : normalized) {
            if (ordinal < 0) {
                throw new IllegalArgumentException("label ordinal is negative");
            }
            if (unique == 0 || normalized[unique - 1] != ordinal) {
                normalized[unique++] = ordinal;
            }
        }
        return Arrays.copyOf(normalized, unique);
    }

    public enum Origin
    {
        FIRST,
        LAST,
    }

    public enum Scope
    {
        RUNNING,
        FINAL,
    }
}
