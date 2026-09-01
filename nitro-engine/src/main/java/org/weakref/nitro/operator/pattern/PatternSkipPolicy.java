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

/// Immutable engine-neutral rule for selecting the next search row after an accepted match.
public sealed interface PatternSkipPolicy
        permits PatternSkipPolicy.Fixed, PatternSkipPolicy.ToLabel
{
    int nextStart(
            int inputStart,
            int searchStart,
            int searchEnd,
            int patternStart,
            PatternLabelEvaluator.LabelHistory labels);

    enum Fixed
            implements PatternSkipPolicy
    {
        NEXT {
            @Override
            public int nextStart(
                    int inputStart,
                    int searchStart,
                    int searchEnd,
                    int patternStart,
                    PatternLabelEvaluator.LabelHistory labels)
            {
                validateMatch(inputStart, searchStart, searchEnd, patternStart, labels);
                return inputStart + 1;
            }
        },
        PAST_LAST {
            @Override
            public int nextStart(
                    int inputStart,
                    int searchStart,
                    int searchEnd,
                    int patternStart,
                    PatternLabelEvaluator.LabelHistory labels)
            {
                validateMatch(inputStart, searchStart, searchEnd, patternStart, labels);
                return patternStart + labels.size();
            }
        },
    }

    record ToLabel(PatternNavigation navigation)
            implements PatternSkipPolicy
    {
        public ToLabel
        {
            requireNonNull(navigation, "navigation is null");
        }

        @Override
        public int nextStart(
                int inputStart,
                int searchStart,
                int searchEnd,
                int patternStart,
                PatternLabelEvaluator.LabelHistory labels)
        {
            validateMatch(inputStart, searchStart, searchEnd, patternStart, labels);
            int position = navigation.resolvePosition(
                    patternStart + labels.size() - 1,
                    labels,
                    searchStart,
                    searchEnd,
                    patternStart);
            if (position < 0) {
                throw new IllegalStateException("AFTER MATCH SKIP target is absent");
            }
            if (position == patternStart) {
                throw new IllegalStateException("AFTER MATCH SKIP cannot target the first row of the match");
            }
            return position;
        }
    }

    private static void validateMatch(
            int inputStart,
            int searchStart,
            int searchEnd,
            int patternStart,
            PatternLabelEvaluator.LabelHistory labels)
    {
        requireNonNull(labels, "labels is null");
        if (searchStart < 0 ||
                searchStart > inputStart ||
                inputStart > patternStart ||
                patternStart > searchEnd - labels.size()) {
            throw new IllegalArgumentException("invalid match bounds");
        }
        if (labels.size() == 0) {
            throw new IllegalArgumentException("match is empty");
        }
    }
}
