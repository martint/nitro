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

/// Restartable INITIAL or SEEK search over one ordered partition range.
public final class PatternSearch
{
    private final PatternMatcher matcher;
    private final PatternDefinitionEvaluator evaluator;

    public PatternSearch(PatternMatcher matcher, PatternDefinitionEvaluator evaluator)
    {
        this.matcher = requireNonNull(matcher, "matcher is null");
        this.evaluator = requireNonNull(evaluator, "evaluator is null");
    }

    public Session start(int partitionStart, int partitionEnd, int attemptStart, boolean initial, long matchNumber)
    {
        if (partitionStart < 0 || partitionStart > attemptStart || attemptStart > partitionEnd) {
            throw new IllegalArgumentException("invalid search bounds");
        }
        return new Session(partitionStart, partitionEnd, attemptStart, initial, matchNumber);
    }

    public final class Session
            implements AutoCloseable
    {
        private final int partitionStart;
        private final int partitionEnd;
        private final boolean initial;
        private final long matchNumber;
        private int patternStart;
        private PatternMatcher.Session match;
        private boolean complete;
        private boolean closed;

        private Session(int partitionStart, int partitionEnd, int patternStart, boolean initial, long matchNumber)
        {
            this.partitionStart = partitionStart;
            this.partitionEnd = partitionEnd;
            this.patternStart = patternStart;
            this.initial = initial;
            this.matchNumber = matchNumber;
            startAttempt();
        }

        /// Runs until the search completes or the execution context suspends at a matcher checkpoint.
        public boolean run(ExecutionContext context)
        {
            requireNonNull(context, "context is null");
            checkOpen();
            while (!complete) {
                if (match.run(context)) {
                    complete = true;
                    return true;
                }
                if (initial || patternStart >= partitionEnd - 1) {
                    complete = true;
                    return false;
                }
                match.close();
                patternStart++;
                startAttempt();
            }
            return match.matched();
        }

        public boolean isComplete()
        {
            checkOpen();
            return complete;
        }

        public boolean matched()
        {
            checkComplete();
            return match.matched();
        }

        public int patternStart()
        {
            checkComplete();
            return patternStart;
        }

        public PatternMatcher.Session match()
        {
            checkComplete();
            return match;
        }

        private void startAttempt()
        {
            evaluator.reset(partitionStart, partitionEnd, patternStart, matchNumber);
            match = matcher.start(
                    partitionEnd - patternStart,
                    patternStart == partitionStart,
                    evaluator);
        }

        private void checkComplete()
        {
            checkOpen();
            if (!complete) {
                throw new IllegalStateException("pattern search is not complete");
            }
        }

        private void checkOpen()
        {
            if (closed) {
                throw new IllegalStateException("pattern search is closed");
            }
        }

        @Override
        public void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            match.close();
        }
    }
}
