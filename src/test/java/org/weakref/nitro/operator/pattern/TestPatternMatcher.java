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

import org.junit.jupiter.api.Test;
import org.weakref.nitro.core.execution.ExecutionContext;
import org.weakref.nitro.core.execution.ExecutionDiagnostics;
import org.weakref.nitro.core.execution.ExecutionPolicy;
import org.weakref.nitro.core.execution.ExecutionSuspension;
import org.weakref.nitro.core.execution.MemoryReservation;
import org.weakref.nitro.data.PrimitiveArrayPool;

import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestPatternMatcher
{
    @Test
    void testMatchesLabelsAnchorsAndExclusions()
    {
        PatternExpression pattern = new PatternExpression.Concatenation(List.of(
                new PatternExpression.Anchor(PatternExpression.Anchor.Type.PARTITION_START),
                new PatternExpression.Label(0),
                new PatternExpression.Exclusion(new PatternExpression.Label(1)),
                new PatternExpression.Anchor(PatternExpression.Anchor.Type.PARTITION_END)));

        try (PrimitiveArrayPool pool = new PrimitiveArrayPool(1 << 20, 0);
                PatternMatcher.Session session = new PatternMatcher(PatternCompiler.compile(pattern), pool)
                        .start(2, true, (_, _, _) -> true)) {
            assertThat(session.run(new TestingExecutionContext())).isTrue();
            assertThat(session.labelCount()).isEqualTo(2);
            assertThat(session.labelAt(0)).isZero();
            assertThat(session.labelAt(1)).isOne();
            assertThat(session.exclusionCount()).isEqualTo(2);
            assertThat(session.exclusionAt(0)).isEqualTo(1);
            assertThat(session.exclusionAt(1)).isEqualTo(2);
        }
    }

    @Test
    void testHonorsAlternationAndQuantifierPreference()
    {
        PatternExpression alternatives = new PatternExpression.Alternation(List.of(
                new PatternExpression.Label(0),
                new PatternExpression.Label(1)));
        try (PrimitiveArrayPool pool = new PrimitiveArrayPool(1 << 20, 0);
                PatternMatcher.Session session = new PatternMatcher(PatternCompiler.compile(alternatives), pool)
                        .start(1, true, (label, _, _) -> label == 1)) {
            assertThat(session.run(new TestingExecutionContext())).isTrue();
            assertThat(session.labelAt(0)).isOne();
        }

        PatternExpression label = new PatternExpression.Label(0);
        PatternExpression greedy = new PatternExpression.Quantified(label, 0, OptionalInt.empty(), true);
        PatternExpression reluctant = new PatternExpression.Quantified(label, 0, OptionalInt.empty(), false);
        try (PrimitiveArrayPool pool = new PrimitiveArrayPool(1 << 20, 0);
                PatternMatcher.Session greedySession = new PatternMatcher(PatternCompiler.compile(greedy), pool)
                        .start(3, true, (_, _, _) -> true);
                PatternMatcher.Session reluctantSession = new PatternMatcher(PatternCompiler.compile(reluctant), pool)
                        .start(3, true, (_, _, _) -> true)) {
            assertThat(greedySession.run(new TestingExecutionContext())).isTrue();
            assertThat(greedySession.labelCount()).isEqualTo(3);
            assertThat(reluctantSession.run(new TestingExecutionContext())).isTrue();
            assertThat(reluctantSession.labelCount()).isZero();
        }
    }

    @Test
    void testEvaluatorSeesTentativeHistory()
    {
        PatternExpression pattern = new PatternExpression.Concatenation(List.of(
                new PatternExpression.Label(2),
                new PatternExpression.Label(3)));
        try (PrimitiveArrayPool pool = new PrimitiveArrayPool(1 << 20, 0);
                PatternMatcher.Session session = new PatternMatcher(PatternCompiler.compile(pattern), pool)
                        .start(2, true, (label, position, history) ->
                                history.size() == position + 1 && history.labelAt(position) == label)) {
            assertThat(session.run(new TestingExecutionContext())).isTrue();
            assertThat(session.labelAt(0)).isEqualTo(2);
            assertThat(session.labelAt(1)).isEqualTo(3);
        }
    }

    @Test
    void testGreedyPrefixFallsBackToFollowingLabel()
    {
        PatternExpression pattern = new PatternExpression.Concatenation(List.of(
                new PatternExpression.Quantified(
                        new PatternExpression.Label(0),
                        0,
                        OptionalInt.empty(),
                        true),
                new PatternExpression.Label(1)));
        int[] input = {0, 0, 1, 2};
        try (PrimitiveArrayPool pool = new PrimitiveArrayPool(1 << 20, 0);
                PatternMatcher.Session session = new PatternMatcher(PatternCompiler.compile(pattern), pool)
                        .start(input.length, true, (label, position, _) -> input[position] == label)) {
            assertThat(session.run(new TestingExecutionContext())).isTrue();
            int[] labels = new int[session.labelCount()];
            session.copyLabelsTo(labels, 0);
            assertThat(labels).containsExactly(0, 0, 1);
        }

        int[] noMatch = {0, 0, 2};
        try (PrimitiveArrayPool pool = new PrimitiveArrayPool(1 << 20, 0);
                PatternMatcher.Session session = new PatternMatcher(PatternCompiler.compile(pattern), pool)
                        .start(noMatch.length, true, (label, position, _) -> noMatch[position] == label)) {
            assertThat(session.run(new TestingExecutionContext())).isFalse();
            assertThat(session.matched()).isFalse();
        }
    }

    @Test
    void testResumesAfterCooperativeYield()
    {
        PatternExpression pattern = new PatternExpression.Quantified(
                new PatternExpression.Label(0),
                3,
                OptionalInt.of(3),
                true);
        YieldOnceExecutionContext context = new YieldOnceExecutionContext();
        try (PrimitiveArrayPool pool = new PrimitiveArrayPool(1 << 20, 0);
                PatternMatcher.Session session = new PatternMatcher(PatternCompiler.compile(pattern), pool)
                        .start(3, true, (_, _, _) -> true)) {
            assertThatThrownBy(() -> session.run(context))
                    .isInstanceOf(ExecutionSuspension.class);
            assertThat(session.run(context)).isTrue();
            assertThat(session.labelCount()).isEqualTo(3);
        }
    }

    @Test
    void testPrunesEmptyInstructionCycle()
    {
        PatternExpression pattern = new PatternExpression.Quantified(
                PatternExpression.Empty.EMPTY,
                0,
                OptionalInt.empty(),
                true);
        try (PrimitiveArrayPool pool = new PrimitiveArrayPool(1 << 20, 0);
                PatternMatcher.Session session = new PatternMatcher(PatternCompiler.compile(pattern), pool)
                        .start(0, true, (_, _, _) -> true)) {
            assertThat(session.run(new TestingExecutionContext())).isTrue();
            assertThat(session.labelCount()).isZero();
        }
    }

    @Test
    void testReturnsAllWorkspaceToPool()
    {
        try (PrimitiveArrayPool pool = new PrimitiveArrayPool(1 << 20, 0)) {
            PatternMatcher matcher = new PatternMatcher(
                    PatternCompiler.compile(new PatternExpression.Label(0)),
                    pool);
            try (PatternMatcher.Session session = matcher.start(1, true, (_, _, _) -> true)) {
                assertThat(session.run(new TestingExecutionContext())).isTrue();
            }
            long allocations = pool.allocationCount();
            try (PatternMatcher.Session session = matcher.start(1, true, (_, _, _) -> true)) {
                assertThat(session.run(new TestingExecutionContext())).isTrue();
            }
            assertThat(pool.allocationCount()).isEqualTo(allocations);
            assertThat(pool.reuseCount()).isPositive();
        }
    }

    private static class TestingExecutionContext
            implements ExecutionContext
    {
        private final MemoryReservation memory = new TestingMemoryReservation();

        @Override
        public MemoryReservation memory()
        {
            return memory;
        }

        @Override
        public ExecutionPolicy policy()
        {
            return new ExecutionPolicy() {};
        }

        @Override
        public ExecutionDiagnostics diagnostics()
        {
            return (_, _) -> {};
        }

        @Override
        public boolean isYieldRequested()
        {
            return false;
        }

        @Override
        public boolean isCancelled()
        {
            return false;
        }

        @Override
        public void requestMemoryRevocation() {}
    }

    private static final class YieldOnceExecutionContext
            extends TestingExecutionContext
    {
        private boolean yield = true;

        @Override
        public boolean isYieldRequested()
        {
            boolean result = yield;
            yield = false;
            return result;
        }
    }

    private static final class TestingMemoryReservation
            implements MemoryReservation
    {
        @Override
        public CompletionStage<Void> reserve(long bytes)
        {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void release(long bytes) {}

        @Override
        public long reservedBytes()
        {
            return 0;
        }
    }
}
