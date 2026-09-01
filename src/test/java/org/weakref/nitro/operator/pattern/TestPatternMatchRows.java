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
import org.weakref.nitro.core.execution.MemoryReservation;
import org.weakref.nitro.data.PrimitiveArrayPool;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.assertj.core.api.Assertions.assertThat;

final class TestPatternMatchRows
{
    @Test
    void testSkipsExclusionRangesWithoutCopyingLabels()
    {
        PatternExpression pattern = new PatternExpression.Concatenation(List.of(
                new PatternExpression.Label(0),
                new PatternExpression.Exclusion(new PatternExpression.Concatenation(List.of(
                        new PatternExpression.Label(1),
                        new PatternExpression.Label(1)))),
                new PatternExpression.Label(2)));

        try (PrimitiveArrayPool arrays = new PrimitiveArrayPool(1 << 20, 0);
                PatternMatcher.Session match = new PatternMatcher(PatternCompiler.compile(pattern), arrays)
                        .start(4, true, (_, _, _) -> true)) {
            assertThat(match.run(new TestingExecutionContext())).isTrue();

            PatternMatchRows rows = new PatternMatchRows();
            rows.reset(match, 10);
            assertThat(rows.advance()).isTrue();
            assertThat(rows.position()).isEqualTo(10);
            assertThat(rows.relativePosition()).isZero();
            assertThat(rows.labelOrdinal()).isZero();
            assertThat(rows.advance()).isTrue();
            assertThat(rows.position()).isEqualTo(13);
            assertThat(rows.labelOrdinal()).isEqualTo(2);
            assertThat(rows.advance()).isFalse();
        }
    }

    private static final class TestingExecutionContext
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
