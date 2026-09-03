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
package org.weakref.nitro.function.scalar;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.execution.EngineResources;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestPrimitiveExecutionContext
{
    @Test
    void testClosesExecutionLocalState()
    {
        AtomicBoolean closed = new AtomicBoolean();
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                PrimitiveExecutionContext context = new PrimitiveExecutionContext(allocator)) {
            context.state(this, () -> (AutoCloseable) () -> closed.set(true));
        }

        assertThat(closed).isTrue();
    }

    @Test
    void testAttemptsToCloseEveryStateAfterFailure()
    {
        AtomicBoolean secondClosed = new AtomicBoolean();
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            PrimitiveExecutionContext context = new PrimitiveExecutionContext(allocator);
            Object failingKey = new Object();
            context.state(failingKey, () -> (AutoCloseable) () -> {
                throw new Exception("expected");
            });
            context.state(this, () -> (AutoCloseable) () -> secondClosed.set(true));

            assertThatThrownBy(context::close)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Failed to close primitive execution state")
                    .hasRootCauseMessage("expected");
            assertThat(secondClosed).isTrue();

            context.close();
        }
    }
}
