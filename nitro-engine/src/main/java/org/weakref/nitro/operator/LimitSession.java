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
package org.weakref.nitro.operator;

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Host-driven streaming limit state. The caller retains ownership of each input batch.
 */
public final class LimitSession
        implements AutoCloseable
{
    private final Allocator.Context allocationContext = new Allocator.Context("LimitSession", LimitSession.class);
    private final Allocator allocator;
    private long remaining;
    private boolean closed;

    public LimitSession(Allocator allocator, long limit)
    {
        if (limit < 0) {
            throw new IllegalArgumentException("limit is negative");
        }
        this.allocator = requireNonNull(allocator, "allocator is null");
        remaining = limit;
    }

    public Optional<Mask> select(Batch batch)
    {
        requireNonNull(batch, "batch is null");
        checkOpen();
        if (remaining == 0) {
            return Optional.empty();
        }
        Mask source = batch.borrowMask();
        int selected = (int) Math.min(remaining, source.count());
        Mask mask = allocator.firstMask(allocationContext, source, selected);
        batch.constrain(mask);
        remaining -= selected;
        return Optional.of(mask);
    }

    public boolean isFinished()
    {
        checkOpen();
        return remaining == 0;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        allocator.release(allocationContext);
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("Limit session is closed");
        }
    }
}
