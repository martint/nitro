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

import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.operator.source.ExternallyScheduledSource;

import static java.util.Objects.requireNonNull;

final class ExternallyScheduledBatchFeed
        implements ExternallyScheduledSource
{
    private final Schema schema;

    private Batch input;
    private boolean emitted;
    private boolean finished;
    private boolean closed;

    ExternallyScheduledBatchFeed(Schema schema)
    {
        this.schema = requireNonNull(schema, "schema is null");
    }

    void addInput(Batch batch)
    {
        if (closed || finished) {
            throw new IllegalStateException("batch feed is closed");
        }
        if (input != null) {
            throw new IllegalStateException("batch feed already has input");
        }
        input = requireNonNull(batch, "batch is null");
        emitted = false;
    }

    boolean hasInput()
    {
        return input != null;
    }

    void releaseInput()
    {
        input = null;
        emitted = false;
    }

    void finish()
    {
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public int outputCount()
    {
        return schema.size();
    }

    @Override
    public Schema outputSchema()
    {
        return schema;
    }

    @Override
    public boolean hasNext()
    {
        return !closed && input != null && !emitted;
    }

    @Override
    public Batch next()
    {
        if (!hasNext()) {
            throw new IllegalStateException("no scheduled input");
        }
        emitted = true;
        return input;
    }

    @Override
    public void constrain(Mask mask)
    {
        if (input == null) {
            throw new IllegalStateException("no scheduled input");
        }
        input.constrain(requireNonNull(mask, "mask is null"));
    }

    @Override
    public boolean supportsStableBatchBorrow()
    {
        return true;
    }

    @Override
    public boolean supportsOpenBatchHasNext()
    {
        return true;
    }

    @Override
    public boolean supportsConstrainedReborrow()
    {
        return true;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        if (!emitted && input != null) {
            input.close();
        }
        input = null;
    }
}
