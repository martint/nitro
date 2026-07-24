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
package org.weakref.nitro.operator.source;

import org.weakref.nitro.core.source.BatchSource;
import org.weakref.nitro.core.source.SourceCapability;
import org.weakref.nitro.core.source.SourcePoll;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.operator.Operator;

import java.util.EnumSet;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/// Transitional adapter that exposes an existing native source through the new source port.
public final class OperatorBatchSource
        implements BatchSource
{
    private final Operator source;
    private final Set<SourceCapability> capabilities;
    private boolean closed;

    public OperatorBatchSource(Operator source)
    {
        this.source = requireNonNull(source, "source is null");
        EnumSet<SourceCapability> capabilities = EnumSet.of(SourceCapability.LAZY_COLUMNS, SourceCapability.SELECTION_PUSHDOWN);
        if (source.supportsRetainedBatches()) {
            capabilities.add(SourceCapability.RETAINED_BATCHES);
        }
        if (source.supportsStableBatchBorrow()) {
            capabilities.add(SourceCapability.STABLE_BATCH_BORROW);
        }
        if (source.supportsConstrainedReborrow()) {
            capabilities.add(SourceCapability.CONSTRAINED_REBORROW);
        }
        this.capabilities = Set.copyOf(capabilities);
    }

    @Override
    public Schema schema()
    {
        return source.outputSchema();
    }

    @Override
    public Set<SourceCapability> capabilities()
    {
        return capabilities;
    }

    @Override
    public SourcePoll poll()
    {
        checkOpen();
        if (!source.hasNext()) {
            return SourcePoll.Finished.FINISHED;
        }
        return new SourcePoll.Ready(new NativeSourceBatch(
                schema(),
                source.supportsRetainedBatches(),
                source.supportsStableBatchBorrow(),
                source.next()));
    }

    @Override
    public void close()
    {
        if (!closed) {
            closed = true;
            source.close();
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("source is closed");
        }
    }
}
