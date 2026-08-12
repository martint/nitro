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

/**
 * Aggregation state whose input batches are scheduled by an external host.
 */
public interface BatchAggregationSession
        extends AutoCloseable
{
    enum InputOwnership
    {
        CALLER,
        SESSION
    }

    Schema outputSchema();

    void addInput(Batch batch);

    default void addInput(Batch batch, long inputBytes)
    {
        addInput(batch);
    }

    /**
     * Adds input while allowing the session to take ownership of the batch. A caller receiving
     * {@link InputOwnership#SESSION} must not close the batch; the session or an output batch
     * returned by it will close the input instead.
     */
    default InputOwnership addInputWithOwnership(Batch batch, long inputBytes)
    {
        addInput(batch, inputBytes);
        return InputOwnership.CALLER;
    }

    default boolean hasOutput()
    {
        return false;
    }

    default Batch getOutput()
    {
        throw new IllegalStateException("aggregation session has no output");
    }

    default void flush()
    {
        throw new UnsupportedOperationException("aggregation session cannot flush");
    }

    /**
     * Bytes retained by live aggregation state, excluding reusable allocator pools.
     */
    long retainedBytes();

    default AggregationPhaseMetrics phaseMetrics()
    {
        return AggregationPhaseMetrics.EMPTY;
    }

    Batch finish();

    @Override
    void close();
}
