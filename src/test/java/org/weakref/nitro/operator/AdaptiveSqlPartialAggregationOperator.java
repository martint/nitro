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
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.aggregation.Accumulator;
import org.weakref.nitro.operator.aggregation.PhysicalAggregationProgram;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/** Streaming test-harness model of a Trino adaptive partial aggregation plan node. */
public final class AdaptiveSqlPartialAggregationOperator
        implements Operator
{
    private final Operator source;
    private final GroupedAggregationSession[] sessions;
    private final AdaptiveControl control;
    private final long maxRetainedBytes;
    private final Schema outputSchema;

    private Batch pendingOutput;
    private int nextInputPartition;
    private int finishingPartition;
    private boolean sourceFinished;
    private boolean closed;

    public AdaptiveSqlPartialAggregationOperator(
            Allocator allocator,
            Operator source,
            int partitionCount,
            List<Integer> groupByColumns,
            Supplier<List<Accumulator>> accumulators,
            long maxRetainedBytes,
            double uniqueRowsRatioThreshold)
    {
        requireNonNull(allocator, "allocator is null");
        this.source = requireNonNull(source, "source is null");
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be positive");
        }
        if (maxRetainedBytes <= 0) {
            throw new IllegalArgumentException("maxRetainedBytes must be positive");
        }
        this.maxRetainedBytes = maxRetainedBytes;
        control = new AdaptiveControl(maxRetainedBytes, uniqueRowsRatioThreshold);
        OperatorResources resources = EngineResources.from(allocator).operatorResources();
        requireNonNull(accumulators, "accumulators is null");
        sessions = new GroupedAggregationSession[partitionCount];
        for (int partition = 0; partition < partitionCount; partition++) {
            sessions[partition] = new GroupedAggregationSession(
                    allocator,
                    source.outputSchema(),
                    groupByColumns,
                    groupByColumns,
                    PhysicalAggregationProgram.independent(accumulators.get()),
                    resources,
                    control);
        }
        outputSchema = sessions[0].outputSchema();
    }

    @Override
    public int outputCount()
    {
        return outputSchema.size();
    }

    @Override
    public Schema outputSchema()
    {
        return outputSchema;
    }

    @Override
    public boolean hasNext()
    {
        if (pendingOutput != null) {
            return true;
        }
        while (!sourceFinished && source.hasNext()) {
            try (Batch batch = source.next()) {
                if (batch.borrowMask().none()) {
                    continue;
                }
                GroupedAggregationSession session = sessions[nextInputPartition];
                nextInputPartition = (nextInputPartition + 1) % sessions.length;
                session.addInput(batch, estimatedInputBytes(batch));
                if (session.hasOutput()) {
                    pendingOutput = session.getOutput();
                    return true;
                }
                if (session.retainedBytes() >= maxRetainedBytes) {
                    session.flush();
                    pendingOutput = session.getOutput();
                    return true;
                }
            }
        }
        if (!sourceFinished) {
            sourceFinished = true;
            source.close();
        }
        while (finishingPartition < sessions.length) {
            GroupedAggregationSession session = sessions[finishingPartition++];
            pendingOutput = session.finish();
            if (pendingOutput.borrowMask().count() > 0) {
                return true;
            }
            pendingOutput.close();
            pendingOutput = null;
        }
        return false;
    }

    @Override
    public Batch next()
    {
        if (!hasNext()) {
            throw new IllegalStateException("No more batches");
        }
        Batch output = pendingOutput;
        pendingOutput = null;
        return output;
    }

    @Override
    public void constrain(Mask mask) {}

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        if (pendingOutput != null) {
            pendingOutput.close();
            pendingOutput = null;
        }
        source.close();
        for (GroupedAggregationSession session : sessions) {
            session.close();
        }
    }

    private long estimatedInputBytes(Batch batch)
    {
        Mask mask = batch.borrowMask();
        Set<Vector> vectors = Collections.newSetFromMap(new IdentityHashMap<>());
        long bytes = 0;
        for (int channel = 0; channel < source.outputCount(); channel++) {
            Output output = batch.output(channel);
            for (Stream stream : output.streams()) {
                Vector vector = output.borrow(stream, mask);
                if (vectors.add(vector)) {
                    bytes += vector.retainedBytes();
                }
            }
        }
        return bytes;
    }

    private static final class AdaptiveControl
            implements PartialAggregationControl
    {
        private static final double DISABLE_SAMPLE_FACTOR = 1.5;
        private static final double ENABLE_SAMPLE_FACTOR = DISABLE_SAMPLE_FACTOR * 200;

        private final long maxRetainedBytes;
        private final double uniqueRowsRatioThreshold;
        private volatile boolean disabled;
        private long inputBytes;
        private long inputRows;
        private long outputRows;

        private AdaptiveControl(long maxRetainedBytes, double uniqueRowsRatioThreshold)
        {
            this.maxRetainedBytes = maxRetainedBytes;
            this.uniqueRowsRatioThreshold = uniqueRowsRatioThreshold;
        }

        @Override
        public boolean aggregationEnabled()
        {
            return !disabled;
        }

        @Override
        public synchronized void onAggregatedFlush(long inputBytes, long inputRows, long outputRows)
        {
            this.inputBytes += inputBytes;
            this.inputRows += inputRows;
            this.outputRows += outputRows;
            if (!disabled && this.inputBytes >= maxRetainedBytes * DISABLE_SAMPLE_FACTOR &&
                    (double) this.outputRows / this.inputRows > uniqueRowsRatioThreshold) {
                disabled = true;
            }
        }

        @Override
        public synchronized void onPassthroughFlush(long inputBytes, long inputRows)
        {
            this.inputBytes += inputBytes;
            this.inputRows += inputRows;
            if (disabled && this.inputBytes >= maxRetainedBytes * ENABLE_SAMPLE_FACTOR) {
                this.inputBytes = 0;
                this.inputRows = 0;
                outputRows = 0;
                disabled = false;
            }
        }
    }
}
