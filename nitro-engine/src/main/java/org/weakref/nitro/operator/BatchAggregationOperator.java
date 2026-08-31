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
import org.weakref.nitro.data.ValueDemand;

import java.util.Map;
import java.util.Optional;
import java.util.function.ToLongFunction;

import static java.util.Objects.requireNonNull;
import static org.weakref.nitro.operator.BatchAggregationSession.InputOwnership.CALLER;

/// Pull-operator view of an incremental aggregation session.
///
/// Input ownership follows [BatchAggregationSession#addInputWithOwnership(Batch, long)]. The
/// operator closes ordinary inputs immediately after their synchronous update, while a session
/// which returns `SESSION` remains responsible for that input. This permits adaptive partial
/// aggregation to transfer a pass-through batch to its corresponding output without copying it.
public final class BatchAggregationOperator
        implements Operator
{
    private final Operator source;
    private final BatchAggregationSession aggregation;
    private final ToLongFunction<Batch> inputSize;
    private final Map<Integer, ValueDemand> inputDemand;

    private Batch finalOutput;
    private Batch currentOutput;
    private boolean inputFinished;
    private boolean closed;

    public BatchAggregationOperator(
            Operator source,
            BatchAggregationSession aggregation,
            ToLongFunction<Batch> inputSize)
    {
        this(source, aggregation, inputSize, Operator.fullOutputDemand(source.outputCount()));
    }

    public BatchAggregationOperator(
            Operator source,
            BatchAggregationSession aggregation,
            ToLongFunction<Batch> inputSize,
            Map<Integer, ValueDemand> inputDemand)
    {
        this.source = requireNonNull(source, "source is null");
        this.aggregation = requireNonNull(aggregation, "aggregation is null");
        this.inputSize = requireNonNull(inputSize, "inputSize is null");
        this.inputDemand = Map.copyOf(requireNonNull(inputDemand, "inputDemand is null"));
        for (int output : this.inputDemand.keySet()) {
            if (output < 0 || output >= source.outputCount()) {
                throw new IllegalArgumentException("demanded input is outside source schema: " + output);
            }
        }
    }

    @Override
    public int outputCount()
    {
        return outputSchema().size();
    }

    @Override
    public Schema outputSchema()
    {
        return aggregation.outputSchema();
    }

    @Override
    public boolean hasNext()
    {
        checkOpen();
        if (finalOutput != null || aggregation.hasOutput()) {
            return true;
        }
        if (inputFinished) {
            return false;
        }

        while (source.hasNext()) {
            Batch input = source.next();
            boolean callerOwned = true;
            try {
                long bytes = inputSize.applyAsLong(input);
                if (bytes < 0) {
                    throw new IllegalArgumentException("input size is negative");
                }
                callerOwned = aggregation.addInputWithOwnership(input, bytes) == CALLER;
            }
            finally {
                if (callerOwned) {
                    input.close();
                }
            }
            if (aggregation.hasOutput()) {
                return true;
            }
        }

        inputFinished = true;
        finalOutput = requireNonNull(aggregation.finishOutput(), "aggregation returned null final output").orElse(null);
        return finalOutput != null;
    }

    @Override
    public Batch next()
    {
        if (!hasNext()) {
            throw new IllegalStateException("No more rows");
        }
        currentOutput = aggregation.hasOutput() ? aggregation.getOutput() : takeFinalOutput();
        if (currentOutput.outputCount() != outputCount()) {
            int actualOutputCount = currentOutput.outputCount();
            currentOutput.close();
            currentOutput = null;
            throw new IllegalStateException("Aggregation output batch does not match its declared schema: expected %s channels, got %s"
                    .formatted(outputCount(), actualOutputCount));
        }
        return currentOutput;
    }

    private Batch takeFinalOutput()
    {
        Batch output = requireNonNull(finalOutput, "finalOutput is null");
        finalOutput = null;
        return output;
    }

    @Override
    public void constrain(Mask mask)
    {
        requireNonNull(mask, "mask is null");
        checkOpen();
        if (currentOutput == null) {
            throw new IllegalStateException("No current batch");
        }
        currentOutput.constrain(mask);
    }

    @Override
    public Optional<Map<Integer, ValueDemand>> sourceOutputDemand(Map<Integer, ValueDemand> demandedOutputs)
    {
        requireNonNull(demandedOutputs, "demandedOutputs is null");
        for (int output : demandedOutputs.keySet()) {
            if (output < 0 || output >= outputCount()) {
                throw new IllegalArgumentException("demanded output is outside aggregation schema: " + output);
            }
        }
        return source.sourceOutputDemand(inputDemand);
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        if (finalOutput != null) {
            finalOutput.close();
            finalOutput = null;
        }
        try {
            aggregation.close();
        }
        finally {
            source.close();
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("operator is closed");
        }
    }
}
