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
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

public class EnforceSingleRowOperator
        implements Operator
{
    private final Allocator.Context allocationContext = new Allocator.Context("EnforceSingleRowOperator");

    private final Allocator allocator;
    private final Operator source;
    private final Streams[] stagedColumns;

    private boolean loaded;
    private boolean done;
    private Batch stagedBatch;

    public EnforceSingleRowOperator(Allocator allocator, Operator source)
    {
        this.allocator = allocator;
        this.source = source;
        this.stagedColumns = new Streams[source.outputCount()];
    }

    @Override
    public int outputCount()
    {
        return source.outputCount();
    }

    @Override
    public boolean hasNext()
    {
        if (!loaded) {
            load();
        }
        return !done;
    }

    @Override
    public Batch next()
    {
        if (!loaded) {
            load();
        }
        done = true;
        return stagedBatch;
    }

    @Override
    public void constrain(Mask mask)
    {
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        return true;
    }

    @Override
    public boolean supportsConstrainedReborrow()
    {
        // constrain() is a no-op, so a downstream constrain + re-borrow would re-read the full,
        // differently-indexed output. Cannot satisfy a constrained re-borrow.
        return false;
    }

    @Override
    public void close()
    {
        source.close();
        allocator.release(allocationContext);
    }

    private void load()
    {
        loaded = true;

        int rowCount = 0;
        while (source.hasNext()) {
            try (Batch batch = source.next()) {
                captureSchema(batch);
                Mask mask = batch.borrowMask();
                for (int position : mask) {
                    rowCount++;
                    if (rowCount > 1) {
                        throw new IllegalStateException("Scalar subquery returned multiple rows");
                    }
                    copyRow(batch, position);
                }
            }
        }

        if (rowCount == 0) {
            materializeNullRow();
        }

        stagedBatch = new Batch(
                allocator.allocateRangeMask(allocationContext, 0, 1),
                outputs());
    }

    private void captureSchema(Batch batch)
    {
        for (int outputIndex = 0; outputIndex < outputCount(); outputIndex++) {
            if (stagedColumns[outputIndex] != null) {
                continue;
            }
            stagedColumns[outputIndex] = emptyStreamsLike(batch.output(outputIndex));
        }
    }

    private void copyRow(Batch batch, int position)
    {
        for (int outputIndex = 0; outputIndex < outputCount(); outputIndex++) {
            stagedColumns[outputIndex] = copySinglePosition(borrowedStreams(batch.output(outputIndex)), stagedColumns[outputIndex], position, 1);
        }
    }

    private void materializeNullRow()
    {
        for (int outputIndex = 0; outputIndex < outputCount(); outputIndex++) {
            Streams schema = stagedColumns[outputIndex];
            if (schema == null) {
                throw new IllegalStateException("Unable to determine output schema for EnforceSingleRowOperator");
            }
            stagedColumns[outputIndex] = nullStreamsLike(schema);
        }
    }

    private Output[] outputs()
    {
        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            int index = outputIndex;
            outputs[outputIndex] = new Output(
                    stagedColumns[index].streams(),
                    stream -> stagedColumns[index].get(stream),
                    (stream, vector) -> allocator.transfer(allocationContext, vector));
        }
        return outputs;
    }

    private Streams emptyStreamsLike(Output output)
    {
        Streams.Builder builder = Streams.builder();
        for (Stream stream : output.streams()) {
            Vector vector = output.borrow(stream);
            builder.put(stream, vector.emptyLike(allocator, allocationContext));
        }
        return builder.build();
    }

    private Streams nullStreamsLike(Streams schema)
    {
        Streams.Builder builder = Streams.builder();
        Vector values = valuesLike(schema.values());
        builder.put(Stream.VALUES, values);

        BooleanVector nulls = allocator.allocate(allocationContext, BooleanVector.class, 1, BooleanVector::new);
        nulls.values()[0] = true;
        builder.put(Stream.NULLS, nulls);

        if (schema.has(Stream.ERRORS)) {
            BooleanVector errors = allocator.allocate(allocationContext, BooleanVector.class, 1, BooleanVector::new);
            builder.put(Stream.ERRORS, errors);
        }
        return builder.build();
    }

    private Vector valuesLike(Vector schemaValues)
    {
        return switch (schemaValues) {
            case I64Vector _ -> allocator.allocate(allocationContext, I64Vector.class, 1, I64Vector::new);
            case I32Vector _ -> allocator.allocate(allocationContext, I32Vector.class, 1, I32Vector::new);
            case F64Vector _ -> allocator.allocate(allocationContext, F64Vector.class, 1, F64Vector::new);
            case BooleanVector _ -> allocator.allocate(allocationContext, BooleanVector.class, 1, BooleanVector::new);
            case BinaryVector _ -> allocator.allocate(allocationContext, BinaryVector.class, 1, size -> new BinaryVector(size, 0));
            default -> throw new IllegalArgumentException("Unsupported EnforceSingleRow output type: " + schemaValues.getClass().getSimpleName());
        };
    }

    private Streams copySinglePosition(Streams source, Streams existing, int sourcePosition, int outputSize)
    {
        Streams.Builder builder = Streams.builder();
        for (Stream stream : source.streams()) {
            Vector copied = source.get(stream).copySinglePositionInto(allocator, allocationContext, existing.getOrNull(stream), sourcePosition, 0, outputSize);
            builder.put(stream, copied);
        }
        return builder.build();
    }

    private static Streams borrowedStreams(Output output)
    {
        Streams.Builder builder = Streams.builder();
        for (Stream stream : output.streams()) {
            builder.put(stream, output.borrow(stream));
        }
        return builder.build();
    }
}
