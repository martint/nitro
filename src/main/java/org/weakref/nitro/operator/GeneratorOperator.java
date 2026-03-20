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
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.operator.evaluator.ir.Stream;
import org.weakref.nitro.operator.generator.I64Generator;

import java.util.List;
import java.util.Set;

import static java.lang.Math.toIntExact;

public class GeneratorOperator
        implements Operator
{
    private static final int DEFAULT_BATCH_SIZE = 1024 * 10;
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("GeneratorOperator");

    private final int batchSize;
    private final List<I64Generator> generators;
    private final I64Vector[] results;

    private final boolean[] filled;
    private final Allocator allocator;
    private long remaining;
    private int currentBatchSize;
    private Mask mask;
    private Batch currentBatch;

    public GeneratorOperator(Allocator allocator, long rowCount, List<I64Generator> generators)
    {
        this(allocator, rowCount, DEFAULT_BATCH_SIZE, generators);
    }

    public GeneratorOperator(Allocator allocator, long rowCount, int batchSize, List<I64Generator> generators)
    {
        this.allocator = allocator;
        this.remaining = rowCount;
        this.batchSize = batchSize;
        this.generators = generators;

        results = generators.stream()
                .map(_ -> allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, batchSize, I64Vector::new))
                .toArray(I64Vector[]::new);

        filled = new boolean[generators.size()];
    }

    @Override
    public int outputCount()
    {
        return results.length;
    }

    @Override
    public boolean hasNext()
    {
        return remaining > 0;
    }

    @Override
    public Batch next()
    {
        closeCurrentBatch();

        for (int i = 0; i < filled.length; i++) {
            if (!filled[i]) {
                generators.get(i).skip(currentBatchSize);
            }
            filled[i] = false;
        }

        currentBatchSize = toIntExact(Math.min(remaining, batchSize));
        if (mask == null) {
            mask = allocator.allocateAllMask(ALLOCATION_CONTEXT, currentBatchSize);
        }
        else {
            mask.selectAll(currentBatchSize);
        }
        remaining -= currentBatchSize;

        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            int output = outputIndex;
            outputs[outputIndex] = new Output(
                    Set.of(Stream.VALUES),
                    stream -> outputVector(output),
                    (stream, vector) -> {
                        results[output] = null;
                        return allocator.transfer(ALLOCATION_CONTEXT, vector);
                    },
                    (stream, vector) -> allocator.release(ALLOCATION_CONTEXT, vector));
        }
        Batch batch = new Batch(
                mask,
                _ -> {},
                takenMask -> allocator.transfer(ALLOCATION_CONTEXT, takenMask),
                releasedMask -> allocator.release(ALLOCATION_CONTEXT, releasedMask),
                () -> {},
                outputs);
        currentBatch = batch;
        return batch;
    }

    @Override
    public void constrain(Mask mask)
    {
        this.mask = mask;
    }

    private I64Vector outputVector(int output)
    {
        if (filled[output] || mask.none()) {
            return results[output];
        }

        filled[output] = true;

        I64Generator generator = generators.get(output);
        I64Vector result = results[output];
        if (result == null) {
            result = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, batchSize, I64Vector::new);
            results[output] = result;
        }

        for (int position = 0; position < currentBatchSize; position++) {
            generator.next();
            result.values()[position] = generator.value();
        }

        return result;
    }

    @Override
    public void close()
    {
        closeCurrentBatch();
        allocator.release(ALLOCATION_CONTEXT);
    }

    private void closeCurrentBatch()
    {
        if (currentBatch != null) {
            currentBatch.close();
            currentBatch = null;
        }
    }
}
