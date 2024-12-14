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
import org.weakref.nitro.data.LongVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.generator.Generator;

import java.util.List;

import static java.lang.Math.toIntExact;

public class GeneratorOperator
        implements Operator
{
    private static final int DEFAULT_BATCH_SIZE = 1024 * 10;
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("GeneratorOperator");

    private final int batchSize;
    private final List<Generator> generators;
    private final List<Vector> results;

    private final boolean[] filled;
    private final Allocator allocator;
    private long remaining;
    private int currentBatchSize;
    private Mask mask;

    public GeneratorOperator(Allocator allocator, long rowCount, List<Generator> generators)
    {
        this(allocator, rowCount, DEFAULT_BATCH_SIZE, generators);
    }

    public GeneratorOperator(Allocator allocator, long rowCount, int batchSize, List<Generator> generators)
    {
        this.allocator = allocator;
        this.remaining = rowCount;
        this.batchSize = batchSize;
        this.generators = generators;

        results = generators.stream()
                .map(_ -> allocator.allocate(ALLOCATION_CONTEXT, batchSize))
                .toList();

        filled = new boolean[generators.size()];
    }

    @Override
    public int columnCount()
    {
        return results.size();
    }

    @Override
    public Mask next()
    {
        // for any column not filled in the last run, advance the generators
        for (int i = 0; i < filled.length; i++) {
            if (!filled[i]) {
                generators.get(i).skip(currentBatchSize);
            }
            filled[i] = false;
        }

        currentBatchSize = toIntExact(Math.min(this.remaining, batchSize));
        if (mask == null || mask.count() != currentBatchSize) {
            mask = Mask.all(currentBatchSize);
        }

        this.remaining -= currentBatchSize;

        return mask;
    }

    @Override
    public boolean hasNext()
    {
        return remaining > 0;
    }

    @Override
    public void constrain(Mask mask)
    {
        this.mask = mask;
    }

    @Override
    public Vector column(int column)
    {
        if (filled[column] || mask.none()) {
            return results.get(column);
        }

        filled[column] = true;

        Generator generator = generators.get(column);
        LongVector result = (LongVector) results.get(column);

        for (int position = 0; position < currentBatchSize; position++) {
            generator.next();
            result.nulls()[position] = generator.isNull();
            result.values()[position] = generator.value();
        }

        return result;
    }

    @Override
    public void close()
    {
        allocator.release(ALLOCATION_CONTEXT);
    }
}
