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
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/** Materializes a lazy batch generation and pins its vector trees for independent downstream ownership. */
final class RetainedBatch
{
    private RetainedBatch() {}

    public static Batch retain(Allocator allocator, Batch source, int outputCount)
    {
        requireNonNull(allocator, "allocator is null");
        requireNonNull(source, "source is null");
        if (outputCount < 0) {
            throw new IllegalArgumentException("outputCount is negative");
        }

        Allocator.VectorTreeLease lease = null;
        try {
            Mask mask = copyMask(source.borrowMask());
            Output[] outputs = new Output[outputCount];
            List<Vector> roots = new ArrayList<>(outputCount * 3);
            for (int outputIndex = 0; outputIndex < outputCount; outputIndex++) {
                Output sourceOutput = source.output(outputIndex);
                Streams.Builder streams = Streams.builder();
                for (Stream stream : sourceOutput.streams()) {
                    Vector vector = sourceOutput.borrow(stream);
                    streams.put(stream, vector);
                    roots.add(vector);
                }
                outputs[outputIndex] = Output.of(streams.build());
            }
            lease = allocator.leaseVectorTree(roots);
            source.close();
            Allocator.VectorTreeLease retained = lease;
            if (!roots.stream().allMatch(allocator::ownsVectorTree)) {
                return new Batch(mask, _ -> {}, Function.identity(), _ -> {}, retained::close, outputs);
            }
            return Batch.retained(
                    mask,
                    retained::close,
                    owner -> owner == allocator ? retained.tryDetachForAsyncRelease() : java.util.Optional.empty(),
                    outputs);
        }
        catch (RuntimeException | Error failure) {
            if (lease != null) {
                lease.close();
            }
            try {
                source.close();
            }
            catch (RuntimeException | Error closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private static Mask copyMask(Mask mask)
    {
        if (mask.all()) {
            return Mask.all(mask.size());
        }
        int[] positions = new int[mask.selectedCount()];
        for (int index = 0; index < positions.length; index++) {
            positions[index] = mask.position(index);
        }
        return Mask.sparse(positions, mask.size());
    }
}
