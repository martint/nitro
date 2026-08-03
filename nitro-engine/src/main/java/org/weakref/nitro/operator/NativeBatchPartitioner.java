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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Partitions one native batch into independently owned compact batches.
 *
 * <p>Hash semantics come exclusively from the schema's registry-supplied type bindings. The
 * partitioner does not inspect logical type identities or concrete vector implementations.
 */
public final class NativeBatchPartitioner
{
    private static final long NULL_HASH = 0;

    private final Allocator allocator;
    private final StructuralKeyKernel[] keyKernels;
    private final int[] partitionChannels;
    private final int outputCount;
    private final int partitionCount;
    private final int partitionMask;

    public NativeBatchPartitioner(Allocator allocator, Schema schema, int[] partitionChannels, int partitionCount)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        requireNonNull(schema, "schema is null");
        outputCount = schema.size();
        this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null").clone();
        checkArgument(this.partitionChannels.length > 0, "partitionChannels is empty");
        checkArgument(Integer.bitCount(partitionCount) == 1, "partitionCount must be a power of two");
        this.partitionCount = partitionCount;
        partitionMask = partitionCount - 1;

        StructuralTypeKernelFactory kernels = new StructuralTypeKernelFactory();
        keyKernels = new StructuralKeyKernel[this.partitionChannels.length];
        for (int index = 0; index < this.partitionChannels.length; index++) {
            int channel = this.partitionChannels[index];
            checkArgument(channel >= 0 && channel < schema.size(), "partition channel is out of bounds: %s", channel);
            keyKernels[index] = kernels.key(schema.field(channel).type());
        }
    }

    public List<Partition> partition(Batch source)
    {
        requireNonNull(source, "source is null");
        Streams[] columns = new Streams[outputCount];
        for (int channel = 0; channel < outputCount; channel++) {
            Output output = source.output(channel);
            columns[channel] = Streams.of(
                    output.borrow(Stream.VALUES),
                    output.borrowOrNull(Stream.NULLS),
                    output.borrowOrNull(Stream.ERRORS));
        }

        IntArrayList[] assignments = new IntArrayList[partitionCount];
        for (int partition = 0; partition < partitionCount; partition++) {
            assignments[partition] = new IntArrayList();
        }
        for (int position : source.borrowMask()) {
            long hash = 0;
            for (int key = 0; key < keyKernels.length; key++) {
                Streams streams = columns[partitionChannels[key]];
                Vector nulls = streams.getOrNull(Stream.NULLS);
                long fieldHash = nulls != null && OperatorVectorSupport.isNull(nulls, position)
                        ? NULL_HASH
                        : keyKernels[key].hash(streams.get(Stream.VALUES), nulls, position);
                hash = 31 * hash + fieldHash;
            }
            assignments[mix(hash) & partitionMask].add(position);
        }

        List<Partition> result = new ArrayList<>(partitionCount);
        try {
            for (int partition = 0; partition < partitionCount; partition++) {
                if (!assignments[partition].isEmpty()) {
                    result.add(new Partition(partition, copy(columns, assignments[partition])));
                }
            }
            return List.copyOf(result);
        }
        catch (RuntimeException | Error failure) {
            result.forEach(partition -> partition.batch().close());
            throw failure;
        }
    }

    private Batch copy(Streams[] columns, IntArrayList assignment)
    {
        int count = assignment.size();
        int[] positions = assignment.elements();
        Allocator.Context context = new Allocator.Context("NativeBatchPartitioner.partition");
        try {
            Output[] outputs = new Output[columns.length];
            for (int channel = 0; channel < columns.length; channel++) {
                Streams source = columns[channel];
                Streams.Builder copied = Streams.builder();
                for (Stream stream : source.streams()) {
                    copied.put(stream, source.get(stream).copyPositionsInto(
                            allocator,
                            context,
                            null,
                            positions,
                            count,
                            0,
                            count));
                }
                Streams streams = copied.build();
                outputs[channel] = new Output(
                        streams.streams(),
                        streams::get,
                        (stream, vector) -> allocator.transfer(context, vector),
                        (stream, vector) -> allocator.release(context, vector));
            }
            return new Batch(Mask.all(count), outputs);
        }
        catch (RuntimeException | Error failure) {
            allocator.release(context);
            throw failure;
        }
    }

    private static int mix(long hash)
    {
        long value = hash ^ (hash >>> 33);
        value *= 0xff51afd7ed558ccdL;
        value ^= value >>> 33;
        value *= 0xc4ceb9fe1a85ec53L;
        return (int) (value ^ (value >>> 33));
    }

    public record Partition(int index, Batch batch)
    {
        public Partition
        {
            checkArgument(index >= 0, "index is negative");
            requireNonNull(batch, "batch is null");
        }
    }
}
