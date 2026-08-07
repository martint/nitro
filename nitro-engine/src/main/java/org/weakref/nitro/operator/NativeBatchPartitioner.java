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

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

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
    private final NativeBatchPartitionPolicy policy;

    /**
     * Creates a representation-preserving partition copier whose destination assignments are supplied by the caller.
     */
    public NativeBatchPartitioner(
            Allocator allocator,
            int outputCount,
            int partitionCount,
            NativeBatchPartitionPolicy policy)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.policy = requireNonNull(policy, "policy is null");
        checkArgument(outputCount >= 0, "outputCount is negative");
        checkArgument(partitionCount > 0, "partitionCount must be positive");
        this.outputCount = outputCount;
        this.partitionCount = partitionCount;
        partitionChannels = null;
        keyKernels = null;
        partitionMask = 0;
    }

    public NativeBatchPartitioner(
            Allocator allocator,
            Schema schema,
            int[] partitionChannels,
            int partitionCount,
            NativeBatchPartitionPolicy policy)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.policy = requireNonNull(policy, "policy is null");
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
        checkArgument(keyKernels != null, "partition keys were not configured");
        Streams[] columns = columns(source);

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

        return copyPartitions(columns, assignments);
    }

    /**
     * Copies a batch according to one destination per selected source position. Assignments are ordered like the
     * batch selection rather than indexed by physical position, so sparse masks do not require a dense side array.
     */
    public List<Partition> partition(Batch source, int[] partitionBySelectedPosition)
    {
        requireNonNull(source, "source is null");
        requireNonNull(partitionBySelectedPosition, "partitionBySelectedPosition is null");
        Mask mask = source.borrowMask();
        checkArgument(
                partitionBySelectedPosition.length == mask.selectedCount(),
                "assignment count does not match selected position count");

        IntArrayList[] assignments = new IntArrayList[partitionCount];
        for (int partition = 0; partition < partitionCount; partition++) {
            assignments[partition] = new IntArrayList();
        }
        for (int selectedPosition = 0; selectedPosition < partitionBySelectedPosition.length; selectedPosition++) {
            int partition = partitionBySelectedPosition[selectedPosition];
            checkArgument(partition >= 0 && partition < partitionCount, "partition is out of bounds: %s", partition);
            assignments[partition].add(mask.position(selectedPosition));
        }
        return copyPartitions(columns(source), assignments);
    }

    private Streams[] columns(Batch source)
    {
        Streams[] columns = new Streams[outputCount];
        for (int channel = 0; channel < outputCount; channel++) {
            Output output = source.output(channel);
            columns[channel] = Streams.of(
                    output.borrow(Stream.VALUES),
                    output.borrowOrNull(Stream.NULLS),
                    output.borrowOrNull(Stream.ERRORS));
        }
        return columns;
    }

    private List<Partition> copyPartitions(Streams[] columns, IntArrayList[] assignments)
    {
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
        Map<int[], DictionaryRemapping> dictionaryRemappings = new IdentityHashMap<>();
        try {
            Output[] outputs = new Output[columns.length];
            for (int channel = 0; channel < columns.length; channel++) {
                Streams source = columns[channel];
                Streams.Builder copied = Streams.builder();
                for (Stream stream : source.streams()) {
                    copied.put(stream, copyVector(
                            source.get(stream),
                            context,
                            positions,
                            count,
                            dictionaryRemappings));
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

    private Vector copyVector(
            Vector source,
            Allocator.Context context,
            int[] positions,
            int count,
            Map<int[], DictionaryRemapping> dictionaryRemappings)
    {
        if (source instanceof DictionaryVector dictionary) {
            DictionaryRemapping remapping = dictionaryRemappings.computeIfAbsent(
                    dictionary.ids(),
                    _ -> dictionaryRemapping(dictionary, positions, count));
            if (!remapping.preserved()) {
                return source.copyPositionsInto(allocator, context, null, positions, count, 0, count);
            }
            Vector dictionaryValues = dictionary.values().copyPositionsInto(
                    allocator,
                    context,
                    null,
                    remapping.dictionaryPositions(),
                    remapping.dictionarySize(),
                    0,
                    remapping.dictionarySize());
            return allocator.allocateDictionarySharedIds(context, remapping.ids(), count, dictionaryValues);
        }
        return source.copyPositionsInto(allocator, context, null, positions, count, 0, count);
    }

    private DictionaryRemapping dictionaryRemapping(DictionaryVector dictionary, int[] positions, int count)
    {
        if (!policy.preserveDictionaryEncoding() || policy.maximumCopiedDictionaryEntries() == 0) {
            return DictionaryRemapping.NOT_PRESERVED;
        }
        int[] sourceIds = dictionary.ids();
        int maximumDictionarySize = Math.min(count, policy.maximumCopiedDictionaryEntries());
        int[] dictionaryPositions = new int[maximumDictionarySize];
        int[] ids = new int[count];
        int dictionarySize = dictionary.values().length() <= policy.maximumCopiedDictionaryEntries()
                ? remapWithArray(sourceIds, positions, count, dictionaryPositions, ids, dictionary.values().length())
                : remapWithHashTable(sourceIds, positions, count, dictionaryPositions, ids);
        if (dictionarySize < 0 || !policy.preserveDictionary(dictionarySize, count)) {
            return DictionaryRemapping.NOT_PRESERVED;
        }
        return new DictionaryRemapping(ids, dictionaryPositions, dictionarySize);
    }

    private static int remapWithArray(
            int[] sourceIds,
            int[] positions,
            int count,
            int[] dictionaryPositions,
            int[] ids,
            int sourceDictionarySize)
    {
        int[] oldToNew = new int[sourceDictionarySize];
        Arrays.fill(oldToNew, -1);
        int dictionarySize = 0;
        for (int outputPosition = 0; outputPosition < count; outputPosition++) {
            int sourceId = sourceIds[positions[outputPosition]];
            int compactId = oldToNew[sourceId];
            if (compactId < 0) {
                if (dictionarySize == dictionaryPositions.length) {
                    return -1;
                }
                compactId = dictionarySize++;
                oldToNew[sourceId] = compactId;
                dictionaryPositions[compactId] = sourceId;
            }
            ids[outputPosition] = compactId;
        }
        return dictionarySize;
    }

    private static int remapWithHashTable(
            int[] sourceIds,
            int[] positions,
            int count,
            int[] dictionaryPositions,
            int[] ids)
    {
        Int2IntOpenHashMap oldToNew = new Int2IntOpenHashMap(dictionaryPositions.length);
        oldToNew.defaultReturnValue(-1);
        int dictionarySize = 0;
        for (int outputPosition = 0; outputPosition < count; outputPosition++) {
            int sourceId = sourceIds[positions[outputPosition]];
            int compactId = oldToNew.get(sourceId);
            if (compactId < 0) {
                if (dictionarySize == dictionaryPositions.length) {
                    return -1;
                }
                compactId = dictionarySize++;
                oldToNew.put(sourceId, compactId);
                dictionaryPositions[compactId] = sourceId;
            }
            ids[outputPosition] = compactId;
        }
        return dictionarySize;
    }

    private record DictionaryRemapping(int[] ids, int[] dictionaryPositions, int dictionarySize)
    {
        private static final DictionaryRemapping NOT_PRESERVED = new DictionaryRemapping(null, null, 0);

        private boolean preserved()
        {
            return ids != null;
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
