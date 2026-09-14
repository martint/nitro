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

import org.junit.jupiter.api.Test;
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.data.AllocationResources;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.execution.EngineResources;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestNativeBatchPartitioner
{
    @Test
    void transfersExclusivePartitionTreesWithoutCopying()
            throws Exception
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                Allocator foreignAllocator = new Allocator(resources)) {
            int[] ids = {0, 1, 0, 1, 0, 1};
            I64Vector values = new I64Vector(new long[] {10, 20});
            try (Batch source = new Batch(
                    Mask.all(ids.length),
                    Output.of(Streams.ofValues(DictionaryVector.wrap(ids, values))),
                    Output.of(Streams.ofValues(DictionaryVector.wrap(ids, new I64Vector(new long[] {30, 40})))))) {
                NativeBatchPartitioner partitioner = new NativeBatchPartitioner(allocator, 2, 1, NativeBatchPartitionPolicy.defaults());
                Batch partition = partitioner.partition(source, new int[ids.length]).getFirst().batch();
                try (partition) {
                    DictionaryVector first = (DictionaryVector) partition.output(0).borrow(Stream.VALUES);
                    DictionaryVector second = (DictionaryVector) partition.output(1).borrow(Stream.VALUES);
                    assertThat(first.ids()).isSameAs(second.ids());
                    assertThat(partition.tryDetachRetainedVectorsForAsyncRelease(foreignAllocator)).isEmpty();
                    try (Allocator.AsyncVectorTreeLease lease = partition.tryDetachRetainedVectorsForAsyncRelease(allocator).orElseThrow()) {
                        assertThat(partition.output(0).borrow(Stream.VALUES)).isSameAs(first);
                        partition.close();
                        Arrays.fill(ids, 1);
                        Arrays.fill(values.values(), -1);
                        assertThat(first.ids()).containsExactly(0, 1, 0, 1, 0, 1);
                        assertThat(((I64Vector) first.values()).values()).startsWith(10, 20);
                        assertThat(((I64Vector) second.values()).values()).startsWith(30, 40);
                        FutureTask<Void> release = new FutureTask<>(lease::close, null);
                        Thread releaser = new Thread(release);
                        releaser.start();
                        release.get(10, TimeUnit.SECONDS);
                    }
                }
            }
        }
    }

    @Test
    void reusesTemporaryRemappingWithoutAliasingPublishedIds()
    {
        int[] sourceIds = {7, 7, 9, 7, 9, 9, 7, 9};
        try (AllocationResources resources = new AllocationResources(new PrimitiveArrayPool(1 << 20, 0), new PrimitiveArrayPool(0, 0));
                Allocator allocator = new Allocator(resources);
                Batch source = new Batch(
                        Mask.all(8),
                        Output.of(Streams.ofValues(DictionaryVector.wrap(
                                sourceIds, new I64Vector(new long[16])))))) {
            int[] scratch = allocator.primitiveArrays().borrowInts(16);
            Arrays.fill(scratch, 777);
            allocator.primitiveArrays().release(scratch);
            NativeBatchPartitioner partitioner = new NativeBatchPartitioner(allocator, 1, 1, new NativeBatchPartitionPolicy(true, 2, 16));
            List<NativeBatchPartitioner.Partition> first = partitioner.partition(source, new int[8]);
            try {
                int[] reused = allocator.primitiveArrays().borrowInts(16);
                try {
                    assertThat(reused).isSameAs(scratch);
                    assertThat(reused[7]).isZero();
                    assertThat(reused[9]).isOne();
                    assertThat(reused[0]).isEqualTo(-1);
                    Arrays.fill(reused, 888);
                }
                finally {
                    allocator.primitiveArrays().release(reused);
                }
                List<NativeBatchPartitioner.Partition> second = partitioner.partition(source, new int[8]);
                try {
                    for (List<NativeBatchPartitioner.Partition> partitions : List.of(first, second)) {
                        DictionaryVector output = (DictionaryVector) partitions.getFirst().batch().output(0).borrow(Stream.VALUES);
                        assertThat(output.ids()).containsExactly(0, 0, 1, 0, 1, 1, 0, 1);
                        assertThat(output.ids()).isNotSameAs(scratch);
                    }
                }
                finally {
                    second.forEach(partition -> partition.batch().close());
                }
                sourceIds[2] = 16;
                assertThatThrownBy(() -> partitioner.partition(source, new int[8])).isInstanceOf(IndexOutOfBoundsException.class);
                int[] afterFailure = allocator.primitiveArrays().borrowInts(16);
                try {
                    assertThat(afterFailure).isSameAs(scratch);
                }
                finally {
                    allocator.primitiveArrays().release(afterFailure);
                }
            }
            finally {
                first.forEach(partition -> partition.batch().close());
            }
        }
    }

    @Test
    void preservesReusedDictionaryEncodingInPartitionCopies()
            throws ReflectiveOperationException
    {
        int size = 64;
        long[] keys = new long[size];
        int[] ids = new int[size];
        for (int position = 0; position < size; position++) {
            keys[position] = position;
            ids[position] = position & 1;
        }
        TypeBinding type = longType();
        Schema schema = new Schema(List.of(new Field("key", type, false), new Field("value", type, false)));
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                Batch source = new Batch(
                        Mask.all(size),
                        Output.of(Streams.ofValues(new I64Vector(keys))),
                        Output.of(Streams.ofValues(DictionaryVector.wrap(ids, new I64Vector(new long[] {11, 22})))))) {
            NativeBatchPartitioner partitioner = new NativeBatchPartitioner(
                    allocator,
                    schema,
                    new int[] {0},
                    4,
                    NativeBatchPartitionPolicy.defaults());
            List<NativeBatchPartitioner.Partition> partitions = partitioner.partition(source);
            try {
                assertThat(partitions).hasSize(4);
                for (NativeBatchPartitioner.Partition partition : partitions) {
                    DictionaryVector values = (DictionaryVector) partition.batch().output(1).borrow(Stream.VALUES);
                    assertThat(values.values()).isInstanceOf(I64Vector.class);
                    assertThat(values.values().length()).isEqualTo(2);
                }
            }
            finally {
                partitions.forEach(partition -> partition.batch().close());
            }
        }
    }

    @Test
    void compactsDestinationDictionariesAndReusesTheirIdMapping()
            throws ReflectiveOperationException
    {
        int size = 8;
        int[] ids = {7, 7, 9, 7, 9, 9, 7, 9};
        long[] dictionaryValues = new long[16];
        dictionaryValues[7] = 70;
        dictionaryValues[9] = 90;
        TypeBinding type = longType();
        Schema schema = new Schema(List.of(
                new Field("key", type, false),
                new Field("first", type, false),
                new Field("second", type, false)));
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                Batch source = new Batch(
                        Mask.all(size),
                        Output.of(Streams.ofValues(new I64Vector(new long[size]))),
                        Output.of(Streams.ofValues(DictionaryVector.wrap(ids, new I64Vector(dictionaryValues)))),
                        Output.of(Streams.ofValues(DictionaryVector.wrap(ids, new I64Vector(dictionaryValues)))))) {
            NativeBatchPartitioner partitioner = new NativeBatchPartitioner(
                    allocator,
                    schema,
                    new int[] {0},
                    1,
                    new NativeBatchPartitionPolicy(true, 2, 4));

            List<NativeBatchPartitioner.Partition> partitions = partitioner.partition(source);
            try {
                assertThat(partitions).hasSize(1);
                DictionaryVector first = (DictionaryVector) partitions.getFirst().batch().output(1).borrow(Stream.VALUES);
                DictionaryVector second = (DictionaryVector) partitions.getFirst().batch().output(2).borrow(Stream.VALUES);
                assertThat(first.values()).isInstanceOfSatisfying(I64Vector.class, values ->
                        assertThat(values.values()).containsExactly(70, 90));
                assertThat(first.ids()).containsExactly(0, 0, 1, 0, 1, 1, 0, 1);
                assertThat(second.ids()).isSameAs(first.ids());
            }
            finally {
                partitions.forEach(partition -> partition.batch().close());
            }
        }
    }

    @Test
    void preservesReusedBaseDomainThroughOneToOneOuterDictionary()
    {
        int size = 8;
        int[] outerIds = {0, 1, 2, 3, 4, 5, 6, 7};
        int[] innerIds = {0, 1, 0, 1, 0, 1, 0, 1};
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                Batch source = new Batch(
                        Mask.all(size),
                        Output.of(Streams.ofValues(DictionaryVector.wrapNested(
                                outerIds,
                                size,
                                DictionaryVector.wrapNested(innerIds, size, new I64Vector(new long[] {11, 22}))))))) {
            NativeBatchPartitioner partitioner = new NativeBatchPartitioner(
                    allocator,
                    1,
                    1,
                    new NativeBatchPartitionPolicy(true, 2, 4));

            List<NativeBatchPartitioner.Partition> partitions = partitioner.partition(source, new int[size]);
            try {
                assertThat(partitions).hasSize(1);
                assertThat(partitions.getFirst().batch().output(0).borrow(Stream.VALUES))
                        .isInstanceOfSatisfying(DictionaryVector.class, values -> {
                            assertThat(values.dictionaryDepth()).isEqualTo(1);
                            assertThat(values.ids()).containsExactly(0, 1, 0, 1, 0, 1, 0, 1);
                            assertThat(values.values()).isInstanceOfSatisfying(I64Vector.class, base ->
                                    assertThat(base.values()).containsExactly(11, 22));
                        });
            }
            finally {
                partitions.forEach(partition -> partition.batch().close());
            }
        }
    }

    @Test
    void materializesDictionaryInputWhenFlatPartitionsAreRequired()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                Batch source = new Batch(
                        Mask.all(4),
                        Output.of(Streams.ofValues(DictionaryVector.wrap(
                                new int[] {0, 1, 0, 1},
                                new I64Vector(new long[] {11, 22})))))) {
            NativeBatchPartitioner partitioner = new NativeBatchPartitioner(
                    allocator,
                    1,
                    2,
                    NativeBatchPartitionPolicy.flat());

            List<NativeBatchPartitioner.Partition> partitions = partitioner.partition(source, new int[] {0, 1, 0, 1});
            try {
                assertThat(partitions).hasSize(2);
                assertThat(partitions.get(0).batch().output(0).borrow(Stream.VALUES))
                        .isInstanceOfSatisfying(I64Vector.class, values -> assertThat(values.values()).containsExactly(11, 11));
                assertThat(partitions.get(1).batch().output(0).borrow(Stream.VALUES))
                        .isInstanceOfSatisfying(I64Vector.class, values -> assertThat(values.values()).containsExactly(22, 22));
            }
            finally {
                partitions.forEach(partition -> partition.batch().close());
            }
        }
    }

    @Test
    void copiesExternallyAssignedSparsePositionsToArbitraryPartitionCounts()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                Batch source = new Batch(
                        Mask.sparse(new int[] {0, 2, 5, 7}, 8),
                        Output.of(Streams.ofValues(new I64Vector(new long[] {10, 11, 12, 13, 14, 15, 16, 17}))))) {
            NativeBatchPartitioner partitioner = new NativeBatchPartitioner(
                    allocator,
                    1,
                    3,
                    NativeBatchPartitionPolicy.defaults());

            List<NativeBatchPartitioner.Partition> partitions = partitioner.partition(source, new int[] {2, 0, 2, 1});
            try {
                assertThat(partitions).extracting(NativeBatchPartitioner.Partition::index).containsExactly(0, 1, 2);
                assertThat(values(partitions.get(0).batch())).containsExactly(12);
                assertThat(values(partitions.get(1).batch())).containsExactly(17);
                assertThat(values(partitions.get(2).batch())).containsExactly(10, 15);
            }
            finally {
                partitions.forEach(partition -> partition.batch().close());
            }
        }
    }

    @Test
    void partitionsSelectedRowsIntoCompactIndependentBatches()
            throws ReflectiveOperationException
    {
        TypeBinding type = longType();
        Schema schema = new Schema(List.of(new Field("key", type, true), new Field("value", type, false)));
        EngineResources resources = EngineResources.createDefault();
        try (Allocator allocator = new Allocator(resources);
                Batch source = new Batch(
                        Mask.sparse(new int[] {0, 2, 3, 5}, 6),
                        Output.of(Streams.of(
                                new I64Vector(new long[] {11, 99, 11, 22, 99, 33}),
                                new BooleanVector(new boolean[] {false, false, false, true, false, false}),
                                null)),
                        Output.of(Streams.ofValues(new I64Vector(new long[] {10, 20, 30, 40, 50, 60}))))) {
            NativeBatchPartitioner partitioner = new NativeBatchPartitioner(
                    allocator,
                    schema,
                    new int[] {0},
                    4,
                    NativeBatchPartitionPolicy.defaults());
            List<NativeBatchPartitioner.Partition> partitions = partitioner.partition(source);
            try {
                List<Long> values = new ArrayList<>();
                Integer partitionForEleven = null;
                for (NativeBatchPartitioner.Partition partition : partitions) {
                    Batch batch = partition.batch();
                    assertThat(batch.borrowMask().all()).isTrue();
                    I64Vector keys = (I64Vector) batch.output(0).borrow(Stream.VALUES);
                    BooleanVector nulls = (BooleanVector) batch.output(0).borrow(Stream.NULLS);
                    I64Vector payload = (I64Vector) batch.output(1).borrow(Stream.VALUES);
                    for (int position : batch.borrowMask()) {
                        values.add(payload.values()[position]);
                        if (!nulls.values()[position] && keys.values()[position] == 11) {
                            if (partitionForEleven == null) {
                                partitionForEleven = partition.index();
                            }
                            assertThat(partition.index()).isEqualTo(partitionForEleven);
                        }
                    }
                }
                assertThat(values).containsExactlyInAnyOrder(10L, 30L, 40L, 60L);
            }
            finally {
                partitions.forEach(partition -> partition.batch().close());
            }
        }
        finally {
            resources.close();
        }
    }

    private static TypeBinding longType()
            throws ReflectiveOperationException
    {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        TypeOperators operators = new TypeOperators(
                Optional.of(lookup.findStatic(TestNativeBatchPartitioner.class, "identical", MethodType.methodType(boolean.class, long.class, long.class))),
                Optional.of(lookup.findStatic(TestNativeBatchPartitioner.class, "hash", MethodType.methodType(long.class, long.class))),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(lookup.findStatic(TestNativeBatchPartitioner.class, "read", MethodType.methodType(long.class, Vector.class, int.class))));
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:long");
            }

            @Override
            public Class<?> carrierType()
            {
                return long.class;
            }

            @Override
            public TypeOperators operators()
            {
                return operators;
            }

            @Override
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return Set.of(I64Vector.class);
            }
        };
    }

    private static long[] values(Batch batch)
    {
        return ((I64Vector) batch.output(0).borrow(Stream.VALUES)).values();
    }

    private static long read(Vector vector, int position)
    {
        return ((I64Vector) vector).values()[position];
    }

    private static long hash(long value)
    {
        return Long.hashCode(value);
    }

    private static boolean identical(long left, long right)
    {
        return left == right;
    }
}
