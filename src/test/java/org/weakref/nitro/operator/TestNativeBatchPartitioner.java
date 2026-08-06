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
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.execution.EngineResources;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class TestNativeBatchPartitioner
{
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
