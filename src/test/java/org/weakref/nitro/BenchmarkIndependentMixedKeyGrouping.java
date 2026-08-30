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
package org.weakref.nitro;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.LongFlatKeyStorage;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.core.type.TypeVectorFactory;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAllocator;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.KeyOnlyGroupingSession;
import org.weakref.nitro.operator.Output;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.lang.Math.toIntExact;

/**
 * Models source-side partial DISTINCT for a distributed scan: many independent, moderately sized grouping states
 * consume sparse rows whose keys are two flat integer carriers and one dictionary carrier. This is the physical
 * shape behind ClickBench q11;
 * the ordinary whole-query operator fixture instead builds one global MarkDistinct state and therefore overstates
 * cross-batch reuse relative to the SQL plan.
 */
@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 8, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkIndependentMixedKeyGrouping
{
    private static final TypeBinding INTEGER = longBinding("benchmark:integer", Optional.of(IntegerFlatKeyStorage.INSTANCE));
    private static final TypeBinding BIGINT = longBinding("benchmark:bigint", Optional.empty());
    private static final TypeBinding VARCHAR = new TypeBinding()
    {
        @Override
        public TypeIdentity identity()
        {
            return new TypeIdentity("benchmark:varchar");
        }

        @Override
        public Class<?> carrierType()
        {
            return byte[].class;
        }

        @Override
        public TypeOperators operators()
        {
            return TypeOperators.UNSPECIFIED;
        }

        @Override
        public Optional<TypeVectorFactory> vectorFactory()
        {
            return Optional.of(new TypeVectorFactory()
            {
                @Override
                public Vector constant(VectorAllocator allocator, Object value, int length)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Vector nullValues(VectorAllocator allocator, int length)
                {
                    return allocator.allocate(BinaryVector.class, length, size -> new BinaryVector(size, 0));
                }
            });
        }

        @Override
        public Set<Class<? extends Vector>> supportedVectorTypes()
        {
            return Set.of(BinaryVector.class, DictionaryVector.class);
        }
    };
    private static final Schema INPUT_SCHEMA = new Schema(List.of(
            new Field(INTEGER, false),
            new Field(BIGINT, false),
            new Field(VARCHAR, false)));
    private static final int BATCHES_PER_STATE = 80;
    private static final int ADDRESSABLE_POSITIONS = 10_000;
    private static final int SELECTED_POSITIONS = 556;

    @Param({"1", "16"})
    public int stateCount;

    private EngineResources resources;
    private Allocator allocator;
    private List<InputBatch> inputBatches;
    private long expectedRowsPerState;

    @Setup
    public void setup()
    {
        resources = EngineResources.createDefault();
        allocator = new Allocator(resources);
        inputBatches = new ArrayList<>(BATCHES_PER_STATE);
        for (int batch = 0; batch < BATCHES_PER_STATE; batch++) {
            inputBatches.add(inputBatch(batch));
        }
        expectedRowsPerState = runStates(1);
        if (expectedRowsPerState <= 0 || expectedRowsPerState >= (long) BATCHES_PER_STATE * SELECTED_POSITIONS) {
            throw new IllegalStateException("benchmark input does not contract: " + expectedRowsPerState);
        }
    }

    @TearDown
    public void tearDown()
    {
        allocator.close();
        resources.close();
    }

    @Benchmark
    public long groupIndependentStates()
    {
        long rows = runStates(stateCount);
        long expected = expectedRowsPerState * stateCount;
        if (rows != expected) {
            throw new IllegalStateException("grouped row mismatch: expected=" + expected + ", actual=" + rows);
        }
        return rows;
    }

    private long runStates(int count)
    {
        long outputRows = 0;
        for (int state = 0; state < count; state++) {
            try (KeyOnlyGroupingSession session = new KeyOnlyGroupingSession(
                    allocator,
                    INPUT_SCHEMA,
                    List.of(0, 1, 2),
                    List.of(0, 1, 2),
                    resources.operatorResources())) {
                for (InputBatch input : inputBatches) {
                    try (Batch batch = input.batch()) {
                        session.addInput(batch);
                    }
                    if (session.hasOutput()) {
                        try (Batch output = session.getOutput()) {
                            outputRows += output.borrowMask().selectedCount();
                        }
                    }
                }
                try (Batch ignored = session.finish()) {
                    // A key-only session emits new representatives eagerly.
                }
            }
        }
        return outputRows;
    }

    private static InputBatch inputBatch(int batch)
    {
        int[] selected = new int[SELECTED_POSITIONS];
        I32Vector first = new I32Vector(ADDRESSABLE_POSITIONS);
        I64Vector second = new I64Vector(ADDRESSABLE_POSITIONS);
        int[] thirdIds = new int[ADDRESSABLE_POSITIONS];
        for (int index = 0; index < SELECTED_POSITIONS; index++) {
            int position = 64 + (int) ((long) index * (ADDRESSABLE_POSITIONS - 64) / SELECTED_POSITIONS);
            selected[index] = position;
            long mixed = mix64((long) position * 0x9E37_79B9L + batch * 0xC2B2_AE35L);
            first.values()[position] = Math.floorMod((int) mixed, 16);
            second.values()[position] = 1_000_000L + Math.floorMod((int) (mixed >>> 21), 32);
            thirdIds[position] = Math.floorMod((int) (mixed >>> 42), 32);
        }

        BinaryVector thirdDomain = new BinaryVector(32, 256);
        for (int index = 0; index < 32; index++) {
            thirdDomain.setBytes(index, ("model-" + index).getBytes(StandardCharsets.UTF_8));
        }
        return new InputBatch(
                Mask.sparse(selected, ADDRESSABLE_POSITIONS),
                new Streams[] {
                        Streams.ofValues(first),
                        Streams.ofValues(second),
                        Streams.ofValues(DictionaryVector.wrap(thirdIds, thirdDomain))});
    }

    private static long mix64(long value)
    {
        value ^= value >>> 33;
        value *= 0xFF51_AFD7_ED55_8CCDL;
        value ^= value >>> 33;
        value *= 0xC4CE_B9FE_1A85_EC53L;
        return value ^ (value >>> 33);
    }

    private static TypeBinding longBinding(String identity, Optional<LongFlatKeyStorage> storage)
    {
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity(identity);
            }

            @Override
            public Class<?> carrierType()
            {
                return long.class;
            }

            @Override
            public TypeOperators operators()
            {
                return TypeOperators.UNSPECIFIED;
            }

            @Override
            public Optional<TypeVectorFactory> vectorFactory()
            {
                return Optional.of(new TypeVectorFactory()
                {
                    @Override
                    public Vector constant(VectorAllocator allocator, Object value, int length)
                    {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Vector nullValues(VectorAllocator allocator, int length)
                    {
                        return allocator.allocate(I64Vector.class, length, I64Vector::new);
                    }
                });
            }

            @Override
            public Optional<LongFlatKeyStorage> longFlatKeyStorage()
            {
                return storage;
            }

            @Override
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return Set.of(I32Vector.class, I64Vector.class, DictionaryVector.class);
            }
        };
    }

    private enum IntegerFlatKeyStorage
            implements LongFlatKeyStorage
    {
        INSTANCE;

        @Override
        public int fixedSize()
        {
            return Integer.BYTES;
        }

        @Override
        public void write(byte[] target, int offset, long value)
        {
            int integer = toIntExact(value);
            target[offset] = (byte) integer;
            target[offset + 1] = (byte) (integer >>> 8);
            target[offset + 2] = (byte) (integer >>> 16);
            target[offset + 3] = (byte) (integer >>> 24);
        }

        @Override
        public long read(byte[] source, int offset)
        {
            return (source[offset] & 0xFFL) |
                    (source[offset + 1] & 0xFFL) << 8 |
                    (source[offset + 2] & 0xFFL) << 16 |
                    (long) source[offset + 3] << 24;
        }
    }

    private record InputBatch(Mask mask, Streams[] streams)
    {
        private Batch batch()
        {
            Output[] outputs = new Output[streams.length];
            for (int channel = 0; channel < streams.length; channel++) {
                outputs[channel] = Output.of(streams[channel]);
            }
            return new Batch(mask, outputs);
        }
    }
}
