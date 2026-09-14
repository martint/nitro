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
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.NativeBatchPartitionPolicy;
import org.weakref.nitro.operator.NativeBatchPartitioner;
import org.weakref.nitro.operator.Output;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(2)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkNativeBatchPartitioner
{
    @Param({"2048", "131072"})
    public int baseSize;

    @Param({"16", "512", "513", "1024"})
    public int selectedDistinctCount;

    private EngineResources resources;
    private Batch source;
    private int[] assignments;

    @Setup
    public void setup()
    {
        resources = EngineResources.createDefault();
        int[] ids = new int[1024];
        long[] values = new long[baseSize];
        for (int position = 0; position < values.length; position++) {
            values[position] = position;
        }
        for (int position = 0; position < ids.length; position++) {
            ids[position] = position % selectedDistinctCount;
        }
        assignments = new int[ids.length];
        source = new Batch(Mask.all(ids.length), Output.of(Streams.ofValues(DictionaryVector.wrap(ids, new I64Vector(values)))));
    }

    @TearDown
    public void tearDown()
    {
        source.close();
        resources.close();
    }

    @Benchmark
    @OperationsPerInvocation(128)
    public void copyPartition(Blackhole blackhole)
    {
        copyPartitions(blackhole, false);
    }

    @Benchmark
    @OperationsPerInvocation(128)
    public void copyAsyncPartition(Blackhole blackhole)
    {
        copyPartitions(blackhole, true);
    }

    private void copyPartitions(Blackhole blackhole, boolean asynchronous)
    {
        try (Allocator allocator = new Allocator(resources)) {
            NativeBatchPartitioner partitioner = new NativeBatchPartitioner(allocator, 1, 1, NativeBatchPartitionPolicy.defaults());
            for (int partitionIndex = 0; partitionIndex < 128; partitionIndex++) {
                try (Batch partition = partitioner.partition(source, assignments).getFirst().batch()) {
                    if (asynchronous) {
                        try (Allocator.AsyncVectorTreeLease lease = partition.tryDetachRetainedVectorsForAsyncRelease(allocator).orElseThrow()) {
                            blackhole.consume(partition.output(0).borrow(Stream.VALUES));
                            partition.close();
                        }
                    }
                    else {
                        blackhole.consume(partition.output(0).borrow(Stream.VALUES));
                    }
                }
            }
        }
    }
}
