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
package org.weakref.nitro.data;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.weakref.nitro.execution.EngineResources;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class BenchmarkPrimitiveMaskedCopy
{
    @Param({"BOOLEAN", "I32", "I64", "F64", "ERROR"})
    public String representation;

    @Param({"512", "8192"})
    public int rows;

    @Param({"ALL", "PREFIX", "SPARSE"})
    public String selection;

    private EngineResources resources;
    private Allocator allocator;
    private Allocator.Context context;
    private Vector source;
    private Vector target;
    private Mask mask;

    @Setup
    public void setup()
    {
        resources = EngineResources.createDefault();
        allocator = new Allocator(resources);
        context = new Allocator.Context("masked-copy");
        source = switch (representation) {
            case "BOOLEAN" -> new BooleanVector(rows);
            case "I32" -> new I32Vector(IntStream.range(0, rows).toArray());
            case "I64" -> new I64Vector(IntStream.range(0, rows).asLongStream().toArray());
            case "F64" -> new F64Vector(IntStream.range(0, rows).asDoubleStream().toArray());
            case "ERROR" -> new ErrorVector(rows);
            default -> throw new IllegalArgumentException(representation);
        };
        if (source instanceof BooleanVector booleans) {
            for (int position = 0; position < rows; position++) {
                booleans.values()[position] = position % 8 == 0;
            }
        }
        mask = switch (selection) {
            case "ALL" -> Mask.all(rows);
            case "PREFIX" -> Mask.all(rows / 2);
            case "SPARSE" -> Mask.sparse(IntStream.range(0, rows).filter(position -> position % 2 == 0).toArray(), rows);
            default -> throw new IllegalArgumentException(selection);
        };
        target = source.copy(allocator, context);
    }

    @Benchmark
    public Vector copy()
    {
        target = source.copyMasked(allocator, context, target, mask);
        return target;
    }

    @TearDown
    public void tearDown()
    {
        try {
            allocator.close();
        }
        finally {
            resources.close();
        }
    }
}
