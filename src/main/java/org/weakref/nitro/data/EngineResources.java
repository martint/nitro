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

import org.weakref.nitro.operator.AggregationOperatorResources;
import org.weakref.nitro.operator.HashJoinOperatorResources;
import org.weakref.nitro.operator.OperatorCodeGenerationResources;
import org.weakref.nitro.operator.ProjectOperatorResources;

import static java.util.Objects.requireNonNull;

/**
 * Stateful resources owned by a Nitro engine integration.
 *
 * <p>The embedding engine chooses this object's lifetime and passes it to every allocator that should share retained
 * storage. Nothing in Nitro discovers an ambient process-wide pool. Separate resource owners are therefore isolated,
 * and an integration can bind retention to a worker, task, query, or test fixture as appropriate.
 */
public final class EngineResources
        implements AutoCloseable
{
    private static final long MIN_DEFAULT_MAX_RETAINED_BYTES = 512L << 20;
    private static final long MAX_DEFAULT_MAX_RETAINED_BYTES = 1L << 30;
    private static final long DEFAULT_MIN_RETAINED_BYTES = 256L << 10;
    private static final long DEFAULT_MAX_RETAINED_NATIVE_BYTES = 256L << 20;

    private final PrimitiveArrayPool primitiveArrays;
    private final PrimitiveArrayPool nativeBuffers;
    private final OperatorCodeGenerationResources operatorCodeGeneration;
    private final ProjectOperatorResources projectOperator;
    private final AggregationOperatorResources aggregationOperator;
    private final HashJoinOperatorResources hashJoinOperator;
    private boolean closed;

    public EngineResources(
            PrimitiveArrayPool primitiveArrays,
            PrimitiveArrayPool nativeBuffers,
            OperatorCodeGenerationResources operatorCodeGeneration,
            ProjectOperatorResources projectOperator,
            AggregationOperatorResources aggregationOperator,
            HashJoinOperatorResources hashJoinOperator)
    {
        this.primitiveArrays = requireNonNull(primitiveArrays, "primitiveArrays is null");
        this.nativeBuffers = requireNonNull(nativeBuffers, "nativeBuffers is null");
        this.operatorCodeGeneration = requireNonNull(operatorCodeGeneration, "operatorCodeGeneration is null");
        this.projectOperator = requireNonNull(projectOperator, "projectOperator is null");
        this.aggregationOperator = requireNonNull(aggregationOperator, "aggregationOperator is null");
        this.hashJoinOperator = requireNonNull(hashJoinOperator, "hashJoinOperator is null");
    }

    /**
     * Constructs a new, isolated resource owner using Nitro's standalone retention defaults.
     *
     * <p>This is a factory for a fresh owner, not a shared resource accessor.
     */
    public static EngineResources createDefault()
    {
        return new EngineResources(
                new PrimitiveArrayPool(
                        Long.getLong("nitro.primitiveArrayPool.maxRetainedBytes", defaultMaxRetainedBytes()),
                        Long.getLong("nitro.primitiveArrayPool.minRetainedBytes", DEFAULT_MIN_RETAINED_BYTES)),
                new PrimitiveArrayPool(
                        Long.getLong("nitro.nativeBufferPool.maxRetainedBytes", DEFAULT_MAX_RETAINED_NATIVE_BYTES),
                        Long.getLong("nitro.nativeBufferPool.minRetainedBytes", DEFAULT_MIN_RETAINED_BYTES)),
                new OperatorCodeGenerationResources(),
                new ProjectOperatorResources(),
                new AggregationOperatorResources(),
                new HashJoinOperatorResources(Boolean.parseBoolean(
                        System.getProperty("nitro.hash.join.shareBufferPoolAcrossOperators", "true"))));
    }

    public PrimitiveArrayPool primitiveArrays()
    {
        checkOpen();
        return primitiveArrays;
    }

    public PrimitiveArrayPool nativeBuffers()
    {
        checkOpen();
        return nativeBuffers;
    }

    public OperatorCodeGenerationResources operatorCodeGeneration()
    {
        checkOpen();
        return operatorCodeGeneration;
    }

    public ProjectOperatorResources projectOperator()
    {
        checkOpen();
        return projectOperator;
    }

    public AggregationOperatorResources aggregationOperator()
    {
        checkOpen();
        return aggregationOperator;
    }

    public HashJoinOperatorResources hashJoinOperator()
    {
        checkOpen();
        return hashJoinOperator;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        primitiveArrays.close();
        if (nativeBuffers != primitiveArrays) {
            nativeBuffers.close();
        }
        operatorCodeGeneration.close();
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("Engine resources are closed");
        }
    }

    private static long defaultMaxRetainedBytes()
    {
        // Keep the original bounded footprint on small heaps, but let large analytic-query heaps retain enough of
        // their actual primitive working set to avoid recreating it every invocation. The hard upper bound remains
        // below one tenth of the 12 GiB publication heap, and the explicit property remains authoritative.
        return Math.min(
                MAX_DEFAULT_MAX_RETAINED_BYTES,
                Math.max(MIN_DEFAULT_MAX_RETAINED_BYTES, Runtime.getRuntime().maxMemory() / 12));
    }
}
