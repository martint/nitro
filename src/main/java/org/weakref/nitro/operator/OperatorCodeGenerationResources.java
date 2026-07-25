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

import org.weakref.nitro.jit.FusedProjectionCompiler;
import org.weakref.nitro.jit.ProjectionMaskCompiler;

/**
 * Engine-owned generated operator kernels.
 *
 * <p>The embedding engine constructs this resource and passes it through {@code EngineResources}. Generated classes
 * and their shape caches therefore have the same explicit lifetime as the engine owner rather than the JVM process.
 */
public final class OperatorCodeGenerationResources
        implements AutoCloseable
{
    private final FusedProjectionCompiler fusedProjection = new FusedProjectionCompiler();
    private final ProjectionMaskCompiler projectionMask = new ProjectionMaskCompiler();
    private final FusedGroupingAggregationKernelGenerator fusedGrouping = new FusedGroupingAggregationKernelGenerator();
    private final MultiLongGroupingTableGenerator multiLongGrouping = new MultiLongGroupingTableGenerator();
    private final AdaptiveLongGroupingTableGenerator adaptiveLongGrouping = new AdaptiveLongGroupingTableGenerator();
    private final DictionaryHashBatchKernelGenerator dictionaryHash = new DictionaryHashBatchKernelGenerator();
    private final MixedComposite3GroupingKernelGenerator mixedComposite3Grouping = new MixedComposite3GroupingKernelGenerator();
    private final DictionaryRecordEqualityKernelGenerator dictionaryRecordEquality = new DictionaryRecordEqualityKernelGenerator();
    private boolean closed;

    FusedProjectionCompiler fusedProjection()
    {
        checkOpen();
        return fusedProjection;
    }

    public ProjectionMaskCompiler projectionMask()
    {
        checkOpen();
        return projectionMask;
    }

    FusedGroupingAggregationKernelGenerator fusedGrouping()
    {
        checkOpen();
        return fusedGrouping;
    }

    MultiLongGroupingTableGenerator multiLongGrouping()
    {
        checkOpen();
        return multiLongGrouping;
    }

    AdaptiveLongGroupingTableGenerator adaptiveLongGrouping()
    {
        checkOpen();
        return adaptiveLongGrouping;
    }

    DictionaryHashBatchKernelGenerator dictionaryHash()
    {
        checkOpen();
        return dictionaryHash;
    }

    MixedComposite3GroupingKernelGenerator mixedComposite3Grouping()
    {
        checkOpen();
        return mixedComposite3Grouping;
    }

    DictionaryRecordEqualityKernelGenerator dictionaryRecordEquality()
    {
        checkOpen();
        return dictionaryRecordEquality;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        fusedProjection.close();
        fusedGrouping.close();
        multiLongGrouping.close();
        adaptiveLongGrouping.close();
        dictionaryHash.close();
        mixedComposite3Grouping.close();
        dictionaryRecordEquality.close();
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("Operator code-generation resources are closed");
        }
    }
}
