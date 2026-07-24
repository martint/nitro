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

/**
 * Engine-owned generated operator kernels.
 *
 * <p>The embedding engine constructs this resource and passes it through {@code EngineResources}. Generated classes
 * and their shape caches therefore have the same explicit lifetime as the engine owner rather than the JVM process.
 */
public final class OperatorCodeGenerationResources
        implements AutoCloseable
{
    private final FusedGroupingAggregationKernelGenerator fusedGrouping = new FusedGroupingAggregationKernelGenerator();
    private boolean closed;

    FusedGroupingAggregationKernelGenerator fusedGrouping()
    {
        checkOpen();
        return fusedGrouping;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        fusedGrouping.close();
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("Operator code-generation resources are closed");
        }
    }
}
