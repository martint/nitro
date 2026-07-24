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
package org.weakref.nitro.legacy.pipeline;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

/**
 * Planner/compiler-scoped registries and generated-class retention.
 *
 * <p>The embedding integration constructs this owner, loads connector/function contributions into its registries,
 * and passes it to compilers. Closing it releases generated classes and rejects later compilation. Nothing here is
 * discovered through process-global state.
 */
public final class CompilerResources
        implements AutoCloseable
{
    private final Types types;
    private final ScalarLibrary scalarFunctions;
    private final AggregateLibrary aggregateFunctions;
    private final Map<String, Class<?>> pipelineClasses = new ConcurrentHashMap<>();
    private final AtomicInteger classNames = new AtomicInteger();
    private boolean closed;

    public CompilerResources(Types types, ScalarLibrary scalarFunctions, AggregateLibrary aggregateFunctions)
    {
        this.types = requireNonNull(types, "types is null");
        this.scalarFunctions = requireNonNull(scalarFunctions, "scalarFunctions is null");
        this.aggregateFunctions = requireNonNull(aggregateFunctions, "aggregateFunctions is null");
    }

    /** Creates a fresh, isolated owner populated with Nitro's standalone built-ins. */
    public static CompilerResources createDefault()
    {
        return new CompilerResources(new Types(), new ScalarLibrary(), new AggregateLibrary());
    }

    public Types types()
    {
        checkOpen();
        return types;
    }

    public ScalarLibrary scalarFunctions()
    {
        checkOpen();
        return scalarFunctions;
    }

    public AggregateLibrary aggregateFunctions()
    {
        checkOpen();
        return aggregateFunctions;
    }

    Map<String, Class<?>> pipelineClasses()
    {
        checkOpen();
        return pipelineClasses;
    }

    int nextClassName()
    {
        checkOpen();
        return classNames.incrementAndGet();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        pipelineClasses.clear();
        closed = true;
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("Compiler resources are closed");
        }
    }
}
