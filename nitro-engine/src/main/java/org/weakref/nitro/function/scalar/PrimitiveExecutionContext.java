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
package org.weakref.nitro.function.scalar;

import org.weakref.nitro.data.Allocator;

import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public final class PrimitiveExecutionContext
{
    private final Allocator allocator;
    private final Map<String, Allocator.Context> allocationContexts = new HashMap<>();
    private final Map<Object, Object> states = new IdentityHashMap<>();

    public PrimitiveExecutionContext(Allocator allocator)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
    }

    public Allocator allocator()
    {
        return allocator;
    }

    public Allocator.Context allocationContext(String name)
    {
        return allocationContexts.computeIfAbsent(name, Allocator.Context::new);
    }

    public Collection<Allocator.Context> allocationContexts()
    {
        return allocationContexts.values();
    }

    /// Returns invocation-local reusable state for an immutable function implementation.
    ///
    /// The execution context is evaluator-owned and thread-confined. Function implementations use
    /// identity keys so unrelated providers cannot collide and steady-state calls do not allocate
    /// temporary argument arrays or other batch scaffolding.
    public <T> T state(Object key, Supplier<T> factory)
    {
        requireNonNull(key, "key is null");
        requireNonNull(factory, "factory is null");
        @SuppressWarnings("unchecked")
        T state = (T) states.computeIfAbsent(key, _ -> requireNonNull(factory.get(), "factory returned null"));
        return state;
    }
}
