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

/**
 * A vector whose retained size may change during its tracked lifetime.
 *
 * <p>The allocator binds this callback whenever it tracks the vector, including pooled reuse and adoption. Concrete
 * vectors can therefore report representation changes without requiring their callers to participate in accounting.
 */
public interface DynamicRetainedBytesVector
        extends Vector
{
    /**
     * Binds retained-size changes to the allocator context that currently owns this vector.
     */
    void bindRetainedBytesAccounting(Allocator allocator, Allocator.Context context);
}
