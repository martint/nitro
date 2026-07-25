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

import java.util.function.IntFunction;
import java.util.function.Supplier;

/// Connector-safe allocator scope for constructing Nitro SPI vectors.
///
/// Implementations own one allocation context. Closing the scope releases every vector that has
/// not been individually released or transferred to a downstream owner.
public interface VectorAllocator
        extends AutoCloseable
{
    <T extends Vector> T allocate(Class<T> vectorType, int size, IntFunction<T> factory);

    <T extends Vector> T allocatePooled(
            Object poolFamily,
            int minimumPoolCapacity,
            boolean exactCapacityMatch,
            Class<T> vectorType,
            Supplier<T> factory);

    <T extends Vector> T adopt(T vector);

    DictionaryVector dictionary(int[] ids, int length, Vector values);

    RleVector runLength(int[] counts, Vector values);

    <T extends Vector> T transfer(T vector);

    void release(Vector vector);

    @Override
    void close();
}
