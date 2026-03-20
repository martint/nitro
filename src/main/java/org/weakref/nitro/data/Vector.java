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

import java.util.function.Consumer;

public sealed interface Vector
        permits DictionaryVector, FlatVector, RleVector
{
    enum PoolingMode
    {
        NONE,
        STANDARD,
        BINARY,
    }

    int length();

    long retainedBytes();

    Vector copy(Allocator allocator, Allocator.Context allocationContext);

    Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions);

    default void copyInto(Vector target)
    {
        throw new UnsupportedOperationException("Vector does not support copyInto: " + getClass().getSimpleName());
    }

    default void clearForReuse()
    {
        throw new UnsupportedOperationException("Vector does not support clearForReuse: " + getClass().getSimpleName());
    }

    default PoolingMode poolingMode()
    {
        return PoolingMode.NONE;
    }

    default void forEachChildVector(Consumer<Vector> consumer)
    {
    }
}
