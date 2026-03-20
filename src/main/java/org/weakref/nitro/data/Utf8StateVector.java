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

import java.util.Arrays;

public final class Utf8StateVector
        implements FlatVector
{
    private final byte[][] values;

    public Utf8StateVector(int size)
    {
        this.values = new byte[size][];
    }

    public byte[][] values()
    {
        return values;
    }

    @Override
    public int length()
    {
        return values.length;
    }

    @Override
    public long retainedBytes()
    {
        return Arrays.stream(values)
                .filter(java.util.Objects::nonNull)
                .mapToLong(entry -> entry.length)
                .sum();
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext)
    {
        Utf8StateVector copy = allocator.allocate(allocationContext, Utf8StateVector.class, values.length, Utf8StateVector::new);
        copyInto(copy);
        return copy;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        Utf8StateVector copy = allocator.allocate(allocationContext, Utf8StateVector.class, positions.length, Utf8StateVector::new);
        for (int index = 0; index < positions.length; index++) {
            byte[] value = values[positions[index]];
            copy.values()[index] = value == null ? null : Arrays.copyOf(value, value.length);
        }
        return copy;
    }

    @Override
    public void copyInto(Vector target)
    {
        System.arraycopy(values, 0, ((Utf8StateVector) target).values(), 0, values.length);
    }

    @Override
    public void clearForReuse()
    {
        Arrays.fill(values, null);
    }

    @Override
    public PoolingMode poolingMode()
    {
        return PoolingMode.STANDARD;
    }
}
