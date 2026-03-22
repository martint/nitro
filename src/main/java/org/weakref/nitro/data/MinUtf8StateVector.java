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

public final class MinUtf8StateVector
        implements FlatVector
{
    private byte[][] values;
    private boolean[] nulls;

    public MinUtf8StateVector(int size)
    {
        this.values = new byte[size][];
        this.nulls = new boolean[size];
        Arrays.fill(this.nulls, true);
    }

    public static MinUtf8StateVector grow(MinUtf8StateVector source, int size)
    {
        if (source.length() >= size) {
            return source;
        }

        MinUtf8StateVector grown = new MinUtf8StateVector(size);
        System.arraycopy(source.values, 0, grown.values, 0, source.values.length);
        System.arraycopy(source.nulls, 0, grown.nulls, 0, source.nulls.length);
        return grown;
    }

    @Override
    public int length()
    {
        return values.length;
    }

    @Override
    public long retainedBytes()
    {
        long retained = (long) nulls.length;
        for (byte[] value : values) {
            retained += value == null ? 0 : value.length;
        }
        return retained;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext)
    {
        MinUtf8StateVector copy = new MinUtf8StateVector(length());
        System.arraycopy(nulls, 0, copy.nulls, 0, nulls.length);
        for (int index = 0; index < values.length; index++) {
            copy.values[index] = values[index] == null ? null : Arrays.copyOf(values[index], values[index].length);
        }
        return allocator.adopt(allocationContext, copy);
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        throw new UnsupportedOperationException("MinUtf8StateVector does not support positional copy");
    }

    public boolean isNull(int group)
    {
        return nulls[group];
    }

    public byte[] value(int group)
    {
        return values[group];
    }

    public void setValue(int group, byte[] value)
    {
        values[group] = value;
        nulls[group] = value == null;
    }

    public void initialize(int offset, int length)
    {
        Arrays.fill(nulls, offset, offset + length, true);
        Arrays.fill(values, offset, offset + length, null);
    }
}
