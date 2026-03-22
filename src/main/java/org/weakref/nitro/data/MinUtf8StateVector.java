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
    private long retainedBytes;

    public MinUtf8StateVector(int size)
    {
        this.values = new byte[size][];
        this.nulls = new boolean[size];
        Arrays.fill(this.nulls, true);
        this.retainedBytes = this.nulls.length;
    }

    private MinUtf8StateVector(byte[][] values, boolean[] nulls, long retainedBytes)
    {
        this.values = values;
        this.nulls = nulls;
        this.retainedBytes = retainedBytes;
    }

    public static MinUtf8StateVector grow(MinUtf8StateVector source, int size)
    {
        if (source.length() >= size) {
            return source;
        }

        byte[][] values = Arrays.copyOf(source.values, size);
        boolean[] nulls = Arrays.copyOf(source.nulls, size);
        Arrays.fill(nulls, source.nulls.length, nulls.length, true);
        long retainedBytes = source.retainedBytes - source.nulls.length + nulls.length;
        return new MinUtf8StateVector(values, nulls, retainedBytes);
    }

    @Override
    public int length()
    {
        return values.length;
    }

    @Override
    public long retainedBytes()
    {
        return retainedBytes;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext)
    {
        byte[][] values = new byte[this.values.length][];
        boolean[] nulls = Arrays.copyOf(this.nulls, this.nulls.length);
        long retainedBytes = nulls.length;
        for (int index = 0; index < values.length; index++) {
            values[index] = this.values[index] == null ? null : Arrays.copyOf(this.values[index], this.values[index].length);
            retainedBytes += values[index] == null ? 0 : values[index].length;
        }
        return allocator.adopt(allocationContext, new MinUtf8StateVector(values, nulls, retainedBytes));
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
        byte[] previous = values[group];
        retainedBytes -= previous == null ? 0 : previous.length;
        values[group] = value;
        retainedBytes += value == null ? 0 : value.length;
        nulls[group] = value == null;
    }

    public void initialize(int offset, int length)
    {
        for (int index = offset; index < offset + length; index++) {
            byte[] value = values[index];
            retainedBytes -= value == null ? 0 : value.length;
        }
        Arrays.fill(nulls, offset, offset + length, true);
        Arrays.fill(values, offset, offset + length, null);
    }
}
