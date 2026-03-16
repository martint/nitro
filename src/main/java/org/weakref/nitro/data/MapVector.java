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

import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import static com.google.common.base.Preconditions.checkArgument;

public final class MapVector
        implements FlatVector
{
    private final int positionCount;
    private final int[] offsets;
    private Streams keys = Streams.empty();
    private Streams values = Streams.empty();

    public MapVector(int positionCount)
    {
        checkArgument(positionCount >= 0, "positionCount is negative");
        this.positionCount = positionCount;
        this.offsets = new int[positionCount + 1];
    }

    public int[] offsets()
    {
        return offsets;
    }

    public int startOffset(int position)
    {
        return offsets[position];
    }

    public int endOffset(int position)
    {
        return offsets[position + 1];
    }

    public int length(int position)
    {
        return endOffset(position) - startOffset(position);
    }

    public Streams keys()
    {
        return keys;
    }

    public Streams values()
    {
        return values;
    }

    public Vector keyValues()
    {
        return keys.values();
    }

    public Vector keyStreamOrNull(Stream stream)
    {
        return keys.getOrNull(stream);
    }

    public Vector valueValues()
    {
        return values.values();
    }

    public Vector valueStreamOrNull(Stream stream)
    {
        return values.getOrNull(stream);
    }

    public void setEntries(Streams keys, Streams values)
    {
        this.keys = keys;
        this.values = values;
    }

    public void clearEntries()
    {
        keys = Streams.empty();
        values = Streams.empty();
    }

    @Override
    public int length()
    {
        return positionCount;
    }
}
