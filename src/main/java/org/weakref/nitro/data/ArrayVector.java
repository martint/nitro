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

public final class ArrayVector
        implements FlatVector
{
    private final int positionCount;
    private final int[] offsets;
    private Streams elements = Streams.empty();

    public ArrayVector(int positionCount)
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

    public Streams elements()
    {
        return elements;
    }

    public Vector elementValues()
    {
        return elements.values();
    }

    public Vector elementStreamOrNull(Stream stream)
    {
        return elements.getOrNull(stream);
    }

    public void setElements(Streams elements)
    {
        this.elements = elements;
    }

    public void clearElements()
    {
        elements = Streams.empty();
    }

    @Override
    public int length()
    {
        return positionCount;
    }
}
