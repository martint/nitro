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
import java.util.Iterator;

public class Mask
        implements Iterable<Integer>
{
    private final int[] positions;
    private final int count;
    private final boolean all;

    public static Mask all(int count)
    {
        int[] positions = new int[count];
        for (int i = 0; i < count; i++) {
            positions[i] = i;
        }

        return new Mask(positions, count, true);
    }

    public static Mask range(int start, int length)
    {
        int[] positions = new int[length];
        for (int i = 0; i < length; i++) {
            positions[i] = start + i;
        }

        return new Mask(positions, length, start == 0);
    }

    private Mask(int[] positions, int count, boolean all)
    {
        this.positions = positions;
        this.count = count;
        this.all = all;
    }

    public static Mask sparse(int[] positions, int count)
    {
        return new Mask(positions, count, count == positions.length);
    }

    public boolean all()
    {
        return all;
    }

    public int maxPosition()
    {
        return positions[count - 1];
    }

    public int position(int index)
    {
        // TODO verify index < count
        return positions[index];
    }

    public int count()
    {
        return count;
    }

    public boolean none()
    {
        return count == 0;
    }

    @Override
    public String toString()
    {
        return Arrays.toString(Arrays.copyOf(positions, count));
    }

    public Mask first(int n)
    {
        if (n >= count) {
            return this;
        }

        return new Mask(Arrays.copyOf(positions, n), n, false);
    }

    public Mask last(int n)
    {
        if (n >= count) {
            return this;
        }

        int[] positions = new int[n];
        System.arraycopy(this.positions, count - n, positions, 0, n);
        return new Mask(positions, n, false);
    }

    @Override
    public Iterator<Integer> iterator()
    {
        return new Iterator<>()
        {
            private int index;

            @Override
            public boolean hasNext()
            {
                return index < count;
            }

            @Override
            public Integer next()
            {
                return positions[index++];
            }
        };
    }
}
