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

public class I64Vector
        implements Vector
{
    private final boolean[] nulls;
    private final long[] values;

    public I64Vector(int size)
    {
        this(new boolean[size], new long[size]);
    }

    I64Vector(boolean[] nulls, long[] values)
    {
        this.nulls = nulls;
        this.values = values;
    }

    @Override
    public Vector copy(int size)
    {
        return new I64Vector(
                Arrays.copyOf(nulls, size),
                Arrays.copyOf(values, size));
    }

    public boolean[] nulls()
    {
        return nulls;
    }

    public long[] values()
    {
        return values;
    }

    @Override
    public int length()
    {
        return values.length;
    }
}
