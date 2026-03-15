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

public class BooleanVector
        implements FlatVector
{
    private final boolean[] values;

    public BooleanVector(int size)
    {
        this(new boolean[size]);
    }

    public BooleanVector(boolean[] values)
    {
        this.values = values;
    }

    public boolean[] values()
    {
        return values;
    }

    @Override
    public int length()
    {
        return values.length;
    }

    @Override
    public String toString()
    {
        return "BooleanVector" + Arrays.toString(values);
    }
}
