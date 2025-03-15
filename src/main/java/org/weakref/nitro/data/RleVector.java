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

import static com.google.common.base.Preconditions.checkArgument;

public class RleVector
        implements Vector
{
    private final int length;
    private final int[] counts;

    public RleVector(int[] counts, Vector values)
    {
        checkArgument(counts.length == values.length(), "Run lengths counts (%s) must match the length of the underlying values vector (%s)", counts.length, values.length());
        this.counts = counts;

        length = Arrays.stream(counts).sum();
    }

    @Override
    public Vector copy(int size)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public int length()
    {
        return length;
    }
}
