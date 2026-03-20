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

public final class AvgStateVector
        implements FlatVector
{
    private final long[] sums;
    private final long[] counts;

    public AvgStateVector(int size)
    {
        this.sums = new long[size];
        this.counts = new long[size];
    }

    public long[] sums()
    {
        return sums;
    }

    public long[] counts()
    {
        return counts;
    }

    @Override
    public int length()
    {
        return sums.length;
    }
}
