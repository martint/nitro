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
package org.weakref.nitro.core.function.aggregation;

import org.weakref.nitro.data.Vector;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * A weighted physical key domain for grouped aggregation.
 *
 * <p>Each position names one resolved group and carries the exact number of selected logical rows represented by
 * that physical key. The view is valid only for the duration of the aggregation call and must not be retained.
 */
public final class GroupedAggregationDomain
{
    private final Vector groups;
    private final int[] frequencies;
    private final int size;

    public GroupedAggregationDomain(Vector groups, int[] frequencies, int size)
    {
        this.groups = requireNonNull(groups, "groups is null");
        this.frequencies = requireNonNull(frequencies, "frequencies is null");
        checkArgument(size >= 0, "size is negative");
        checkArgument(size <= groups.length(), "size exceeds group domain length");
        checkArgument(size <= frequencies.length, "size exceeds frequency capacity");
        this.size = size;
    }

    public Vector groups()
    {
        return groups;
    }

    public int size()
    {
        return size;
    }

    public int frequency(int domain)
    {
        if (domain < 0 || domain >= size) {
            throw new IndexOutOfBoundsException("Domain position " + domain + " is out of bounds for size " + size);
        }
        return frequencies[domain];
    }
}
