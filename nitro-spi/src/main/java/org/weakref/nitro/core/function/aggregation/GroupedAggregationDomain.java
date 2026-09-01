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

import org.weakref.nitro.data.DictionaryVector;
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
    private final DictionaryVector rowMapping;
    private final int[] representatives;

    public GroupedAggregationDomain(
            Vector groups,
            int[] frequencies,
            int size,
            DictionaryVector rowMapping,
            int[] representatives)
    {
        this.groups = requireNonNull(groups, "groups is null");
        this.frequencies = requireNonNull(frequencies, "frequencies is null");
        this.rowMapping = requireNonNull(rowMapping, "rowMapping is null");
        this.representatives = representatives;
        checkArgument(size >= 0, "size is negative");
        checkArgument(size <= groups.length(), "size exceeds group domain length");
        checkArgument(size <= frequencies.length, "size exceeds frequency capacity");
        checkArgument(size == rowMapping.values().length(), "size differs from row-mapping domain length");
        checkArgument(representatives == null || size <= representatives.length, "size exceeds representative capacity");
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

    /**
     * Returns whether an encoded input has the exact logical-row-to-domain mapping from which this domain was
     * formed. Matching only domain width is insufficient: independently encoded columns may assign different
     * physical positions to the same logical rows.
     */
    public boolean hasSameRowMapping(DictionaryVector input)
    {
        return rowMapping.hasSameRowMapping(input);
    }

    public DictionaryVector rowMapping()
    {
        return rowMapping;
    }

    /**
     * Returns one selected logical position represented by a used physical-domain entry. Providers request this
     * optional view when one argument is aligned with the physical domain but another payload must be read in the
     * original logical position space.
     */
    public int representative(int domain)
    {
        if (representatives == null) {
            throw new IllegalStateException("Grouped domain does not carry logical representatives");
        }
        if (domain < 0 || domain >= size) {
            throw new IndexOutOfBoundsException("Domain position " + domain + " is out of bounds for size " + size);
        }
        int representative = representatives[domain];
        if (representative < 0) {
            throw new IllegalArgumentException("Unused domain position does not have a logical representative");
        }
        return representative;
    }
}
