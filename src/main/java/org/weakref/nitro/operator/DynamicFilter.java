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
package org.weakref.nitro.operator;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

/**
 * A runtime (Velox-style) dynamic filter: the membership of a build side's join key, pushed down a probe operator
 * chain to the scan so it can eliminate non-matching rows during decode — decoding the filtered key column, testing
 * membership, and skip-decoding the remaining columns only for survivors. The target {@link #column()} is the probe
 * key column's index in the output space of the operator currently holding the filter; each operator remaps it
 * ({@link #withColumn}) as it forwards the filter toward its source.
 *
 * <p>This is a superset filter over a single {@code long} key: it only removes rows that cannot join, so an exact
 * downstream join still produces identical output. {@code min}/{@code max} gate the (cheaper) set membership test.
 */
public final class DynamicFilter
{
    // Above this value span a membership bitset would be too large; fall back to the hash set. Surrogate-key
    // domains (stores, time-of-day, demographics) are far smaller than this, so they take the bitset fast path.
    private static final long MAX_BITSET_SPAN = 1L << 26;   // up to ~64M entries (8MB), well above any dimension key range

    private final int column;
    private final LongSet values;
    private final long min;
    private final long max;
    // Velox-style membership representation: for a dense small integer domain, a bitset indexed by (value - min)
    // tests membership with a single array read -- no hashing -- which matters because the probe runs once per
    // scanned row. Null when the domain is too wide; then {@link #values} (a hash set) is consulted instead.
    private final boolean[] present;

    /** Build a filter over the distinct build-side key values for the given probe column. */
    public static DynamicFilter fromValues(int column, LongSet values)
    {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (long value : values) {
            min = Math.min(min, value);
            max = Math.max(max, value);
        }
        boolean[] present = null;
        if (!values.isEmpty()) {
            long span = max - min + 1;
            if (span > 0 && span <= MAX_BITSET_SPAN) {
                present = new boolean[(int) span];
                for (long value : values) {
                    present[(int) (value - min)] = true;
                }
            }
        }
        return new DynamicFilter(column, values, min, max, present);
    }

    private DynamicFilter(int column, LongSet values, long min, long max, boolean[] present)
    {
        this.column = column;
        this.values = values;
        this.min = min;
        this.max = max;
        this.present = present;
    }

    public int column()
    {
        return column;
    }

    /** The number of distinct build-side values; a proxy for selectivity used to order filter application. */
    public int size()
    {
        return values.size();
    }

    public boolean isEmpty()
    {
        return values.isEmpty();
    }

    /** Whether {@code value} can join: a range gate, then a bitset read (small domains) or hash-set probe. */
    public boolean accepts(long value)
    {
        if (value < min || value > max) {
            return false;
        }
        if (present != null) {
            return present[(int) (value - min)];
        }
        return values.contains(value);
    }

    /** The same filter retargeted to {@code newColumn} in a source operator's output space. */
    public DynamicFilter withColumn(int newColumn)
    {
        return new DynamicFilter(newColumn, values, min, max, present);
    }

    /** A defensive snapshot helper for collecting distinct build keys. */
    public static LongSet newValueSet()
    {
        return new LongOpenHashSet();
    }
}
