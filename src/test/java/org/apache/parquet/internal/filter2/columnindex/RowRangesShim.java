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
package org.apache.parquet.internal.filter2.columnindex;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

/**
 * Same-package shim that builds a {@link RowRanges} from an arbitrary sorted list of surviving row indices,
 * coalescing consecutive indices into ranges. {@code RowRanges.Range(long,long)} is package-private (callable here);
 * {@code RowRanges(List)} is private (reached via reflection). This is the glue that lets a runtime, value-level
 * survivor mask drive Trino's existing skip-decode read loop -- the one piece missing from the public API.
 */
public final class RowRangesShim
{
    private RowRangesShim() {}

    public static RowRanges fromSortedPositions(long[] positions, int count)
    {
        List<RowRanges.Range> ranges = new ArrayList<>();
        int i = 0;
        while (i < count) {
            long from = positions[i];
            long to = from;
            int j = i + 1;
            while (j < count && positions[j] == to + 1) {
                to = positions[j];
                j++;
            }
            ranges.add(new RowRanges.Range(from, to));
            i = j;
        }
        try {
            Constructor<RowRanges> ctor = RowRanges.class.getDeclaredConstructor(List.class);
            ctor.setAccessible(true);
            return ctor.newInstance(ranges);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("RowRanges(List) reflection failed", e);
        }
    }
}
