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

import org.weakref.nitro.core.type.FixedWidthKeyLayout;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Exact physical shape consumed by the fixed-width key-table generator.
 *
 * <p>Sources may use any supported primitive carrier and canonical lanes may consume any positive number of
 * sources. The descriptor assigns no logical meaning to either. A raw scalar or an N-column tuple is represented by
 * the same mechanism as a projected structural value.
 */
record FixedWidthKeyTableLayout(
        List<FixedWidthKeyLayout.Carrier> sourceCarriers,
        List<Integer> laneSourceCounts,
        List<Optional<MethodHandle>> projections)
{
    FixedWidthKeyTableLayout
    {
        sourceCarriers = List.copyOf(sourceCarriers);
        laneSourceCounts = List.copyOf(laneSourceCounts);
        projections = List.copyOf(projections);
        if (laneSourceCounts.size() != projections.size()) {
            throw new IllegalArgumentException("Invalid fixed-width key table layout");
        }
        if (laneSourceCounts.stream().anyMatch(count -> count == null || count <= 0) ||
                laneSourceCounts.stream().mapToInt(Integer::intValue).sum() != sourceCarriers.size()) {
            throw new IllegalArgumentException("Invalid fixed-width key table source layout");
        }
    }

    static FixedWidthKeyTableLayout from(ResolvedFixedWidthKeyLayout layout)
    {
        return new FixedWidthKeyTableLayout(layout.sourceCarriers(), layout.laneSourceCounts(), layout.projections());
    }

    static FixedWidthKeyTableLayout raw(List<FixedWidthKeyLayout.Carrier> carriers)
    {
        carriers = List.copyOf(carriers);
        return new FixedWidthKeyTableLayout(
                carriers,
                java.util.Collections.nCopies(carriers.size(), 1),
                java.util.Collections.nCopies(carriers.size(), Optional.empty()));
    }

    static FixedWidthKeyTableLayout rawI64(int laneCount)
    {
        if (laneCount <= 0) {
            throw new IllegalArgumentException("laneCount must be positive");
        }
        ArrayList<FixedWidthKeyLayout.Carrier> carriers = new ArrayList<>(laneCount);
        for (int lane = 0; lane < laneCount; lane++) {
            carriers.add(FixedWidthKeyLayout.Carrier.I64);
        }
        return raw(carriers);
    }

    int laneCount()
    {
        return laneSourceCounts.size();
    }
}
