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

import java.util.List;

import static java.util.Objects.requireNonNull;

/// Immutable physical ordering required of an operator's input rows.
public record PhysicalOrdering(List<Key> keys)
{
    public PhysicalOrdering
    {
        keys = List.copyOf(requireNonNull(keys, "keys is null"));
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("Physical ordering requires at least one key");
        }
    }

    public int[] columns()
    {
        return keys.stream().mapToInt(Key::column).toArray();
    }

    public boolean[] descending()
    {
        boolean[] descending = new boolean[keys.size()];
        for (int index = 0; index < keys.size(); index++) {
            descending[index] = keys.get(index).descending();
        }
        return descending;
    }

    public boolean[] nullsFirst()
    {
        boolean[] nullsFirst = new boolean[keys.size()];
        for (int index = 0; index < keys.size(); index++) {
            nullsFirst[index] = keys.get(index).nullsFirst();
        }
        return nullsFirst;
    }

    public record Key(int column, boolean descending, boolean nullsFirst)
    {
        public Key
        {
            if (column < 0) {
                throw new IllegalArgumentException("Ordering column is negative");
            }
        }
    }
}
