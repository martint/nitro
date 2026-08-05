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

/**
 * Physical ordering guaranteed by the producer of a window input.
 *
 * @param partitionsContiguous every equal PARTITION BY key occupies one contiguous input range
 * @param orderingPrefix number of leading ORDER BY keys already ordered within each partition
 */
public record WindowInputOrder(boolean partitionsContiguous, int orderingPrefix)
{
    public WindowInputOrder
    {
        if (orderingPrefix < 0 || (!partitionsContiguous && orderingPrefix != 0)) {
            throw new IllegalArgumentException("Invalid window input order");
        }
    }

    public static WindowInputOrder unordered()
    {
        return new WindowInputOrder(false, 0);
    }

    public boolean isFullyOrdered(int orderingColumns)
    {
        return partitionsContiguous && orderingPrefix == orderingColumns;
    }
}
