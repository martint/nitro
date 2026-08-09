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

/** Immutable physical-encoding policy for native batch partition output. */
public record NativeBatchPartitionPolicy(
        boolean preserveDictionaryEncoding,
        int minimumDictionaryReuse,
        int maximumCopiedDictionaryEntries)
{
    public NativeBatchPartitionPolicy
    {
        if (minimumDictionaryReuse < 1) {
            throw new IllegalArgumentException("minimumDictionaryReuse must be positive");
        }
        if (maximumCopiedDictionaryEntries < 0) {
            throw new IllegalArgumentException("maximumCopiedDictionaryEntries is negative");
        }
    }

    public static NativeBatchPartitionPolicy defaults()
    {
        return new NativeBatchPartitionPolicy(true, 2, 65_536);
    }

    public boolean preserveDictionary(int dictionaryEntries, int outputPositions)
    {
        return preserveDictionaryEncoding &&
                dictionaryEntries <= maximumCopiedDictionaryEntries &&
                (long) dictionaryEntries * minimumDictionaryReuse <= outputPositions;
    }
}
