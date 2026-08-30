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
package org.weakref.nitro.parquet;

import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;

/** Preserves a common encoded row domain across independently encoded sibling Parquet leaves. */
final class DictionaryDomainCoalescer
{
    private DictionaryDomainCoalescer() {}

    /**
     * Returns sibling streams over one row-mapping identity when every value dictionary uses the same ids and
     * physical-domain size. Parquet stores leaves independently, so this relationship must be recovered after
     * decoding before an enclosing MAP/ROW hides it from downstream encoded-domain consumers.
     */
    static Streams[] coalesce(Streams... siblings)
    {
        if (siblings.length < 2 || !(siblings[0].values() instanceof DictionaryVector anchor)) {
            return siblings;
        }
        int rowCount = anchor.length();
        int domainSize = anchor.values().length();
        for (Streams sibling : siblings) {
            if (!(sibling.values() instanceof DictionaryVector dictionary) ||
                    dictionary.length() != rowCount ||
                    dictionary.values().length() != domainSize ||
                    !sameIds(anchor, dictionary, rowCount)) {
                return siblings;
            }
            for (Stream stream : sibling.streams()) {
                Vector side = sibling.get(stream);
                if (side instanceof DictionaryVector sideDictionary &&
                        (sideDictionary.length() != rowCount ||
                                sideDictionary.values().length() != domainSize ||
                                !sameIds(anchor, sideDictionary, rowCount))) {
                    return siblings;
                }
            }
        }

        Streams[] result = siblings.clone();
        for (int sibling = 0; sibling < result.length; sibling++) {
            Streams.Builder streams = Streams.builder();
            for (Stream stream : result[sibling].streams()) {
                Vector vector = result[sibling].get(stream);
                streams.put(stream, vector instanceof DictionaryVector dictionary
                        ? anchor.sharedMappingWithValues(dictionary.values())
                        : vector);
            }
            result[sibling] = streams.build();
        }
        return result;
    }

    static boolean sameIds(DictionaryVector left, DictionaryVector right, int length)
    {
        return left.length() == length && right.length() == length && left.hasEquivalentRowMapping(right);
    }
}
