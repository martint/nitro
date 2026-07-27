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

/// Engine-selected representation policy shared by runtime dynamic-filter producers.
public record DynamicFilterPolicy(long maxDenseBitsetSpan)
{
    public DynamicFilterPolicy
    {
        if (maxDenseBitsetSpan < 0 || maxDenseBitsetSpan > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("maxDenseBitsetSpan must be between 0 and Integer.MAX_VALUE");
        }
    }

    public static DynamicFilterPolicy defaults()
    {
        return new DynamicFilterPolicy(1L << 26);
    }

    public static DynamicFilterPolicy fromSystemProperties()
    {
        DynamicFilterPolicy defaults = defaults();
        return new DynamicFilterPolicy(
                Long.getLong("nitro.dynamicFilter.maxDenseBitsetSpan", defaults.maxDenseBitsetSpan()));
    }
}
