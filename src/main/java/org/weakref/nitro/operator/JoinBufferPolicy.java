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

/// Engine-selected vector copying and position-mapping policy used by buffered operators.
///
/// The property-backed factory is a standalone composition adapter. Buffering helpers receive one immutable policy
/// from their resource owner and never consult process-global configuration.
public record JoinBufferPolicy(
        boolean positionMappingCache,
        int positionMappingCacheSize,
        boolean fuseDictionaryPositionCopies,
        boolean copyAndCompactRangeSelection,
        boolean preserveDictionaryBinaryPositionCopies,
        boolean compactAllFalsePositionCopies,
        boolean indexedVectorTreeTraversal)
{
    public static JoinBufferPolicy defaults()
    {
        return new JoinBufferPolicy(true, 8, true, true, true, true, true);
    }

    public static JoinBufferPolicy fromSystemProperties()
    {
        JoinBufferPolicy defaults = defaults();
        return new JoinBufferPolicy(
                booleanProperty("nitro.join.positionMappingCache", defaults.positionMappingCache()),
                defaults.positionMappingCacheSize(),
                booleanProperty("nitro.join.fuseDictionaryPositionCopies", defaults.fuseDictionaryPositionCopies()),
                booleanProperty("nitro.join.copyAndCompactRangeSelection", defaults.copyAndCompactRangeSelection()),
                booleanProperty(
                        "nitro.join.preserveDictionaryBinaryPositionCopies",
                        defaults.preserveDictionaryBinaryPositionCopies()),
                booleanProperty(
                        "nitro.join.compactAllFalsePositionCopies",
                        defaults.compactAllFalsePositionCopies()),
                booleanProperty("nitro.allocator.indexedVectorTreeTraversal", defaults.indexedVectorTreeTraversal()));
    }

    private static boolean booleanProperty(String name, boolean defaultValue)
    {
        return Boolean.parseBoolean(System.getProperty(name, Boolean.toString(defaultValue)));
    }
}
