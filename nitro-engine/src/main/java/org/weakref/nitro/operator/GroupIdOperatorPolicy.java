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
 * Engine-selected physical policies for grouping-set expansion.
 */
public record GroupIdOperatorPolicy(
        int maxInputBatchRows,
        boolean shareDenseDictionaryIds,
        boolean propagateDenseDictionaryMappingIdentity,
        int denseDictionaryMappingMinGroupingSets,
        boolean useKnownFalseMetadata,
        boolean compactAllFalseStreams)
{
    public GroupIdOperatorPolicy
    {
        if (maxInputBatchRows <= 0) {
            throw new IllegalArgumentException("maxInputBatchRows must be positive");
        }
        if (denseDictionaryMappingMinGroupingSets < 0) {
            throw new IllegalArgumentException("denseDictionaryMappingMinGroupingSets is negative");
        }
    }

    public static GroupIdOperatorPolicy defaults()
    {
        return new GroupIdOperatorPolicy(1 << 16, true, true, 8, true, true);
    }

    public static GroupIdOperatorPolicy fromSystemProperties()
    {
        GroupIdOperatorPolicy defaults = defaults();
        return new GroupIdOperatorPolicy(
                Integer.getInteger("nitro.groupId.maxInputBatchRows", defaults.maxInputBatchRows()),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.groupId.shareDenseDictionaryIds",
                        Boolean.toString(defaults.shareDenseDictionaryIds()))),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.groupId.propagateDenseDictionaryMappingIdentity",
                        Boolean.toString(defaults.propagateDenseDictionaryMappingIdentity()))),
                Integer.getInteger(
                        "nitro.groupId.denseDictionaryMappingMinGroupingSets",
                        defaults.denseDictionaryMappingMinGroupingSets()),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.groupId.useKnownFalseMetadata",
                        Boolean.toString(defaults.useKnownFalseMetadata()))),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.groupId.compactAllFalseStreams",
                        Boolean.toString(defaults.compactAllFalseStreams()))));
    }
}
