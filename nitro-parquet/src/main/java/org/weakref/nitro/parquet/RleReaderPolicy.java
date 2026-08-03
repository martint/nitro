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

/**
 * Immutable execution choices for Parquet hybrid-RLE decoding.
 */
public record RleReaderPolicy(
        boolean unrolledUleb128,
        boolean swarUleb128,
        boolean scanAllOneDefinitionRuns,
        boolean directNullableDictionaryUnrollIds)
{
    public static RleReaderPolicy defaults()
    {
        return new RleReaderPolicy(true, true, true, false);
    }

    public static RleReaderPolicy fromSystemProperties()
    {
        return new RleReaderPolicy(
                Boolean.parseBoolean(System.getProperty("nitro.parquet.unrolledUleb128", "true")),
                Boolean.parseBoolean(System.getProperty("nitro.parquet.swarUleb128", "true")),
                Boolean.parseBoolean(System.getProperty("nitro.parquet.scanAllOneDefinitionRuns", "true")),
                Boolean.parseBoolean(System.getProperty("nitro.parquet.directNullableDictionaryUnrollIds", "false")));
    }
}
