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
 * Immutable choices for locating and selecting Parquet pages while a column reader advances.
 */
public record ParquetPageNavigationPolicy(
        boolean fastPageHeader,
        int fastPageHeaderMaxChunks,
        int skipPageDensePercent,
        int skipPageMinAverageRun,
        boolean binarySearchPageSurvivors,
        int binarySearchPageSurvivorMaxPercent,
        int binarySearchPageMinSurvivors,
        boolean skipWholeChunks)
{
    public static ParquetPageNavigationPolicy defaults()
    {
        return new ParquetPageNavigationPolicy(true, 64, 50, 100, true, 1, 64, true);
    }

    public static ParquetPageNavigationPolicy fromSystemProperties()
    {
        return new ParquetPageNavigationPolicy(
                Boolean.parseBoolean(System.getProperty("nitro.parquet.fastPageHeader", "true")),
                Integer.getInteger("nitro.parquet.fastPageHeaderMaxChunks", 64),
                Integer.getInteger("nitro.parquet.skipPageDensePercent", 50),
                Integer.getInteger("nitro.parquet.skipPageMinAvgRun", 100),
                Boolean.parseBoolean(System.getProperty("nitro.parquet.binarySearchPageSurvivors", "true")),
                Integer.getInteger("nitro.parquet.binarySearchPageSurvivorMaxPercent", 1),
                Integer.getInteger("nitro.parquet.binarySearchPageMinSurvivors", 64),
                Boolean.parseBoolean(System.getProperty("nitro.parquet.skipWholeChunks", "true")));
    }
}
