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
 * Immutable output-batch sizing for a native Parquet scan.
 */
public record ParquetScanBatchPolicy(int maxRows)
{
    public static ParquetScanBatchPolicy defaults()
    {
        return new ParquetScanBatchPolicy(10_000);
    }

    public static ParquetScanBatchPolicy fromSystemProperties()
    {
        return new ParquetScanBatchPolicy(
                Integer.getInteger("nitro.parquet.scan.maxBatchRows", 10_000));
    }
}
