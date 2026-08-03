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
 * Immutable admission policy for executable runtime filters in a Parquet scan.
 *
 * <p>The source reports accepted filters as residual, so a connector composition root may disable either optimization
 * without changing query results.
 */
public record ParquetRuntimeFilterPolicy(
        boolean rowGroupFiltering,
        boolean rowLevelFiltering,
        boolean nullableRowLevelFiltering)
{
    public static ParquetRuntimeFilterPolicy defaults()
    {
        return new ParquetRuntimeFilterPolicy(true, true, true);
    }
}
