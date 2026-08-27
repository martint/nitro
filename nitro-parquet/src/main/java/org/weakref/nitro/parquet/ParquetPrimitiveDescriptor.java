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

import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.Type;

/// Physical and annotated logical metadata for one primitive Parquet leaf.
///
/// This is source-format metadata, not an engine logical type. A connector uses it together with
/// its requested logical type and session semantics to bind a conversion.
public record ParquetPrimitiveDescriptor(
        Type physicalType,
        ConvertedType convertedType,
        LogicalType logicalType,
        int typeLength,
        int precision,
        int scale)
{
    public ParquetPrimitiveDescriptor
    {
        if (physicalType == null) {
            throw new NullPointerException("physicalType is null");
        }
    }
}
