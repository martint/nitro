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

import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Vector;

/// Connector-supplied logical interpretation of one primitive Parquet leaf.
///
/// The Parquet reader owns physical decoding. A binding validates the file annotation against the
/// requested connector type, selects the physical vector shape to decode, and converts that vector
/// into the registered Nitro representation. Implementations must be immutable and instance-owned.
public interface ParquetPrimitiveValueBinding
{
    Bound bind(ParquetPrimitiveDescriptor source, TypeBinding outputType);

    interface Bound
    {
        /// Physical vector representation the generic decoder should produce.
        Class<? extends Vector> decodedVectorType();

        /// Converts a freshly decoded vector. The result and any allocated children belong to context.
        Vector convert(Allocator allocator, Allocator.Context context, Vector decoded);

        /// Whether a logical long domain can be applied directly to the physical values.
        ///
        /// False is the safe default. A future binding may expose an explicit domain translation
        /// contract; value conversion alone is not proof that min/max or dictionary pruning is sound.
        default boolean preservesLongDomain()
        {
            return false;
        }
    }
}
