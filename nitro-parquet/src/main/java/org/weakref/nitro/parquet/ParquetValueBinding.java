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

import java.util.List;

import static java.util.Objects.requireNonNull;

/// Connector-supplied logical interpretation tree for a projected Parquet value.
///
/// Physical Parquet annotations determine whether a node is a struct, list, or map. A group binding
/// supplies only the corresponding logical children, in physical order; primitive children supply
/// their logical conversion through [ParquetPrimitiveValueBinding]. This keeps connector type
/// semantics out of the reader while allowing conversions at any depth.
public sealed interface ParquetValueBinding
        permits ParquetPrimitiveValueBinding, ParquetValueBinding.Direct, ParquetValueBinding.Group
{
    static Direct direct()
    {
        return Direct.INSTANCE;
    }

    static Group group(List<? extends ParquetValueBinding> children)
    {
        return new Group(List.copyOf(requireNonNull(children, "children is null")));
    }

    record Group(List<ParquetValueBinding> children)
            implements ParquetValueBinding
    {
        public Group
        {
            children = List.copyOf(requireNonNull(children, "children is null"));
        }
    }

    enum Direct
            implements ParquetValueBinding
    {
        INSTANCE
    }
}
