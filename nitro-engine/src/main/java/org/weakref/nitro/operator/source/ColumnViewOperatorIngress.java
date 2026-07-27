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
package org.weakref.nitro.operator.source;

import org.weakref.nitro.core.batch.ColumnView;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.operator.Output;

import java.util.function.Supplier;

/// Engine-side binding for importing one logical source column into a native lazy output.
///
/// Implementations are constructed from registry/type physical metadata. The supplier defers
/// [ColumnView] lookup until the output is actually consumed, preserving source-side laziness.
public interface ColumnViewOperatorIngress
{
    TypeBinding type();

    Output output(Supplier<ColumnView> column);
}
