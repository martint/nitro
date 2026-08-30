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
package org.weakref.nitro.core.type;

/// Provider-owned key operations bound to one admitted physical vector.
///
/// Binding lets a type provider resolve vector encodings, structural children, and typed accessors
/// once per batch. Positions remain logical positions in the bound vector. The engine owns hash-table
/// policy and invokes these semantics without learning the logical type or its physical representation.
public interface BoundTypeKey
{
    long hash(int position);

    boolean identical(int position, BoundTypeKey other, int otherPosition);
}
