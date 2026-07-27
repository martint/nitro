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

/// Resolves opaque logical type identities supplied by a planner or integration adapter.
///
/// The engine carries the returned bindings but does not interpret their identities. An embedding
/// environment can assemble this registry from dynamically loaded type providers without exposing
/// provider or host-engine objects to operators.
public interface TypeRegistry
{
    TypeBinding resolve(TypeIdentity identity);
}
