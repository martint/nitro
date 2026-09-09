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
package org.weakref.nitro.core.source;

/// Classloader-neutral view of an exact domain over raw binary values.
///
/// The enclosing [TypedDomain] remains authoritative for logical type and null membership. A source may consume
/// this capability only when its physical binary representation preserves the registered type's equality.
@FunctionalInterface
public interface BinaryDomain
{
    boolean test(byte[] data, int offset, int length);
}
