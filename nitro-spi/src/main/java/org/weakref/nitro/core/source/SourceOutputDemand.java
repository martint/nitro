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

import java.util.Set;

/// Optional pre-poll declaration of source outputs that the consumer may borrow.
///
/// A source may use this declaration to avoid materializing fields that exist only to enforce an accepted source
/// predicate. Undeclared fields remain part of the logical schema but must not be borrowed by the consumer. The
/// default is that every output may be borrowed.
@FunctionalInterface
public interface SourceOutputDemand
{
    void retainOutputs(Set<SourceColumnHandle> outputs);
}
