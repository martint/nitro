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

import org.weakref.nitro.core.function.VersionedLongPredicate;

/// Classloader-neutral view of a domain whose physical carrier is a long.
///
/// The type binding determines the logical type. Sources use this optional
/// protocol only after resolving it from a [TypedDomain]; the engine does not
/// inspect a provider's concrete domain representation.
public interface LongDomain
        extends VersionedLongPredicate
{
    int size();

    boolean isEmpty();

    /// Conservative fraction of the domain's numeric range that is accepted.
    double rangeDensity();

    /// Whether this domain can contain a value in the inclusive physical range.
    ///
    /// Sources may use this conservative proof to reject storage units from metadata. Implementations that do not
    /// expose bounds retain every unit by default.
    default boolean mayOverlap(long minimum, long maximum)
    {
        return true;
    }

    /// Whether every value in the inclusive physical range is accepted.
    ///
    /// Sources use this conservative proof to avoid installing a predicate that cannot reject any value described
    /// by their storage metadata. Implementations that do not expose complete coverage return false by default.
    default boolean containsAll(long minimum, long maximum)
    {
        return false;
    }
}
