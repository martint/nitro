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

import org.weakref.nitro.data.Vector;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Provider-owned binding from an admitted physical vector to an exact normalized ordering key.
 *
 * <p>Unsigned comparison of returned keys must have the same sign as the logical comparison of
 * the corresponding non-null values, and logical peers must have equal keys. Providers should
 * expose this capability only for domains whose complete ordering fits in 64 bits. Null placement
 * remains a consumer concern and is not encoded in the key.
 */
@FunctionalInterface
public interface TypeOrderKeyBinder
{
    Optional<Bound> bind(Vector values);

    static TypeOrderKeyBinder signedLong()
    {
        return values -> {
            requireNonNull(values, "values is null");
            try {
                org.weakref.nitro.data.VectorAccess.LongValues accessor =
                        org.weakref.nitro.data.VectorAccess.longValues(values);
                return Optional.of(position -> accessor.value(position) ^ Long.MIN_VALUE);
            }
            catch (IllegalArgumentException _) {
                return Optional.empty();
            }
        };
    }

    @FunctionalInterface
    interface Bound
    {
        long key(int position);
    }
}
