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
package org.weakref.nitro.core.function.aggregation;

import static java.util.Objects.requireNonNull;

/** Classloader-neutral physical contribution emitted by a forward range provider. */
public record PrimitiveRangeContribution(Carrier carrier, NullConvention nullConvention)
{
    public PrimitiveRangeContribution
    {
        carrier = requireNonNull(carrier, "carrier is null");
        nullConvention = requireNonNull(nullConvention, "nullConvention is null");
        if (carrier == Carrier.CARDINALITY && nullConvention != NullConvention.NEVER_NULL) {
            throw new IllegalArgumentException("cardinality contributions cannot be nullable");
        }
    }

    public static PrimitiveRangeContribution longValues(NullConvention nullConvention)
    {
        return new PrimitiveRangeContribution(Carrier.LONG, nullConvention);
    }

    public static PrimitiveRangeContribution cardinality()
    {
        return new PrimitiveRangeContribution(Carrier.CARDINALITY, NullConvention.NEVER_NULL);
    }

    public enum Carrier
    {
        LONG,
        CARDINALITY
    }

    public enum NullConvention
    {
        NEVER_NULL,
        NULLABLE
    }
}
