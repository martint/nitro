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

/**
 * Provider-visible shape of one aggregate argument.
 *
 * <p>Input column ordinals are deliberately absent. A provider describes behavior in terms of its
 * own argument ordinals; the physical resolver binds direct inputs afterward.
 */
public record AggregationArgument(Kind kind, Object literal)
{
    public AggregationArgument
    {
        kind = requireNonNull(kind, "kind is null");
        if (kind != Kind.LITERAL && literal != null) {
            throw new IllegalArgumentException("only literal arguments may carry a literal value");
        }
    }

    public static AggregationArgument input()
    {
        return new AggregationArgument(Kind.INPUT, null);
    }

    public static AggregationArgument computed()
    {
        return new AggregationArgument(Kind.COMPUTED, null);
    }

    public static AggregationArgument literal(Object value)
    {
        return new AggregationArgument(Kind.LITERAL, value);
    }

    public enum Kind
    {
        INPUT,
        COMPUTED,
        LITERAL
    }
}
