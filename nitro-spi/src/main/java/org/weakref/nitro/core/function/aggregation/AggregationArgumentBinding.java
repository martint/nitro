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
 * Plan-time binding from a provider-visible argument shape to a physical input column.
 */
public record AggregationArgumentBinding(AggregationArgument argument, int inputColumn)
{
    public AggregationArgumentBinding
    {
        argument = requireNonNull(argument, "argument is null");
        if ((argument.kind() == AggregationArgument.Kind.INPUT) != (inputColumn >= 0)) {
            throw new IllegalArgumentException("only direct input arguments have an input column");
        }
    }

    public static AggregationArgumentBinding input(int inputColumn)
    {
        return new AggregationArgumentBinding(AggregationArgument.input(), inputColumn);
    }

    public static AggregationArgumentBinding computed()
    {
        return new AggregationArgumentBinding(AggregationArgument.computed(), -1);
    }

    public static AggregationArgumentBinding literal(Object value)
    {
        return new AggregationArgumentBinding(AggregationArgument.literal(value), -1);
    }
}
