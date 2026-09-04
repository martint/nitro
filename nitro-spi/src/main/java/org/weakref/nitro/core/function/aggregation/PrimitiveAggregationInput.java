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

/** Provider binding for one primitive raw-input contribution. Input {@code -1} selects range cardinality. */
public record PrimitiveAggregationInput(int input, PrimitiveRangeContribution contribution, PrimitiveRangeConsumer consumer)
{
    public PrimitiveAggregationInput
    {
        contribution = requireNonNull(contribution, "contribution is null");
        consumer = requireNonNull(consumer, "consumer is null");
        if (input < -1) {
            throw new IllegalArgumentException("input is less than -1");
        }
        if ((input == -1) != (contribution.carrier() == PrimitiveRangeContribution.Carrier.CARDINALITY)) {
            throw new IllegalArgumentException("only cardinality contributions may omit an input");
        }
    }
}
