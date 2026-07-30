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
package org.weakref.nitro.operator.aggregation;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.core.function.aggregation.AggregationExecution;
import org.weakref.nitro.core.function.aggregation.AggregationInput;
import org.weakref.nitro.core.function.aggregation.MultiAggregationImplementation;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.operator.aggregation.RegisteredMultiAggregationUnit.OutputMode.FINAL;
import static org.weakref.nitro.operator.aggregation.RegisteredMultiAggregationUnit.OutputMode.INTERMEDIATE;

class TestRegisteredMultiAggregationUnit
{
    @Test
    void testSharedInputAndOutputRoutingArePlanOwned()
    {
        TrackingImplementation implementation = new TrackingImplementation();
        I64Vector first = new I64Vector(new long[] {11});
        I64Vector second = new I64Vector(new long[] {22});
        StreamAccessor inputs = (column, stream) -> switch (column) {
            case 2 -> first;
            case 7 -> second;
            default -> throw new AssertionError("unexpected physical column: " + column);
        };
        RegisteredMultiAggregationUnit unit = new RegisteredMultiAggregationUnit(
                implementation,
                List.of(INTERMEDIATE, FINAL),
                new int[] {7, 2},
                4);
        assertThat(unit.filterInputColumn()).isEqualTo(4);
        Object state = new Object();

        unit.accumulate(state, 3, Mask.all(1), inputs);

        assertThat(implementation.firstInput).isSameAs(second);
        assertThat(implementation.secondInput).isSameAs(first);
        assertThat(unit.outputCount()).isEqualTo(2);
        assertThat(unit.result(0, 0, state, Streams.empty(), null, null))
                .isSameAs(implementation.intermediate);
        assertThat(unit.result(1, 0, state, Streams.empty(), null, null))
                .isSameAs(implementation.result);
    }

    private static final class TrackingImplementation
            implements MultiAggregationImplementation
    {
        private final Streams intermediate = Streams.ofValues(new I64Vector(1));
        private final Streams result = Streams.ofValues(new I64Vector(1));
        private Vector firstInput;
        private Vector secondInput;

        @Override
        public int outputCount()
        {
            return 2;
        }

        @Override
        public Object allocate(AggregationExecution execution, int groups)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object grow(Allocator allocator, Allocator.Context allocationContext, Object state, int groups)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void initialize(Object state, int offset, int length)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addRawInput(Object state, int group, Mask mask, AggregationInput input)
        {
            firstInput = input.stream(0, Stream.VALUES);
            secondInput = input.stream(1, Stream.VALUES);
        }

        @Override
        public void addRawInput(Object state, Vector groups, Mask mask, AggregationInput input)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Streams intermediate(int output, int maxGroup, Object state, Streams existing, Allocator allocator, Allocator.Context allocationContext)
        {
            return intermediate;
        }

        @Override
        public Streams result(int output, int maxGroup, Object state, Streams existing, Allocator allocator, Allocator.Context allocationContext)
        {
            return result;
        }
    }
}
