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
import org.weakref.nitro.core.function.aggregation.AggregationImplementation;
import org.weakref.nitro.core.function.aggregation.AggregationInput;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.operator.aggregation.RegisteredAggregationUnit.InputMode.RAW;
import static org.weakref.nitro.operator.aggregation.RegisteredAggregationUnit.OutputMode.FINAL;

class TestRegisteredAggregationUnit
{
    @Test
    void testPhysicalModesAndInputMappingArePlanOwned()
    {
        TrackingImplementation implementation = new TrackingImplementation();
        I64Vector first = new I64Vector(new long[] {11});
        I64Vector second = new I64Vector(new long[] {22});
        StreamAccessor inputs = (column, stream) -> switch (column) {
            case 2 -> first;
            case 7 -> second;
            default -> throw new AssertionError("unexpected physical column: " + column);
        };
        Mask mask = Mask.all(1);
        Object state = new Object();

        RegisteredAggregationUnit rawIntermediate = new RegisteredAggregationUnit(
                implementation,
                RAW,
                RegisteredAggregationUnit.OutputMode.INTERMEDIATE,
                new int[] {7, 2});
        rawIntermediate.accumulate(state, 3, mask, inputs);
        assertThat(implementation.raw).isTrue();
        assertThat(implementation.firstInput).isSameAs(second);
        assertThat(implementation.secondInput).isSameAs(first);
        assertThat(rawIntermediate.result(0, 0, state, Streams.empty(), null, null))
                .isSameAs(implementation.intermediate);

        implementation.reset();
        RegisteredAggregationUnit intermediateFinal = new RegisteredAggregationUnit(
                implementation,
                RegisteredAggregationUnit.InputMode.INTERMEDIATE,
                FINAL,
                new int[] {2});
        intermediateFinal.accumulate(state, new I64Vector(new long[] {0}), mask, inputs);
        assertThat(implementation.intermediateInput).isTrue();
        assertThat(implementation.firstInput).isSameAs(first);
        assertThat(intermediateFinal.result(0, 0, state, mask, Streams.empty(), null, null))
                .isSameAs(implementation.result);
    }

    private static final class TrackingImplementation
            implements AggregationImplementation
    {
        private final Streams intermediate = Streams.ofValues(new I64Vector(1));
        private final Streams result = Streams.ofValues(new I64Vector(1));
        private boolean raw;
        private boolean intermediateInput;
        private Vector firstInput;
        private Vector secondInput;

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
            raw = true;
            firstInput = input.stream(0, Stream.VALUES);
            secondInput = input.stream(1, Stream.VALUES);
        }

        @Override
        public void addRawInput(Object state, Vector groups, Mask mask, AggregationInput input)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addIntermediate(Object state, int group, Mask mask, AggregationInput input)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addIntermediate(Object state, Vector groups, Mask mask, AggregationInput input)
        {
            intermediateInput = true;
            firstInput = input.stream(0, Stream.VALUES);
        }

        @Override
        public Streams intermediate(
                int maxGroup,
                Object state,
                Streams existing,
                Allocator allocator,
                Allocator.Context allocationContext)
        {
            return intermediate;
        }

        @Override
        public Streams result(
                int maxGroup,
                Object state,
                Streams existing,
                Allocator allocator,
                Allocator.Context allocationContext)
        {
            return result;
        }

        private void reset()
        {
            raw = false;
            intermediateInput = false;
            firstInput = null;
            secondInput = null;
        }
    }
}
