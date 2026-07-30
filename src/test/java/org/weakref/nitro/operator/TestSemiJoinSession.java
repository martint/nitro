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
package org.weakref.nitro.operator;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.execution.EngineResources;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

class TestSemiJoinSession
{
    @Test
    void testPreservesSqlInMembershipAcrossIndependentlyScheduledProbeBatches()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                SemiJoinSession session = new SemiJoinSession(
                        resources.operatorResources(),
                        allocator,
                        Schema.unspecified(1),
                        0,
                        nullableTable(2L, null),
                        0,
                        true,
                        new Field(Schema.unspecified(1).field(0).type(), true),
                        SemiJoinOperator.MatchOutputSemantics.SQL_IN)) {
            allocator.beginExecution();

            List<Boolean> values = new ArrayList<>();
            List<Boolean> nulls = new ArrayList<>();
            session.addInput(nullableBatch(1L, 2L));
            drain(session, values, nulls);
            session.addInput(nullableBatch(null, 3L));
            drain(session, values, nulls);
            session.finish();

            assertThat(values).containsExactly(false, true, false, false);
            assertThat(nulls).containsExactly(true, false, true, true);
            assertThat(session.isFinished()).isTrue();
            try (Batch late = nullableBatch(4L)) {
                assertThatIllegalStateException()
                        .isThrownBy(() -> session.addInput(late))
                        .withMessage("semi-join session is finishing");
            }
        }
    }

    private static void drain(SemiJoinSession session, List<Boolean> values, List<Boolean> nulls)
    {
        while (session.hasOutput()) {
            try (Batch output = session.getOutput()) {
                boolean[] outputValues = ((BooleanVector) output.output(1).borrow(Stream.VALUES)).values();
                boolean[] outputNulls = ((BooleanVector) output.output(1).borrow(Stream.NULLS)).values();
                for (int position : output.borrowMask()) {
                    values.add(outputValues[position]);
                    nulls.add(outputNulls[position]);
                }
            }
        }
    }

    private static Batch nullableBatch(Long... values)
    {
        long[] data = new long[values.length];
        boolean[] nulls = new boolean[values.length];
        for (int position = 0; position < values.length; position++) {
            if (values[position] == null) {
                nulls[position] = true;
            }
            else {
                data[position] = values[position];
            }
        }
        return new Batch(
                Mask.all(values.length),
                Output.of(Streams.ofValuesAndNulls(new I64Vector(data), new BooleanVector(nulls))));
    }

    private static Operator nullableTable(Long... values)
    {
        Batch batch = nullableBatch(values);
        Streams[] columns = {Streams.of(
                batch.output(0).take(Stream.VALUES),
                batch.output(0).take(Stream.NULLS),
                null)};
        batch.close();
        return new TableOperator(
                Schema.unspecified(1),
                List.of(new TableOperator.Page(values.length, columns, Mask.all(values.length))));
    }
}
