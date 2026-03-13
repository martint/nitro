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
package org.weakref.nitro;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.I64VectorWithNulls;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.ConstantTableOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Output;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.weakref.nitro.data.Row.row;

public class TestBatchRuntime
{
    @Test
    void testSimpleOutputAllowsRepeatedBorrowButInvalidatesAfterTake()
    {
        I64Vector values = new I64Vector(new long[] {11, 12, 13});
        BooleanVector nulls = new BooleanVector(new boolean[] {false, true, false});
        Output output = Output.of(Streams.of(Stream.VALUES, values).with(Stream.NULLS, nulls));

        assertThat(output.borrow(Stream.VALUES)).isSameAs(values);
        assertThat(output.borrow(Stream.VALUES)).isSameAs(values);
        assertThat(output.take(Stream.NULLS)).isSameAs(nulls);
        assertThat(output.borrow(Stream.VALUES)).isSameAs(values);

        assertThatThrownBy(() -> output.borrow(Stream.NULLS))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already taken");
    }

    @Test
    void testSimpleBatchInvalidatesMaskAfterTake()
    {
        Mask mask = Mask.range(5, 3);
        Batch batch = new Batch(mask, Output.values(new I64Vector(8)));

        assertThat(batch.borrowMask()).isSameAs(mask);
        assertThat(batch.takeMask()).isSameAs(mask);

        assertThatThrownBy(batch::borrowMask)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already taken");
    }

    @Test
    void testOperatorOutputsRespectBorrowAndTakeSemantics()
    {
        Allocator allocator = new Allocator();
        Operator operator = new ConstantTableOperator(
                allocator,
                2,
                List.of(
                        row(1L, 10L),
                        row(2L, 20L),
                        row(3L, 30L)));

        assertThat(operator.hasNext()).isTrue();

        Batch batch = operator.nextBatch();
        Output firstOutput = batch.output(0);
        Output secondOutput = batch.output(1);

        assertThat(((I64VectorWithNulls) firstOutput.borrow(Stream.VALUES)).values()).containsExactly(1L, 2L, 3L);
        assertThat(((I64VectorWithNulls) firstOutput.borrow(Stream.VALUES)).values()).containsExactly(1L, 2L, 3L);
        assertThat(((I64VectorWithNulls) secondOutput.take(Stream.VALUES)).values()).containsExactly(10L, 20L, 30L);

        assertThatThrownBy(() -> secondOutput.borrow(Stream.VALUES))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already taken");

        assertThatThrownBy(() -> firstOutput.borrow(Stream.NULLS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not expose stream");

        operator.close();
    }
}
