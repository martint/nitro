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
import org.weakref.nitro.core.execution.ExecutionSuspension;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.execution.EngineResources;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestEncodedRowBuffer
{
    @Test
    void testRetainsEncodedSparseBatchesAcrossSourceAdvance()
    {
        DictionaryVector first = new DictionaryVector(new int[] {0, 1, 0, 1}, new I64Vector(new long[] {11, 12}));
        DictionaryVector second = new DictionaryVector(new int[] {1, 0, 1}, new I64Vector(new long[] {21, 22}));
        TableOperator source = TableOperator.retained(
                Schema.unspecified(1),
                List.of(
                        TableOperator.Page.values(4, new DictionaryVector[] {first}, Mask.sparse(new int[] {1, 2}, 4)),
                        TableOperator.Page.values(3, new DictionaryVector[] {second}, Mask.all(3))));

        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                EncodedRowBuffer rows = new EncodedRowBuffer(allocator, new Allocator.Context("test"), 1)) {
            rows.load(source);

            assertThat(rows.size()).isEqualTo(5);
            assertThat(rows.longValue(0, 0)).isEqualTo(12);
            assertThat(rows.longValue(0, 1)).isEqualTo(11);
            assertThat(rows.longValue(0, 2)).isEqualTo(22);
            assertThat(rows.longValue(0, 4)).isEqualTo(22);
            assertThat(rows.column(0, 0).values()).isSameAs(first);
            assertThat(rows.sourceSize(0)).isEqualTo(4);
            assertThat(rows.sourceSize(2)).isEqualTo(3);
            assertThat(rows.sharesSource(0, 1)).isTrue();
            assertThat(rows.sharesSource(1, 2)).isFalse();
        }
    }

    @Test
    void testCopiesInputWhoseBuffersAreReusedOnAdvance()
    {
        ReusingSource source = new ReusingSource();
        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                EncodedRowBuffer rows = new EncodedRowBuffer(allocator, new Allocator.Context("test"), 1)) {
            rows.load(source);

            assertThat(rows.size()).isEqualTo(4);
            assertThat(rows.longValue(0, 0)).isEqualTo(11);
            assertThat(rows.longValue(0, 1)).isEqualTo(13);
            assertThat(rows.longValue(0, 2)).isEqualTo(21);
            assertThat(rows.longValue(0, 3)).isEqualTo(23);
            assertThat(rows.sourceSize(0)).isEqualTo(2);
            assertThat(rows.sourceSize(2)).isEqualTo(2);
            assertThat(rows.sharesSource(0, 1)).isTrue();
            assertThat(rows.sharesSource(1, 2)).isFalse();
        }
    }

    @Test
    void testResumesLoadingAfterExecutionSuspension()
    {
        Operator delegate = new TableOperator(
                1,
                List.of(
                        TableOperator.Page.values(1, new I64Vector[] {new I64Vector(new long[] {11})}, Mask.all(1)),
                        TableOperator.Page.values(1, new I64Vector[] {new I64Vector(new long[] {22})}, Mask.all(1))));
        Operator source = new Operator()
        {
            private int hasNextCalls;

            @Override
            public int outputCount()
            {
                return delegate.outputCount();
            }

            @Override
            public Schema outputSchema()
            {
                return delegate.outputSchema();
            }

            @Override
            public boolean hasNext()
            {
                if (++hasNextCalls == 2) {
                    throw ExecutionSuspension.yield();
                }
                return delegate.hasNext();
            }

            @Override
            public Batch next()
            {
                return delegate.next();
            }

            @Override
            public void constrain(Mask mask)
            {
                delegate.constrain(mask);
            }

            @Override
            public void close()
            {
                delegate.close();
            }
        };

        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                EncodedRowBuffer rows = new EncodedRowBuffer(allocator, new Allocator.Context("test"), 1)) {
            assertThatThrownBy(() -> rows.load(source)).isSameAs(ExecutionSuspension.yield());

            rows.load(source);
            assertThat(rows.size()).isEqualTo(2);
            assertThat(rows.longValue(0, 0)).isEqualTo(11);
            assertThat(rows.longValue(0, 1)).isEqualTo(22);
        }
    }

    private static final class ReusingSource
            implements Operator
    {
        private final long[] values = new long[3];
        private int batch;

        @Override
        public int outputCount()
        {
            return 1;
        }

        @Override
        public boolean hasNext()
        {
            return batch < 2;
        }

        @Override
        public Batch next()
        {
            batch++;
            values[0] = batch * 10L + 1;
            values[1] = batch * 10L + 2;
            values[2] = batch * 10L + 3;
            return new Batch(
                    Mask.sparse(new int[] {0, 2}, 3),
                    Output.of(Streams.ofValues(new I64Vector(values))));
        }

        @Override
        public void constrain(Mask mask) {}

        @Override
        public void close() {}
    }
}
