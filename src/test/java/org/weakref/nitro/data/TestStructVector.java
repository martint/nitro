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
package org.weakref.nitro.data;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.execution.EngineResources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestStructVector
{
    @Test
    void testSinglePositionReplacementPreservesOtherFields()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("test");
        StructVector existing = new StructVector(3);
        existing.setField("value", Streams.ofValues(new I64Vector(new long[] {1, 2, 3})));
        StructVector source = new StructVector(1);
        source.setField("value", Streams.ofValues(new I64Vector(new long[] {5})));

        StructVector result = (StructVector) source.copySinglePositionInto(allocator, context, existing, 0, 0, 3);

        assertThat(result).isSameAs(existing);
        assertThat(((I64Vector) result.fieldValues("value")).values()).containsExactly(5, 2, 3);
        allocator.release(context);
    }

    @Test
    void testFieldViewsPreserveSemanticOrder()
    {
        StructVector vector = new StructVector(1);
        vector.setField("sum", Streams.ofValues(new F64Vector(new double[] {7})));
        vector.setField("count", Streams.ofValues(new I64Vector(new long[] {2})));
        vector.setField("m2", Streams.ofValues(new F64Vector(new double[] {3})));

        assertThat(vector.fields().keySet()).containsExactly("sum", "count", "m2");
        assertThat(vector.fieldNames()).containsExactly("sum", "count", "m2");
        assertThat(vector.field(0)).isSameAs(vector.field("sum"));
        assertThat(vector.field(1)).isSameAs(vector.field("count"));
        assertThat(vector.field(2)).isSameAs(vector.field("m2"));
        assertThatThrownBy(() -> vector.fields().remove("sum"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testMaterializesMixedEncodedSegmentsInOnePass()
    {
        StructVector first = new StructVector(2);
        first.setField("value", Streams.ofValuesAndNulls(
                new I64Vector(new long[] {1, 2}),
                new BooleanVector(2)));
        StructVector second = new StructVector(1);
        second.setField("value", Streams.ofValuesAndNulls(
                new I64Vector(new long[] {3}),
                new BooleanVector(1)));

        EngineResources resources = EngineResources.createDefault();
        Allocator allocator = new Allocator(resources);
        Allocator.Context context = new Allocator.Context("mixed-struct-materialize");
        try {
            Vector encodedFirst = DictionaryVector.wrap(new int[] {1, 0}, first);
            StructVector result = (StructVector) encodedFirst.materializeRows(
                    allocator,
                    context,
                    new Vector[] {encodedFirst, second});
            assertThat(((I64Vector) result.fieldValues("value")).values()).containsExactly(2, 1, 3);
            assertThat(((BooleanVector) result.field("value").get(Stream.NULLS)).values())
                    .containsExactly(false, false, false);
        }
        finally {
            allocator.release(context);
            resources.close();
        }
    }
}
