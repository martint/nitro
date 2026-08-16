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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestStructVector
{
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
}
