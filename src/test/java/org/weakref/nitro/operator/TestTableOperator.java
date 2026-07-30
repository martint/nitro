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
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

final class TestTableOperator
{
    @Test
    void testRetentionCapabilityIsExplicit()
    {
        List<TableOperator.Page> pages = List.of(
                TableOperator.Page.values(2, new I64Vector[] {new I64Vector(new long[] {11, 12})}, Mask.all(2)),
                TableOperator.Page.values(1, new I64Vector[] {new I64Vector(new long[] {13})}, Mask.all(1)));

        assertThat(new TableOperator(1, pages).supportsRetainedBatches()).isFalse();

        TableOperator table = TableOperator.retained(Schema.unspecified(1), pages);
        assertThat(table.supportsRetainedBatches()).isTrue();

        Batch first = table.next();
        table.next().close();
        assertThat(((I64Vector) first.output(0).borrow(Stream.VALUES)).values())
                .containsExactly(11, 12);
        first.close();
    }
}
