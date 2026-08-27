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
package org.weakref.nitro.function.scalar.builtin;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class TestLengthUtf8
{
    @Test
    void testEvaluatesNestedDictionaryDomainOnce()
    {
        BinaryVector values = new BinaryVector(2, 6);
        values.setBytes(0, "a".getBytes(StandardCharsets.UTF_8));
        values.setBytes(1, "nitro".getBytes(StandardCharsets.UTF_8));
        DictionaryVector inner = DictionaryVector.wrap(new int[] {0, 1, 0}, values);
        DictionaryVector outer = DictionaryVector.wrapNested(new int[] {0, 1, 2, 0, 2, 1}, 6, inner);
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Streams result = new LengthUtf8().apply(
                    List.of(Streams.ofValues(outer)),
                    Mask.all(6),
                    Set.of(Stream.VALUES),
                    null,
                    new PrimitiveExecutionContext(allocator));

            DictionaryVector outerResult = (DictionaryVector) result.values();
            DictionaryVector innerResult = (DictionaryVector) outerResult.values();
            assertThat(((I64Vector) innerResult.values()).values()).containsExactly(1, 5);
            assertThat(outerResult.ids()).containsExactly(0, 1, 2, 0, 2, 1);
            assertThat(innerResult.ids()).containsExactly(0, 1, 0);
        }
    }
}
