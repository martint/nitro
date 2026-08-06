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
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.execution.EngineResources;

import static org.assertj.core.api.Assertions.assertThat;

class TestGenericJoinIndexFactory
{
    @Test
    void testDirectRangeBuildAdmissionUsesBoundedAbsoluteDomain()
    {
        try (EngineResources resources = EngineResources.createDefault()) {
            GenericJoinIndexFactory indexes = resources.operatorResources().genericJoinIndexes();

            assertThat(indexes.shouldUseDirectRangeBuild(batch(sequence(0, 2, 4096)), vectors(sequence(0, 2, 4096)), 4096))
                    .isTrue();
            assertThat(indexes.shouldUseDirectRangeBuild(batch(sequence(1_000_000, 1, 4096)), vectors(sequence(1_000_000, 1, 4096)), 4096))
                    .isFalse();
            assertThat(indexes.shouldUseDirectRangeBuild(batch(sequence(0, 1, 255)), vectors(sequence(0, 1, 255)), 255))
                    .isFalse();
        }
    }

    private static BufferedJoinInput.InnerBatch batch(long[] values)
    {
        return new BufferedJoinInput.InnerBatch(new Streams[] {Streams.ofValues(new I64Vector(values))}, values.length);
    }

    private static Vector[] vectors(long[] values)
    {
        return new Vector[] {new I64Vector(values)};
    }

    private static long[] sequence(long start, long stride, int size)
    {
        long[] values = new long[size];
        for (int index = 0; index < size; index++) {
            values[index] = start + stride * index;
        }
        return values;
    }
}
