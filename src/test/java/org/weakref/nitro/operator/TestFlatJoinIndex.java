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
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.execution.EngineResources;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

class TestFlatJoinIndex
{
    @Test
    void testPreparedProbeViewsDoNotShareBatchState()
            throws Exception
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                var executor = Executors.newFixedThreadPool(2)) {
            FlatKeyLayout layout = FlatKeyLayout.tryCreate(
                    new Vector[] {binaryVector("a", "b")},
                    true,
                    allocator.primitiveArrays(),
                    resources.operatorResources().codeGeneration(),
                    resources.operatorResources().flatKeyTablePolicy());
            FlatJoinIndex owner = new FlatJoinIndex(HashJoinIndexPolicy.defaults(), layout, 2);
            owner.add(new Vector[] {binaryVector("a", "b")}, new Vector[] {null}, 0, 10);
            owner.add(new Vector[] {binaryVector("a", "b")}, new Vector[] {null}, 1, 11);

            FlatJoinIndex first = owner.newProbeView();
            FlatJoinIndex second = owner.newProbeView();
            CyclicBarrier barrier = new CyclicBarrier(2);
            var firstProbe = executor.submit(() -> probeRepeatedly(first, 257, barrier));
            var secondProbe = executor.submit(() -> probeRepeatedly(second, 509, barrier));

            firstProbe.get(30, SECONDS);
            secondProbe.get(30, SECONDS);
        }
    }

    private static void probeRepeatedly(FlatJoinIndex index, int size, CyclicBarrier barrier)
    {
        String[] keys = new String[size];
        int[] positions = new int[size];
        boolean[] nullFlags = new boolean[size];
        for (int position = 0; position < size; position++) {
            keys[position] = (position & 1) == 0 ? "a" : "b";
            positions[position] = position;
        }
        nullFlags[size - 1] = true;
        Vector[] values = {binaryVector(keys)};
        Vector[] nulls = {new BooleanVector(nullFlags)};
        long[] references = new long[size];
        for (int iteration = 0; iteration < 200; iteration++) {
            try {
                barrier.await();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            index.matchSingleRows(values, nulls, true, positions, size, references);
            assertThat(references[0]).isEqualTo(10);
            assertThat(references[1]).isEqualTo(11);
            assertThat(references[size - 1]).isEqualTo(-1);
        }
    }

    private static BinaryVector binaryVector(String... values)
    {
        byte[][] bytes = java.util.Arrays.stream(values)
                .map(value -> value.getBytes(StandardCharsets.UTF_8))
                .toArray(byte[][]::new);
        int[] offsets = new int[values.length + 1];
        int totalBytes = 0;
        for (int position = 0; position < values.length; position++) {
            totalBytes += bytes[position].length;
            offsets[position + 1] = totalBytes;
        }
        byte[] data = new byte[totalBytes];
        int offset = 0;
        for (byte[] value : bytes) {
            System.arraycopy(value, 0, data, offset, value.length);
            offset += value.length;
        }
        return new BinaryVector(values.length, offsets, data);
    }
}
