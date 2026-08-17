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
package org.weakref.nitro.parquet;

import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.assertj.core.api.Assertions.assertThat;

class TestParquetDecodeScratchPool
{
    @Test
    void testScratchIsCrossThreadAndReused()
            throws InterruptedException
    {
        try (ParquetDecodeScratchPool pool = new ParquetDecodeScratchPool(
                new ParquetDecodeScratchPolicy(1 << 20, 1 << 20))) {
            long address;
            try (ParquetDecodeScratchPool.Lease lease = pool.borrow(100)) {
                MemorySegment segment = lease.segment();
                address = segment.address();
                Thread writer = Thread.ofPlatform().start(() -> segment.set(ValueLayout.JAVA_BYTE, 0, (byte) 37));
                writer.join();
                assertThat(segment.get(ValueLayout.JAVA_BYTE, 0)).isEqualTo((byte) 37);
            }

            try (ParquetDecodeScratchPool.Lease lease = pool.borrow(100)) {
                assertThat(lease.segment().address()).isEqualTo(address);
            }
        }
    }

    @Test
    void testClosedLeaseRejectsAccess()
    {
        try (ParquetDecodeScratchPool pool = new ParquetDecodeScratchPool(
                new ParquetDecodeScratchPolicy(1 << 20, 1 << 20))) {
            ParquetDecodeScratchPool.Lease lease = pool.borrow(1);
            lease.close();
            org.assertj.core.api.Assertions.assertThatThrownBy(lease::segment)
                    .isInstanceOf(IllegalStateException.class);
        }
    }
}
