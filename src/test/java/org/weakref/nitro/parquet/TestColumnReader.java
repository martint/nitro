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
import org.weakref.nitro.data.AllocationResources;
import org.weakref.nitro.data.Allocator;

import static org.assertj.core.api.Assertions.assertThat;

final class TestColumnReader
{
    @Test
    void testDictionaryDomainPresenceCompletesInProbe()
    {
        try (AllocationResources resources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            assertThat(ColumnReader.dictionaryDomainPresence(allocator, new int[] {2, 0, 1, 2}, 4, 3, 1))
                    .isEqualTo(0b111);
        }
    }

    @Test
    void testDictionaryDomainPresenceFallsBackForAbsentEntry()
    {
        try (AllocationResources resources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            assertThat(ColumnReader.dictionaryDomainPresence(allocator, new int[] {0, 2, 0, 2, 0, 2}, 6, 3, 1))
                    .isEqualTo(0b101);
        }
    }

    @Test
    void testDictionaryDomainPresenceFallsBackForLateEntry()
    {
        try (AllocationResources resources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            assertThat(ColumnReader.dictionaryDomainPresence(allocator, new int[] {0, 0, 0, 0, 1, 2}, 6, 3, 1))
                    .isEqualTo(0b111);
        }
    }

    @Test
    void testDictionaryDomainPresenceSupportsSixtyFourEntries()
    {
        int[] ids = new int[64];
        for (int entry = 0; entry < ids.length; entry++) {
            ids[entry] = entry;
        }
        try (AllocationResources resources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            assertThat(ColumnReader.dictionaryDomainPresence(allocator, ids, ids.length, 64, 0))
                    .isEqualTo(-1L);
        }
    }
}
