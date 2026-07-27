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

/**
 * Representation-neutral stored-row and distinct-key cardinality for a long-join build.
 */
final class LongJoinBuildCardinality
{
    private int storedRowCount;
    private int distinctKeyCount;

    int storedRowCount()
    {
        return storedRowCount;
    }

    void recordStoredRow()
    {
        storedRowCount++;
    }

    int distinctKeyCount()
    {
        return distinctKeyCount;
    }

    void recordDistinctKey()
    {
        distinctKeyCount++;
    }

    void resetDistinctKeyCount()
    {
        distinctKeyCount = 0;
    }

    boolean isEmpty()
    {
        return distinctKeyCount == 0;
    }
}
