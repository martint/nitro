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

final class VectorSupport
{
    private VectorSupport() {}

    public static int totalLength(Vector[] rows)
    {
        int totalLength = 0;
        for (Vector row : rows) {
            totalLength += row.length();
        }
        return totalLength;
    }

    public static int[] densePositions(int length)
    {
        int[] positions = new int[length];
        for (int index = 0; index < length; index++) {
            positions[index] = index;
        }
        return positions;
    }

    public static int[] selectedPositions(Mask mask)
    {
        int[] positions = new int[mask.selectedCount()];
        int outputIndex = 0;
        for (int position : mask) {
            positions[outputIndex++] = position;
        }
        return positions;
    }
}
