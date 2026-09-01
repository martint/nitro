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

    public static Vector[] normalizeRows(
            Allocator allocator,
            Allocator.Context allocationContext,
            Vector[] rows,
            Class<? extends Vector> expectedType)
    {
        Vector[] normalized = new Vector[rows.length];
        try {
            for (int index = 0; index < rows.length; index++) {
                Vector row = rows[index];
                if (expectedType.isInstance(row)) {
                    normalized[index] = row;
                    continue;
                }
                Vector copy = row.copy(allocator, allocationContext, densePositions(row.length()));
                normalized[index] = copy;
                if (!expectedType.isInstance(copy)) {
                    releaseNormalizedRows(allocator, allocationContext, rows, normalized);
                    return null;
                }
            }
            return normalized;
        }
        catch (RuntimeException | Error failure) {
            releaseNormalizedRows(allocator, allocationContext, rows, normalized);
            throw failure;
        }
    }

    public static void releaseNormalizedRows(
            Allocator allocator,
            Allocator.Context allocationContext,
            Vector[] rows,
            Vector[] normalized)
    {
        for (int index = 0; index < normalized.length; index++) {
            Vector vector = normalized[index];
            if (vector != null && vector != rows[index]) {
                allocator.release(allocationContext, vector);
            }
        }
    }
}
