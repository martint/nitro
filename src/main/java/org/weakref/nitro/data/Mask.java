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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

public class Mask
        implements Iterable<Integer>
{
    private final int[] positions;
    private final int count;
    private final boolean all;

    public static Mask all(int count)
    {
        int[] positions = new int[count];
        for (int i = 0; i < count; i++) {
            positions[i] = i;
        }

        return new Mask(positions, count, true);
    }

    public static Mask range(int start, int length)
    {
        int[] positions = new int[length];
        for (int i = 0; i < length; i++) {
            positions[i] = start + i;
        }

        return new Mask(positions, length, start == 0);
    }

    private Mask(int[] positions, int count, boolean all)
    {
        this.positions = positions;
        this.count = count;
        this.all = all;
    }

    public static Mask sparse(int[] activePositions, int totalPositions)
    {
        return new Mask(Arrays.copyOf(activePositions, totalPositions), activePositions.length, totalPositions == activePositions.length);
    }

    public boolean all()
    {
        return all;
    }

    public int maxPosition()
    {
        return positions[count - 1];
    }

    public int position(int index)
    {
        // TODO verify index < count
        return positions[index];
    }

    public int count()
    {
        return count;
    }

    public boolean none()
    {
        return count == 0;
    }

    public boolean anyTrue(int start, int end)
    {
        if (all) {
            return true;
        }

        for (int i = 0; i < count; i++) {
            int position = positions[i];
            if (position >= start && position <= end) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString()
    {
        return Arrays.toString(Arrays.copyOf(positions, count));
    }

    public Mask first(int n)
    {
        if (n >= count) {
            return this;
        }

        return new Mask(Arrays.copyOf(positions, n), n, false);
    }

    public Mask last(int n)
    {
        if (n >= count) {
            return this;
        }

        int[] positions = new int[this.positions.length];
        System.arraycopy(this.positions, count - n, positions, 0, n);
        return new Mask(positions, n, false);
    }

    @Override
    public Iterator<Integer> iterator()
    {
        return new Iterator<>()
        {
            private int index;

            @Override
            public boolean hasNext()
            {
                return index < count;
            }

            @Override
            public Integer next()
            {
                return positions[index++];
            }
        };
    }

    /**
     * Returns a new mask with positions that are in this mask but not in the other mask.
     *
     * @param other the mask to subtract from this mask
     * @return a new mask containing positions in this mask that are not in the other mask
     */
    public Mask difference(Mask other)
    {
        if (other.none()) {
            return this;
        }

        if (this.none()) {
            return this;
        }

        // Use a set for O(1) lookup
        Set<Integer> otherPositions = new HashSet<>(other.count);
        for (int i = 0; i < other.count; i++) {
            otherPositions.add(other.positions[i]);
        }

        // Count positions in this mask but not in other
        int resultCount = 0;
        for (int i = 0; i < this.count; i++) {
            if (!otherPositions.contains(this.positions[i])) {
                resultCount++;
            }
        }

        if (resultCount == 0) {
            // All positions are in other mask
            return new Mask(new int[positions.length], 0, false);
        }

        if (resultCount == this.count) {
            // No positions in other mask
            return this;
        }

        // Create the result mask
        int[] resultPositions = new int[positions.length];
        int resultIndex = 0;
        for (int i = 0; i < this.count; i++) {
            if (!otherPositions.contains(this.positions[i])) {
                resultPositions[resultIndex++] = this.positions[i];
            }
        }

        return new Mask(resultPositions, resultCount, false);
    }

    /**
     * Returns a new mask that is the union of this mask and another mask.
     *
     * @param other the mask to union with this mask
     * @return a new mask containing all positions from both masks (without duplicates)
     */
    public Mask union(Mask other)
    {
        if (other.none()) {
            return this;
        }

        if (this.none()) {
            return other;
        }

        // Use a set to collect unique positions
        Set<Integer> allPositions = new HashSet<>(this.count + other.count);

        for (int i = 0; i < this.count; i++) {
            allPositions.add(this.positions[i]);
        }

        for (int i = 0; i < other.count; i++) {
            allPositions.add(other.positions[i]);
        }

        // Convert back to an array
        int[] resultPositions = new int[positions.length];
        int index = 0;
        for (int position : allPositions) {
            resultPositions[index++] = position;
        }

        // Sort the positions to maintain order
        Arrays.sort(resultPositions);

        return new Mask(resultPositions, resultPositions.length, false);
    }

    /**
     * Checks if this mask contains the specified position.
     *
     * @param position the position to check
     * @return true if the mask contains the position, false otherwise
     */
    public boolean contains(int position)
    {
        if (all && position < positions.length) {
            return true;
        }

        return Arrays.binarySearch(positions, 0, count, position) >= 0;
    }

    /**
     * Checks if all positions in the other mask are contained in this mask.
     *
     * @param other the mask to check
     * @return true if this mask contains all positions from the other mask, false otherwise
     */
    public boolean containsAll(Mask other)
    {
        if (other.none()) {
            return true;
        }

        if (this.none()) {
            return other.none();
        }

        for (int i = 0; i < other.count; i++) {
            if (!this.contains(other.positions[i])) {
                return false;
            }
        }

        return true;
    }

    public Mask and(BooleanVector other)
    {
        if (other == null || other.length() == 0) {
            return new Mask(new int[positions.length], 0, false);
        }

        int[] resultPositions = new int[positions.length];
        int resultCount = 0;

        for (int i = 0; i < count; i++) {
            int position = positions[i];
            if (other.values()[position]) {
                resultPositions[resultCount++] = position;
            }
        }

        return new Mask(resultPositions, resultCount, resultCount == positions.length);
    }

    public Mask andNot(Mask other)
    {
        checkArgument(positions.length == other.positions.length, "Masks must have the same length");

        // TODO: optimize
        return andNot(other.toVector());
    }

    public Mask andNot(BooleanVector other)
    {
        if (other.length() == 0) {
            return this;
        }

        int[] resultPositions = new int[positions.length];
        int resultCount = 0;

        for (int i = 0; i < count; i++) {
            int position = positions[i];
            if (!other.values()[position]) {
                resultPositions[resultCount++] = position;
            }
        }

        return new Mask(resultPositions, resultCount, resultCount == positions.length);
    }

    public Mask complement()
    {
        if (all) {
            return new Mask(new int[positions.length], 0, false);
        }

        int[] result = new int[positions.length];
        int outputIndex = 0;
        int position = 0;

        // add positions not in this.positions up to count
        for (int i = 0; i < count; i++) {
            while (position < positions[i]) {
                result[outputIndex++] = position++;
            }
            position++;
        }

        for (int i = position; i < positions.length; i++) {
            result[outputIndex++] = i;
        }

        return new Mask(result, outputIndex, false);
    }

    private BooleanVector toVector()
    {
        BooleanVector booleanVector = new BooleanVector(positions.length);
        for (int i = 0; i < count; i++) {
            booleanVector.values()[positions[i]] = true;
        }
        return booleanVector;
    }

    public Mask or(Mask other)
    {
        checkArgument(positions.length == other.positions.length, "Masks must have the same length");

        if (other.none()) {
            return this;
        }

        if (this.none()) {
            return other;
        }

        int[] result = new int[positions.length];

        int i = 0;
        int j = 0;
        int output = 0;

        while (i < count && j < other.count) {
            if (positions[i] < other.positions[j]) {
                result[output] = positions[i];
                i++;
            }
            else if (positions[i] > other.positions[j]) {
                result[output] = other.positions[j];
                j++;
            }
            else {
                result[output] = positions[i];
                i++;
                j++;
            }
            output++;
        }

        while (i < count) {
            result[output++] = positions[i++];
        }

        while (j < other.count) {
            result[output++] = other.positions[j++];
        }

        return new Mask(result, output, output == positions.length);
    }
}
