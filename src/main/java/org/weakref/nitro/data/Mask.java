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
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.IntPredicate;

import static com.google.common.base.Preconditions.checkArgument;

public class Mask
        implements Iterable<Integer>
{
    private static final int[] EMPTY_POSITIONS = new int[0];

    private Mask trackedPrevious;
    private Mask trackedNext;
    private boolean trackedInUse;
    private int size;
    private int selectedCount;
    private boolean allSelected;
    private int[] positions;

    public static Mask all(int size)
    {
        checkArgument(size >= 0, "size is negative");
        return new Mask(size, size, true, EMPTY_POSITIONS);
    }

    public static Mask range(int start, int length)
    {
        checkArgument(start >= 0, "start is negative");
        checkArgument(length >= 0, "length is negative");
        if (start == 0) {
            return all(length);
        }

        int[] positions = new int[length];
        for (int index = 0; index < length; index++) {
            positions[index] = start + index;
        }
        return new Mask(start + length, length, false, positions);
    }

    public static Mask sparse(int[] activePositions, int totalPositions)
    {
        checkArgument(totalPositions >= 0, "totalPositions is negative");
        checkArgument(activePositions.length <= totalPositions, "More active positions than total positions");

        if (totalPositions == 0) {
            return all(0);
        }

        if (activePositions.length == 0) {
            return new Mask(totalPositions, 0, false, EMPTY_POSITIONS);
        }

        int[] positions = Arrays.copyOf(activePositions, activePositions.length);
        boolean allSelected = activePositions.length == totalPositions && isAllPositions(positions, activePositions.length);
        return new Mask(totalPositions, activePositions.length, allSelected, allSelected ? EMPTY_POSITIONS : positions);
    }

    private Mask(int size, int selectedCount, boolean allSelected, int[] positions)
    {
        checkArgument(size >= 0, "size is negative");
        checkArgument(selectedCount >= 0, "selectedCount is negative");
        checkArgument(selectedCount <= size, "selectedCount exceeds size");
        checkArgument(allSelected == (selectedCount == size), "allSelected must match selectedCount");
        checkArgument(allSelected || positions.length >= selectedCount, "positions capacity is too small");

        this.size = size;
        this.selectedCount = selectedCount;
        this.allSelected = allSelected;
        this.positions = allSelected ? EMPTY_POSITIONS : positions;
    }

    public int size()
    {
        return size;
    }

    public int selectedCount()
    {
        return selectedCount;
    }

    public int count()
    {
        return selectedCount;
    }

    public boolean all()
    {
        return allSelected;
    }

    public boolean none()
    {
        return selectedCount == 0;
    }

    public int maxPosition()
    {
        checkArgument(!none(), "Mask is empty");
        if (allSelected) {
            return size - 1;
        }
        return positions[selectedCount - 1];
    }

    public int position(int index)
    {
        checkArgument(index >= 0 && index < selectedCount, "index out of bounds");
        if (allSelected) {
            return index;
        }
        return positions[index];
    }

    /**
     * The backing array of selected positions for a sparse mask — its first {@link #selectedCount()} entries
     * are the active positions in order. Returns {@code null} when {@link #all()} is true (there is no
     * materialized array; callers should iterate {@code 0..size()} densely). Exposed for tight monomorphic
     * kernels that must avoid the per-call bound check of {@link #position(int)}; treat the result as read-only.
     */
    public int[] selectedPositions()
    {
        return allSelected ? null : positions;
    }

    public void selectAll(int size)
    {
        checkArgument(size >= 0, "size is negative");
        this.size = size;
        selectedCount = size;
        allSelected = true;
    }

    public void clear(int size)
    {
        checkArgument(size >= 0, "size is negative");
        this.size = size;
        selectedCount = 0;
        allSelected = false;
    }

    public void copyFrom(Mask other)
    {
        size = other.size;
        selectedCount = other.selectedCount;
        allSelected = other.allSelected;
        if (!allSelected) {
            ensureCapacity(selectedCount);
            System.arraycopy(other.positions, 0, positions, 0, selectedCount);
        }
    }

    public void intersectInPlace(Mask other)
    {
        checkCompatible(other);

        if (none() || other.all()) {
            return;
        }
        if (other.none()) {
            clear(size);
            return;
        }
        if (allSelected) {
            copyFrom(other);
            return;
        }

        int leftIndex = 0;
        int rightIndex = 0;
        int outputIndex = 0;
        while (leftIndex < selectedCount && rightIndex < other.selectedCount) {
            int leftPosition = positions[leftIndex];
            int rightPosition = other.positions[rightIndex];
            if (leftPosition < rightPosition) {
                leftIndex++;
            }
            else if (leftPosition > rightPosition) {
                rightIndex++;
            }
            else {
                positions[outputIndex++] = leftPosition;
                leftIndex++;
                rightIndex++;
            }
        }
        selectedCount = outputIndex;
        allSelected = outputIndex == size;
        if (allSelected) {
            positions = EMPTY_POSITIONS;
        }
    }

    public void differenceInPlace(Mask other)
    {
        checkCompatible(other);

        if (none() || other.none()) {
            return;
        }
        if (other.all()) {
            clear(size);
            return;
        }
        if (allSelected) {
            int complementCount = size - other.selectedCount;
            ensureCapacity(complementCount);
            int outputIndex = 0;
            int position = 0;
            for (int index = 0; index < other.selectedCount; index++) {
                while (position < other.positions[index]) {
                    positions[outputIndex++] = position++;
                }
                position++;
            }
            while (position < size) {
                positions[outputIndex++] = position++;
            }
            selectedCount = outputIndex;
            allSelected = outputIndex == size;
            return;
        }

        int leftIndex = 0;
        int rightIndex = 0;
        int outputIndex = 0;
        while (leftIndex < selectedCount && rightIndex < other.selectedCount) {
            int leftPosition = positions[leftIndex];
            int rightPosition = other.positions[rightIndex];
            if (leftPosition < rightPosition) {
                positions[outputIndex++] = leftPosition;
                leftIndex++;
            }
            else if (leftPosition > rightPosition) {
                rightIndex++;
            }
            else {
                leftIndex++;
                rightIndex++;
            }
        }
        while (leftIndex < selectedCount) {
            positions[outputIndex++] = positions[leftIndex++];
        }
        selectedCount = outputIndex;
        allSelected = outputIndex == size;
        if (allSelected) {
            positions = EMPTY_POSITIONS;
        }
    }

    public void retainIf(IntPredicate predicate)
    {
        if (none()) {
            return;
        }

        if (allSelected) {
            int[] positions = positionsArray(size);
            int outputIndex = 0;
            for (int position = 0; position < size; position++) {
                if (predicate.test(position)) {
                    positions[outputIndex++] = position;
                }
            }
            if (outputIndex == size) {
                selectAll(size);
                return;
            }
            setSelection(size, outputIndex, false);
            return;
        }

        int outputIndex = 0;
        for (int index = 0; index < selectedCount; index++) {
            int position = positions[index];
            if (predicate.test(position)) {
                positions[outputIndex++] = position;
            }
        }
        if (outputIndex == size) {
            selectAll(size);
            return;
        }
        setSelection(size, outputIndex, false);
    }

    /** Comparison applied by {@link #retainConstantComparison}, in the form {@code column OPERATOR literal}. */
    public enum ComparisonOperator
    {
        EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL
    }

    /**
     * Retains the positions whose {@code values[position]} compares {@code true} against {@code literal} under
     * {@code operator}. The inner loop is a monomorphic scan over the raw column array with a hoisted constant — no
     * per-position {@link IntPredicate} or value-accessor virtual call — so a column-vs-constant filter (the common
     * {@code AdvEngineID <> 0} shape) is evaluated at array speed instead of paying a megamorphic dispatch on every
     * one of millions of rows. The caller is responsible for restricting this to null-free, error-free inputs (a
     * null or error must be excluded from both the true and the false mask, which this scan does not check).
     */
    public void retainConstantComparison(long[] values, long literal, ComparisonOperator operator)
    {
        if (none()) {
            return;
        }
        int[] buffer = positionsArray(allSelected ? size : selectedCount);
        boolean dense = allSelected;
        int iterations = dense ? size : selectedCount;
        int count = 0;
        switch (operator) {
            case EQUAL -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (values[position] == literal) {
                        buffer[count++] = position;
                    }
                }
            }
            case NOT_EQUAL -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (values[position] != literal) {
                        buffer[count++] = position;
                    }
                }
            }
            case LESS_THAN -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (values[position] < literal) {
                        buffer[count++] = position;
                    }
                }
            }
            case LESS_THAN_OR_EQUAL -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (values[position] <= literal) {
                        buffer[count++] = position;
                    }
                }
            }
            case GREATER_THAN -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (values[position] > literal) {
                        buffer[count++] = position;
                    }
                }
            }
            case GREATER_THAN_OR_EQUAL -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (values[position] >= literal) {
                        buffer[count++] = position;
                    }
                }
            }
        }
        setSelection(size, count, count == size);
    }

    /**
     * Null-aware variant of {@link #retainConstantComparison(long[], long, ComparisonOperator)}: a position whose
     * {@code nulls[position]} is set evaluates to NULL and is excluded (a NULL is neither true nor false). The null
     * check stays inside the monomorphic scan, so a nullable column-vs-constant filter avoids the per-position
     * {@link IntPredicate}/null-accessor dispatch of the general path. {@code nulls == null} delegates to the
     * null-free scan; {@code literal} (the constant operand) is assumed non-null by the caller.
     */
    public void retainConstantComparison(long[] values, long literal, ComparisonOperator operator, boolean[] nulls)
    {
        if (nulls == null) {
            retainConstantComparison(values, literal, operator);
            return;
        }
        if (none()) {
            return;
        }
        int[] buffer = positionsArray(allSelected ? size : selectedCount);
        boolean dense = allSelected;
        int iterations = dense ? size : selectedCount;
        int count = 0;
        switch (operator) {
            case EQUAL -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (!nulls[position] && values[position] == literal) {
                        buffer[count++] = position;
                    }
                }
            }
            case NOT_EQUAL -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (!nulls[position] && values[position] != literal) {
                        buffer[count++] = position;
                    }
                }
            }
            case LESS_THAN -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (!nulls[position] && values[position] < literal) {
                        buffer[count++] = position;
                    }
                }
            }
            case LESS_THAN_OR_EQUAL -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (!nulls[position] && values[position] <= literal) {
                        buffer[count++] = position;
                    }
                }
            }
            case GREATER_THAN -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (!nulls[position] && values[position] > literal) {
                        buffer[count++] = position;
                    }
                }
            }
            case GREATER_THAN_OR_EQUAL -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (!nulls[position] && values[position] >= literal) {
                        buffer[count++] = position;
                    }
                }
            }
        }
        setSelection(size, count, count == size);
    }

    /** Integer-column overload of {@link #retainConstantComparison(long[], long, ComparisonOperator)}. */
    public void retainConstantComparison(int[] values, long literal, ComparisonOperator operator)
    {
        if (none()) {
            return;
        }
        int[] buffer = positionsArray(allSelected ? size : selectedCount);
        boolean dense = allSelected;
        int iterations = dense ? size : selectedCount;
        int count = 0;
        switch (operator) {
            case EQUAL -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (values[position] == literal) {
                        buffer[count++] = position;
                    }
                }
            }
            case NOT_EQUAL -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (values[position] != literal) {
                        buffer[count++] = position;
                    }
                }
            }
            case LESS_THAN -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (values[position] < literal) {
                        buffer[count++] = position;
                    }
                }
            }
            case LESS_THAN_OR_EQUAL -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (values[position] <= literal) {
                        buffer[count++] = position;
                    }
                }
            }
            case GREATER_THAN -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (values[position] > literal) {
                        buffer[count++] = position;
                    }
                }
            }
            case GREATER_THAN_OR_EQUAL -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (values[position] >= literal) {
                        buffer[count++] = position;
                    }
                }
            }
        }
        setSelection(size, count, count == size);
    }

    /** Null-aware integer-column overload of {@link #retainConstantComparison(long[], long, ComparisonOperator, boolean[])}. */
    public void retainConstantComparison(int[] values, long literal, ComparisonOperator operator, boolean[] nulls)
    {
        if (nulls == null) {
            retainConstantComparison(values, literal, operator);
            return;
        }
        if (none()) {
            return;
        }
        int[] buffer = positionsArray(allSelected ? size : selectedCount);
        boolean dense = allSelected;
        int iterations = dense ? size : selectedCount;
        int count = 0;
        switch (operator) {
            case EQUAL -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (!nulls[position] && values[position] == literal) {
                        buffer[count++] = position;
                    }
                }
            }
            case NOT_EQUAL -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (!nulls[position] && values[position] != literal) {
                        buffer[count++] = position;
                    }
                }
            }
            case LESS_THAN -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (!nulls[position] && values[position] < literal) {
                        buffer[count++] = position;
                    }
                }
            }
            case LESS_THAN_OR_EQUAL -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (!nulls[position] && values[position] <= literal) {
                        buffer[count++] = position;
                    }
                }
            }
            case GREATER_THAN -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (!nulls[position] && values[position] > literal) {
                        buffer[count++] = position;
                    }
                }
            }
            case GREATER_THAN_OR_EQUAL -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (!nulls[position] && values[position] >= literal) {
                        buffer[count++] = position;
                    }
                }
            }
        }
        setSelection(size, count, count == size);
    }

    /** Double-column overload of {@link #retainConstantComparison(long[], long, ComparisonOperator)}. */
    public void retainConstantComparison(double[] values, double literal, ComparisonOperator operator)
    {
        if (none()) {
            return;
        }
        int[] buffer = positionsArray(allSelected ? size : selectedCount);
        boolean dense = allSelected;
        int iterations = dense ? size : selectedCount;
        int count = 0;
        switch (operator) {
            case EQUAL -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (values[position] == literal) {
                        buffer[count++] = position;
                    }
                }
            }
            case NOT_EQUAL -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (values[position] != literal) {
                        buffer[count++] = position;
                    }
                }
            }
            case LESS_THAN -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (values[position] < literal) {
                        buffer[count++] = position;
                    }
                }
            }
            case LESS_THAN_OR_EQUAL -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (values[position] <= literal) {
                        buffer[count++] = position;
                    }
                }
            }
            case GREATER_THAN -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (values[position] > literal) {
                        buffer[count++] = position;
                    }
                }
            }
            case GREATER_THAN_OR_EQUAL -> {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (values[position] >= literal) {
                        buffer[count++] = position;
                    }
                }
            }
        }
        setSelection(size, count, count == size);
    }

    /**
     * Retains the positions whose dictionary id selects a {@code true} entry in {@code keep} — the predicate-over-
     * dictionary narrow: the per-entry comparison result is computed once over the distinct values, and each row is
     * a single {@code keep[ids[position]]} array lookup rather than a per-row value compare. A monomorphic loop, so
     * a low-cardinality dictionary column filtered against a constant pays one compare per distinct value plus one
     * lookup per row.
     */
    public void retainDictionaryComparison(int[] ids, boolean[] keep)
    {
        if (none()) {
            return;
        }
        int[] buffer = positionsArray(allSelected ? size : selectedCount);
        boolean dense = allSelected;
        int iterations = dense ? size : selectedCount;
        int count = 0;
        for (int index = 0; index < iterations; index++) {
            int position = dense ? index : buffer[index];
            if (keep[ids[position]]) {
                buffer[count++] = position;
            }
        }
        setSelection(size, count, count == size);
    }

    /**
     * Null-aware predicate-over-dictionary narrow: retains positions whose per-entry result {@code keep[ids[position]]}
     * equals {@code wanted} and that are not null ({@code nulls[position]} unset). {@code wanted} selects the true mask
     * ({@code true}) or the false mask ({@code false}); a NULL is excluded from both. {@code nulls == null} means the
     * column has no nulls. A monomorphic scan — the per-position null/dictionary lookup stays out of an
     * {@link IntPredicate} lambda.
     */
    public void retainDictionaryComparison(int[] ids, boolean[] keep, boolean[] nulls, boolean wanted)
    {
        if (none()) {
            return;
        }
        int[] buffer = positionsArray(allSelected ? size : selectedCount);
        boolean dense = allSelected;
        int iterations = dense ? size : selectedCount;
        int count = 0;
        if (nulls == null) {
            for (int index = 0; index < iterations; index++) {
                int position = dense ? index : buffer[index];
                if (keep[ids[position]] == wanted) {
                    buffer[count++] = position;
                }
            }
        }
        else {
            for (int index = 0; index < iterations; index++) {
                int position = dense ? index : buffer[index];
                if (!nulls[position] && keep[ids[position]] == wanted) {
                    buffer[count++] = position;
                }
            }
        }
        setSelection(size, count, count == size);
    }

    public boolean anyTrue(int start, int end)
    {
        if (none() || end < start || end < 0 || start >= size) {
            return false;
        }

        if (allSelected) {
            return start < size && end >= 0;
        }

        for (int index = 0; index < selectedCount; index++) {
            int position = positions[index];
            if (position > end) {
                return false;
            }
            if (position >= start) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString()
    {
        if (allSelected) {
            return "ALL(" + size + ")";
        }
        return Arrays.toString(Arrays.copyOf(positions, selectedCount));
    }

    public Mask first(int n)
    {
        if (n >= selectedCount) {
            return this;
        }
        if (n <= 0) {
            return new Mask(size, 0, false, EMPTY_POSITIONS);
        }
        int[] result = new int[n];
        if (allSelected) {
            for (int index = 0; index < n; index++) {
                result[index] = index;
            }
        }
        else {
            System.arraycopy(positions, 0, result, 0, n);
        }
        return new Mask(size, n, n == size && isAllPositions(result, n), result);
    }

    public Mask last(int n)
    {
        if (n >= selectedCount) {
            return this;
        }
        if (n <= 0) {
            return new Mask(size, 0, false, EMPTY_POSITIONS);
        }
        int[] result = new int[n];
        if (allSelected) {
            for (int index = 0; index < n; index++) {
                result[index] = size - n + index;
            }
        }
        else {
            System.arraycopy(positions, selectedCount - n, result, 0, n);
        }
        return new Mask(size, n, n == size && isAllPositions(result, n), result);
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
                return index < selectedCount;
            }

            @Override
            public Integer next()
            {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return allSelected ? index++ : positions[index++];
            }
        };
    }

    public Mask difference(Mask other)
    {
        checkCompatible(other);
        if (other.none() || none()) {
            return copy();
        }
        if (other.all()) {
            return new Mask(size, 0, false, EMPTY_POSITIONS);
        }
        if (allSelected) {
            return other.complement();
        }

        int[] result = new int[selectedCount];
        int leftIndex = 0;
        int rightIndex = 0;
        int outputIndex = 0;
        while (leftIndex < selectedCount && rightIndex < other.selectedCount) {
            int leftPosition = positions[leftIndex];
            int rightPosition = other.positions[rightIndex];
            if (leftPosition < rightPosition) {
                result[outputIndex++] = leftPosition;
                leftIndex++;
            }
            else if (leftPosition > rightPosition) {
                rightIndex++;
            }
            else {
                leftIndex++;
                rightIndex++;
            }
        }
        while (leftIndex < selectedCount) {
            result[outputIndex++] = positions[leftIndex++];
        }
        return create(size, result, outputIndex);
    }

    public Mask union(Mask other)
    {
        checkCompatible(other);
        if (allSelected || other.all()) {
            return all(size);
        }
        if (other.none()) {
            return copy();
        }
        if (none()) {
            return other.copy();
        }

        int[] result = new int[Math.min(size, selectedCount + other.selectedCount)];
        int leftIndex = 0;
        int rightIndex = 0;
        int outputIndex = 0;
        while (leftIndex < selectedCount && rightIndex < other.selectedCount) {
            int leftPosition = positions[leftIndex];
            int rightPosition = other.positions[rightIndex];
            if (leftPosition < rightPosition) {
                result[outputIndex++] = leftPosition;
                leftIndex++;
            }
            else if (leftPosition > rightPosition) {
                result[outputIndex++] = rightPosition;
                rightIndex++;
            }
            else {
                result[outputIndex++] = leftPosition;
                leftIndex++;
                rightIndex++;
            }
        }
        while (leftIndex < selectedCount) {
            result[outputIndex++] = positions[leftIndex++];
        }
        while (rightIndex < other.selectedCount) {
            result[outputIndex++] = other.positions[rightIndex++];
        }
        return create(size, result, outputIndex);
    }

    public boolean contains(int position)
    {
        if (position < 0 || position >= size) {
            return false;
        }
        if (allSelected) {
            return true;
        }
        return Arrays.binarySearch(positions, 0, selectedCount, position) >= 0;
    }

    public boolean containsAll(Mask other)
    {
        checkCompatible(other);

        if (other.none()) {
            return true;
        }
        if (none()) {
            return false;
        }
        if (allSelected) {
            return true;
        }
        if (other.all()) {
            return false;
        }

        int leftIndex = 0;
        int rightIndex = 0;
        while (leftIndex < selectedCount && rightIndex < other.selectedCount) {
            if (positions[leftIndex] < other.positions[rightIndex]) {
                leftIndex++;
            }
            else if (positions[leftIndex] > other.positions[rightIndex]) {
                return false;
            }
            else {
                leftIndex++;
                rightIndex++;
            }
        }
        return rightIndex == other.selectedCount;
    }

    public Mask and(BooleanVector other)
    {
        checkVectorCompatibility(other);
        if (other.length() == 0 || none()) {
            return new Mask(size, 0, false, EMPTY_POSITIONS);
        }

        int[] result = new int[selectedCount];
        int outputIndex = 0;
        if (allSelected) {
            for (int position = 0; position < size; position++) {
                if (other.values()[position]) {
                    result[outputIndex++] = position;
                }
            }
        }
        else {
            for (int index = 0; index < selectedCount; index++) {
                int position = positions[index];
                if (other.values()[position]) {
                    result[outputIndex++] = position;
                }
            }
        }
        return create(size, result, outputIndex);
    }

    public Mask andNot(Mask other)
    {
        return difference(other);
    }

    public Mask andNot(BooleanVector other)
    {
        checkVectorCompatibility(other);
        if (other.length() == 0 || none()) {
            return copy();
        }

        int[] result = new int[selectedCount];
        int outputIndex = 0;
        if (allSelected) {
            for (int position = 0; position < size; position++) {
                if (!other.values()[position]) {
                    result[outputIndex++] = position;
                }
            }
        }
        else {
            for (int index = 0; index < selectedCount; index++) {
                int position = positions[index];
                if (!other.values()[position]) {
                    result[outputIndex++] = position;
                }
            }
        }
        return create(size, result, outputIndex);
    }

    public Mask complement()
    {
        if (allSelected) {
            return new Mask(size, 0, false, EMPTY_POSITIONS);
        }
        if (none()) {
            return all(size);
        }

        int[] result = new int[size - selectedCount];
        int outputIndex = 0;
        int position = 0;
        for (int index = 0; index < selectedCount; index++) {
            while (position < positions[index]) {
                result[outputIndex++] = position++;
            }
            position++;
        }
        while (position < size) {
            result[outputIndex++] = position++;
        }
        return create(size, result, outputIndex);
    }

    public Mask or(Mask other)
    {
        return union(other);
    }

    Mask copy()
    {
        if (allSelected) {
            return all(size);
        }
        return new Mask(size, selectedCount, false, Arrays.copyOf(positions, selectedCount));
    }

    private void ensureCapacity(int capacity)
    {
        if (positions.length >= capacity) {
            return;
        }
        positions = Arrays.copyOf(positions, capacity);
    }

    int capacity()
    {
        return positions.length;
    }

    boolean trackedInUse()
    {
        return trackedInUse;
    }

    Mask trackedPrevious()
    {
        return trackedPrevious;
    }

    void trackedPrevious(Mask trackedPrevious)
    {
        this.trackedPrevious = trackedPrevious;
    }

    Mask trackedNext()
    {
        return trackedNext;
    }

    void trackedNext(Mask trackedNext)
    {
        this.trackedNext = trackedNext;
    }

    void markTrackedInUse()
    {
        trackedInUse = true;
    }

    void clearTrackedInUse()
    {
        trackedInUse = false;
        trackedPrevious = null;
        trackedNext = null;
    }

    int[] positionsArray(int requiredCapacity)
    {
        ensureCapacity(requiredCapacity);
        return positions;
    }

    void setSelection(int size, int selectedCount, boolean allSelected)
    {
        checkArgument(size >= 0, "size is negative");
        checkArgument(selectedCount >= 0, "selectedCount is negative");
        checkArgument(selectedCount <= size, "selectedCount exceeds size");
        checkArgument(allSelected == (selectedCount == size), "allSelected must match selectedCount");
        checkArgument(allSelected || positions.length >= selectedCount, "positions capacity is too small");

        this.size = size;
        this.selectedCount = selectedCount;
        this.allSelected = allSelected;
    }

    private void checkCompatible(Mask other)
    {
        checkArgument(size == other.size, "Masks must have the same row-domain size");
    }

    private void checkVectorCompatibility(BooleanVector other)
    {
        if (none()) {
            return;
        }
        int requiredLength = allSelected ? size : maxPosition() + 1;
        checkArgument(other.length() >= requiredLength, "Boolean vector is too short for mask domain");
    }

    private static Mask create(int size, int[] positions, int selectedCount)
    {
        boolean allSelected = selectedCount == size && isAllPositions(positions, selectedCount);
        if (selectedCount == 0) {
            return new Mask(size, 0, false, EMPTY_POSITIONS);
        }
        if (allSelected) {
            return all(size);
        }
        return new Mask(size, selectedCount, false, Arrays.copyOf(positions, selectedCount));
    }

    private static boolean isAllPositions(int[] positions, int selectedCount)
    {
        for (int index = 0; index < selectedCount; index++) {
            if (positions[index] != index) {
                return false;
            }
        }
        return true;
    }
}
