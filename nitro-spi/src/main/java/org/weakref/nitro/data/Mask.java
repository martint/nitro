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

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.function.IntPredicate;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class Mask
        implements Iterable<Integer>
{
    private static final int[] EMPTY_POSITIONS = new int[0];
    private static final VectorSpecies<Integer> INT_SPECIES = IntVector.SPECIES_PREFERRED;
    private static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED;

    private final AllocatorPolicy.MaskFiltering filteringPolicy;
    private Mask trackedPrevious;
    private Mask trackedNext;
    private Object trackedOwner;
    private boolean residentTracked;
    private CapacityListener capacityListener;
    private int size;
    private int selectedCount;
    private boolean allSelected;
    private int[] positions;
    private int positionCount;
    private boolean excludedPositions;
    private DictionaryDomainSelection dictionaryDomainSelection;

    /**
     * A compact, row-aligned dictionary-domain selection retained without expanding it to logical row positions.
     * The mapping array is borrowed from the input vector and is valid for the mask's batch lifetime.
     */
    public record DictionaryDomainSelection(int[] ids, int length, int domainSize, long selectedDomainBits)
    {
        public DictionaryDomainSelection
        {
            requireNonNull(ids, "ids is null");
            checkArgument(length >= 0 && length <= ids.length, "invalid mapping length");
            checkArgument(domainSize >= 0 && domainSize <= Long.SIZE, "invalid dictionary domain size");
        }

        public boolean selects(int dictionaryId)
        {
            return ((selectedDomainBits >>> dictionaryId) & 1L) != 0;
        }
    }

    public static Mask all(int size)
    {
        return all(size, AllocatorPolicy.MaskFiltering.defaults());
    }

    static Mask all(int size, AllocatorPolicy.MaskFiltering filteringPolicy)
    {
        checkArgument(size >= 0, "size is negative");
        return new Mask(size, size, true, EMPTY_POSITIONS, filteringPolicy);
    }

    public static Mask none(int size)
    {
        return none(size, AllocatorPolicy.MaskFiltering.defaults());
    }

    static Mask none(int size, AllocatorPolicy.MaskFiltering filteringPolicy)
    {
        checkArgument(size >= 0, "size is negative");
        return new Mask(size, 0, false, EMPTY_POSITIONS, filteringPolicy);
    }

    public static Mask range(int start, int length)
    {
        return range(start, length, AllocatorPolicy.MaskFiltering.defaults());
    }

    static Mask range(int start, int length, AllocatorPolicy.MaskFiltering filteringPolicy)
    {
        checkArgument(start >= 0, "start is negative");
        checkArgument(length >= 0, "length is negative");
        if (start == 0) {
            return all(length, filteringPolicy);
        }

        int[] positions = new int[length];
        for (int index = 0; index < length; index++) {
            positions[index] = start + index;
        }
        return new Mask(start + length, length, false, positions, filteringPolicy);
    }

    public static Mask sparse(int[] activePositions, int totalPositions)
    {
        return sparse(activePositions, totalPositions, AllocatorPolicy.MaskFiltering.defaults());
    }

    static Mask sparse(int[] activePositions, int totalPositions, AllocatorPolicy.MaskFiltering filteringPolicy)
    {
        checkArgument(totalPositions >= 0, "totalPositions is negative");
        checkArgument(activePositions.length <= totalPositions, "More active positions than total positions");

        if (totalPositions == 0) {
            return all(0, filteringPolicy);
        }

        if (activePositions.length == 0) {
            return none(totalPositions, filteringPolicy);
        }

        int[] positions = Arrays.copyOf(activePositions, activePositions.length);
        boolean allSelected = activePositions.length == totalPositions && isAllPositions(positions, activePositions.length);
        return new Mask(totalPositions, activePositions.length, allSelected, allSelected ? EMPTY_POSITIONS : positions, filteringPolicy);
    }

    static Mask sparseTrusted(int[] activePositions, int selectedCount, int totalPositions)
    {
        return sparseTrusted(activePositions, selectedCount, totalPositions, AllocatorPolicy.MaskFiltering.defaults());
    }

    static Mask sparseTrusted(int[] activePositions, int selectedCount, int totalPositions, AllocatorPolicy.MaskFiltering filteringPolicy)
    {
        checkArgument(totalPositions >= 0, "totalPositions is negative");
        checkArgument(selectedCount >= 0, "selectedCount is negative");
        checkArgument(selectedCount <= totalPositions, "More active positions than total positions");
        checkArgument(activePositions.length >= selectedCount, "activePositions capacity is too small");

        if (selectedCount == 0) {
            return none(totalPositions, filteringPolicy);
        }

        boolean allSelected = selectedCount == totalPositions && isAllPositions(activePositions, selectedCount);
        return new Mask(totalPositions, selectedCount, allSelected, allSelected ? EMPTY_POSITIONS : activePositions, filteringPolicy);
    }

    static Mask allExcept(int[] excludedPositions, int excludedCount, int totalPositions)
    {
        return allExcept(excludedPositions, excludedCount, totalPositions, AllocatorPolicy.MaskFiltering.defaults());
    }

    static Mask allExcept(int[] excludedPositions, int excludedCount, int totalPositions, AllocatorPolicy.MaskFiltering filteringPolicy)
    {
        checkArgument(totalPositions >= 0, "totalPositions is negative");
        checkArgument(excludedCount >= 0, "excludedCount is negative");
        checkArgument(excludedCount <= totalPositions, "More excluded positions than total positions");
        checkArgument(excludedPositions.length >= excludedCount, "excludedPositions capacity is too small");

        if (excludedCount == 0) {
            return all(totalPositions, filteringPolicy);
        }
        if (excludedCount == totalPositions && isAllPositions(excludedPositions, excludedCount)) {
            return new Mask(totalPositions, 0, false, EMPTY_POSITIONS, filteringPolicy);
        }

        return new Mask(
                totalPositions,
                totalPositions - excludedCount,
                false,
                Arrays.copyOf(excludedPositions, excludedCount),
                excludedCount,
                true,
                filteringPolicy);
    }

    private Mask(int size, int selectedCount, boolean allSelected, int[] positions, AllocatorPolicy.MaskFiltering filteringPolicy)
    {
        this(size, selectedCount, allSelected, positions, allSelected ? 0 : selectedCount, false, filteringPolicy);
    }

    private Mask(
            int size,
            int selectedCount,
            boolean allSelected,
            int[] positions,
            int positionCount,
            boolean excludedPositions,
            AllocatorPolicy.MaskFiltering filteringPolicy)
    {
        checkArgument(size >= 0, "size is negative");
        checkArgument(selectedCount >= 0, "selectedCount is negative");
        checkArgument(selectedCount <= size, "selectedCount exceeds size");
        checkArgument(allSelected == (selectedCount == size), "allSelected must match selectedCount");
        checkArgument(positionCount >= 0, "positionCount is negative");
        checkArgument(positionCount <= size, "positionCount exceeds size");
        checkArgument(allSelected || positions.length >= positionCount, "positions capacity is too small");
        checkArgument(!excludedPositions || !allSelected, "allSelected mask cannot use excluded positions");
        checkArgument(allSelected || (excludedPositions ? selectedCount == size - positionCount : positionCount == selectedCount), "positionCount does not match selectedCount");

        this.filteringPolicy = requireNonNull(filteringPolicy, "filteringPolicy is null");
        this.size = size;
        this.selectedCount = selectedCount;
        this.allSelected = allSelected;
        this.positions = allSelected ? EMPTY_POSITIONS : positions;
        this.positionCount = allSelected ? 0 : positionCount;
        this.excludedPositions = excludedPositions;
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
        if (dictionaryDomainSelection != null) {
            for (int position = size - 1; position >= 0; position--) {
                if (dictionaryDomainSelection.selects(dictionaryDomainSelection.ids()[position])) {
                    return position;
                }
            }
            throw new IllegalStateException("Non-empty dictionary selection has no selected position");
        }
        if (excludedPositions) {
            int position = size - 1;
            for (int index = positionCount - 1; index >= 0 && positions[index] == position; index--) {
                position--;
            }
            return position;
        }
        int maximum = positions[0];
        for (int index = 1; index < selectedCount; index++) {
            maximum = Math.max(maximum, positions[index]);
        }
        return maximum;
    }

    public int position(int index)
    {
        checkArgument(index >= 0 && index < selectedCount, "index out of bounds");
        if (allSelected) {
            return index;
        }
        materializeSelectedPositions();
        if (excludedPositions) {
            int position = index;
            while (true) {
                int selectedBeforeOrAt = position - upperBound(positions, positionCount, position);
                if (selectedBeforeOrAt == index) {
                    return position;
                }
                position += index - selectedBeforeOrAt;
            }
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
        materializeSelectedPositions();
        return allSelected ? null : positions;
    }

    public void selectAll(int size)
    {
        checkArgument(size >= 0, "size is negative");
        this.size = size;
        selectedCount = size;
        allSelected = true;
        positionCount = 0;
        excludedPositions = false;
        dictionaryDomainSelection = null;
    }

    public void clear(int size)
    {
        checkArgument(size >= 0, "size is negative");
        this.size = size;
        selectedCount = 0;
        allSelected = false;
        positionCount = 0;
        excludedPositions = false;
        dictionaryDomainSelection = null;
    }

    public void copyFrom(Mask other)
    {
        if (other.dictionaryDomainSelection != null) {
            copyDictionaryDomainFrom(
                    other,
                    other.dictionaryDomainSelection.selectedDomainBits(),
                    other.selectedCount);
            return;
        }
        other.materializeSelectedPositions();
        size = other.size;
        selectedCount = other.selectedCount;
        allSelected = other.allSelected;
        positionCount = other.positionCount;
        excludedPositions = other.excludedPositions;
        dictionaryDomainSelection = null;
        if (!allSelected) {
            ensureCapacity(positionCount);
            System.arraycopy(other.positions, 0, positions, 0, positionCount);
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
        materializeSelectedPositions();
        other.materializeSelectedPositions();
        if (excludedPositions || other.excludedPositions) {
            Mask result = difference(difference(other));
            copyFrom(result);
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
            replacePositions(EMPTY_POSITIONS);
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
        materializeSelectedPositions();
        other.materializeSelectedPositions();
        if (excludedPositions || other.excludedPositions) {
            Mask result = difference(other);
            copyFrom(result);
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
            replacePositions(EMPTY_POSITIONS);
        }
    }

    public void retainIf(IntPredicate predicate)
    {
        if (none()) {
            return;
        }
        materializeSelectedPositions();

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

    /**
     * Compacts this mask in place against a flat Boolean stream. Dense and selected masks use separate monomorphic
     * loops so callers can preserve batch shape without allocating an intermediate mask or a predicate object.
     */
    public void retainBooleans(boolean[] values, boolean wanted)
    {
        if (none()) {
            return;
        }
        checkArgument(values.length > maxPosition(), "Boolean vector is too short for mask domain");

        int rows = selectedCount;
        int retained = 0;
        if (allSelected) {
            int[] positions = positionsArrayForOverwrite(rows);
            for (int position = 0; position < rows; position++) {
                if (values[position] == wanted) {
                    positions[retained++] = position;
                }
            }
        }
        else {
            int[] positions = selectedPositions();
            for (int row = 0; row < rows; row++) {
                int position = positions[row];
                if (values[position] == wanted) {
                    positions[retained++] = position;
                }
            }
        }
        finishRetain(retained);
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
                if (dense) {
                    count = retainDenseLongVector(values, size, literal, false, buffer);
                }
                else {
                    for (int index = 0; index < iterations; index++) {
                        int position = buffer[index];
                        if (values[position] <= literal) {
                            buffer[count++] = position;
                        }
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
                if (dense) {
                    count = retainDenseLongVector(values, size, literal, true, buffer);
                }
                else {
                    for (int index = 0; index < iterations; index++) {
                        int position = buffer[index];
                        if (values[position] >= literal) {
                            buffer[count++] = position;
                        }
                    }
                }
            }
        }
        setSelection(size, count, count == size);
    }

    private static int retainDenseLongVector(long[] values, int size, long literal, boolean greater, int[] output)
    {
        int count = 0;
        int position = 0;
        int vectorLimit = LONG_SPECIES.loopBound(size);
        for (; position < vectorLimit; position += LONG_SPECIES.length()) {
            long matches = LongVector.fromArray(LONG_SPECIES, values, position)
                    .compare(greater ? VectorOperators.GE : VectorOperators.LE, literal)
                    .toLong();
            while (matches != 0) {
                int lane = Long.numberOfTrailingZeros(matches);
                output[count++] = position + lane;
                matches &= matches - 1;
            }
        }
        for (; position < size; position++) {
            if (greater ? values[position] >= literal : values[position] <= literal) {
                output[count++] = position;
            }
        }
        return count;
    }

    /** Retains {@code lowerExclusive < values[position] < upperExclusive} in one pass. */
    public void retainConstantRange(long[] values, long lowerExclusive, long upperExclusive, boolean[] nulls)
    {
        if (none()) {
            return;
        }
        int[] buffer = positionsArray(allSelected ? size : selectedCount);
        boolean dense = allSelected;
        int iterations = dense ? size : selectedCount;
        int count = 0;
        if (dense && nulls == null) {
            int position = 0;
            int vectorLimit = LONG_SPECIES.loopBound(size);
            for (; position < vectorLimit; position += LONG_SPECIES.length()) {
                LongVector vector = LongVector.fromArray(LONG_SPECIES, values, position);
                long matches = vector.compare(VectorOperators.GT, lowerExclusive)
                        .and(vector.compare(VectorOperators.LT, upperExclusive))
                        .toLong();
                while (matches != 0) {
                    int lane = Long.numberOfTrailingZeros(matches);
                    buffer[count++] = position + lane;
                    matches &= matches - 1;
                }
            }
            for (; position < size; position++) {
                long value = values[position];
                if (value > lowerExclusive && value < upperExclusive) {
                    buffer[count++] = position;
                }
            }
        }
        else {
            for (int index = 0; index < iterations; index++) {
                int position = dense ? index : buffer[index];
                long value = values[position];
                if ((nulls == null || !nulls[position]) && value > lowerExclusive && value < upperExclusive) {
                    buffer[count++] = position;
                }
            }
        }
        setSelection(size, count, count == size);
    }

    /** Integer-column overload of {@link #retainConstantRange(long[], long, long, boolean[])}. */
    public void retainConstantRange(int[] values, long lowerExclusive, long upperExclusive, boolean[] nulls)
    {
        if (none()) {
            return;
        }
        int[] buffer = positionsArray(allSelected ? size : selectedCount);
        boolean dense = allSelected;
        int iterations = dense ? size : selectedCount;
        int count = 0;
        boolean vectorized = filteringPolicy.vectorizedDenseIntConstantRange() && dense && nulls == null &&
                lowerExclusive >= Integer.MIN_VALUE && lowerExclusive <= Integer.MAX_VALUE &&
                upperExclusive >= Integer.MIN_VALUE && upperExclusive <= Integer.MAX_VALUE;
        if (vectorized) {
            int position = 0;
            int vectorLimit = INT_SPECIES.loopBound(size);
            int lower = (int) lowerExclusive;
            int upper = (int) upperExclusive;
            long allLanes = -1L >>> (Long.SIZE - INT_SPECIES.length());
            boolean materialized = false;
            for (; position < vectorLimit; position += INT_SPECIES.length()) {
                IntVector vector = IntVector.fromArray(INT_SPECIES, values, position);
                long matches = vector.compare(VectorOperators.GT, lower)
                        .and(vector.compare(VectorOperators.LT, upper))
                        .toLong();
                if (!materialized && matches == allLanes) {
                    continue;
                }
                if (!materialized) {
                    for (int prior = 0; prior < position; prior++) {
                        buffer[prior] = prior;
                    }
                    count = position;
                    materialized = true;
                }
                while (matches != 0) {
                    int lane = Long.numberOfTrailingZeros(matches);
                    buffer[count++] = position + lane;
                    matches &= matches - 1;
                }
            }
            if (!materialized) {
                int firstFailure = position;
                while (firstFailure < size && values[firstFailure] > lower && values[firstFailure] < upper) {
                    firstFailure++;
                }
                if (firstFailure == size) {
                    count = size;
                }
                else {
                    for (int prior = 0; prior < position; prior++) {
                        buffer[prior] = prior;
                    }
                    count = position;
                    for (; position < size; position++) {
                        int value = values[position];
                        if (value > lower && value < upper) {
                            buffer[count++] = position;
                        }
                    }
                }
            }
            else {
                for (; position < size; position++) {
                    int value = values[position];
                    if (value > lower && value < upper) {
                        buffer[count++] = position;
                    }
                }
            }
        }
        else {
            for (int index = 0; index < iterations; index++) {
                int position = dense ? index : buffer[index];
                int value = values[position];
                if ((nulls == null || !nulls[position]) && value > lowerExclusive && value < upperExclusive) {
                    buffer[count++] = position;
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
        if (dense && filteringPolicy.branchlessDenseDoubleLessThan() && operator == ComparisonOperator.LESS_THAN) {
            count = retainDenseDoubleLessThan(values, size, literal, buffer);
            setSelection(size, count, count == size);
            return;
        }
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

    private static int retainDenseDoubleLessThan(double[] values, int size, double literal, int[] output)
    {
        int count = 0;
        for (int position = 0; position < size; position++) {
            // A rejected position is overwritten by the next iteration. The unconditional store plus conditional
            // cursor increment lets C2 use a setcc-style comparison instead of a hard-to-predict branch.
            output[count] = position;
            count += values[position] < literal ? 1 : 0;
        }
        return count;
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
        if (keep.length <= Long.SIZE) {
            long keepBits = dictionaryKeepBits(keep);
            if (tryRetainExistingDictionaryDomain(ids, keepBits, keep.length)) {
                return;
            }
            if (allSelected) {
                retainDictionaryDomain(ids, keepBits, keep.length);
                return;
            }
        }
        int[] buffer = positionsArray(allSelected ? size : selectedCount);
        boolean dense = allSelected;
        int iterations = dense ? size : selectedCount;
        int count = 0;
        if (keep.length <= Long.SIZE) {
            long keepBits = dictionaryKeepBits(keep);
            if (dictionarySparseWrites(keep.length, keepBits, true)) {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (((keepBits >>> ids[position]) & 1L) != 0) {
                        buffer[count++] = position;
                    }
                }
            }
            else {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    buffer[count] = position;
                    count += ((keepBits >>> ids[position]) & 1L) != 0 ? 1 : 0;
                }
            }
            setSelection(size, count, count == size);
            return;
        }
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
        if (nulls == null && keep.length <= Long.SIZE) {
            long keepBits = dictionaryKeepBits(keep);
            long domainBits = keep.length == Long.SIZE ? -1L : (1L << keep.length) - 1;
            long selectedBits = wanted ? keepBits : ~keepBits & domainBits;
            if (tryRetainExistingDictionaryDomain(ids, selectedBits, keep.length)) {
                return;
            }
            if (allSelected) {
                retainDictionaryDomain(ids, selectedBits, keep.length);
                return;
            }
        }
        int[] buffer = positionsArray(allSelected ? size : selectedCount);
        boolean dense = allSelected;
        int iterations = dense ? size : selectedCount;
        int count = 0;
        if (nulls == null) {
            if (keep.length <= Long.SIZE) {
                long keepBits = dictionaryKeepBits(keep);
                long wantedBit = wanted ? 1 : 0;
                if (dictionarySparseWrites(keep.length, keepBits, wanted)) {
                    for (int index = 0; index < iterations; index++) {
                        int position = dense ? index : buffer[index];
                        if (((keepBits >>> ids[position]) & 1L) == wantedBit) {
                            buffer[count++] = position;
                        }
                    }
                }
                else {
                    for (int index = 0; index < iterations; index++) {
                        int position = dense ? index : buffer[index];
                        buffer[count] = position;
                        count += ((keepBits >>> ids[position]) & 1L) == wantedBit ? 1 : 0;
                    }
                }
                setSelection(size, count, count == size);
                return;
            }
            for (int index = 0; index < iterations; index++) {
                int position = dense ? index : buffer[index];
                if (keep[ids[position]] == wanted) {
                    buffer[count++] = position;
                }
            }
        }
        else {
            if (keep.length <= Long.SIZE) {
                long keepBits = dictionaryKeepBits(keep);
                long wantedBit = wanted ? 1 : 0;
                if (dictionarySparseWrites(keep.length, keepBits, wanted)) {
                    for (int index = 0; index < iterations; index++) {
                        int position = dense ? index : buffer[index];
                        if (!nulls[position] && ((keepBits >>> ids[position]) & 1L) == wantedBit) {
                            buffer[count++] = position;
                        }
                    }
                }
                else {
                    for (int index = 0; index < iterations; index++) {
                        int position = dense ? index : buffer[index];
                        buffer[count] = position;
                        count += !nulls[position] && ((keepBits >>> ids[position]) & 1L) == wantedBit ? 1 : 0;
                    }
                }
                setSelection(size, count, count == size);
                return;
            }
            for (int index = 0; index < iterations; index++) {
                int position = dense ? index : buffer[index];
                if (!nulls[position] && keep[ids[position]] == wanted) {
                    buffer[count++] = position;
                }
            }
        }
        setSelection(size, count, count == size);
    }

    private static long dictionaryKeepBits(boolean[] keep)
    {
        long bits = 0;
        for (int index = 0; index < keep.length; index++) {
            if (keep[index]) {
                bits |= 1L << index;
            }
        }
        return bits;
    }

    private void retainDictionaryDomain(int[] ids, long selectedDomainBits, int domainSize)
    {
        checkArgument(ids.length >= size, "Dictionary ids are too short for mask domain");
        // A compact dictionary selection does not otherwise use the position buffer. Keep the per-domain
        // histogram there so subsequent aligned predicates can compose in O(domain size) without a side
        // allocation. Materialization may overwrite it only after the compact selection is no longer needed.
        ensureCapacity(domainSize);
        Arrays.fill(positions, 0, domainSize, 0);
        for (int position = 0; position < size; position++) {
            positions[ids[position]]++;
        }
        int count = 0;
        for (int dictionaryId = 0; dictionaryId < domainSize; dictionaryId++) {
            if (((selectedDomainBits >>> dictionaryId) & 1L) != 0) {
                count += positions[dictionaryId];
            }
        }
        if (count == size) {
            selectAll(size);
            return;
        }
        if (count == 0) {
            clear(size);
            return;
        }
        selectedCount = count;
        allSelected = false;
        positionCount = 0;
        excludedPositions = false;
        dictionaryDomainSelection = new DictionaryDomainSelection(ids, size, domainSize, selectedDomainBits);
    }

    private boolean tryRetainExistingDictionaryDomain(int[] ids, long selectedDomainBits, int domainSize)
    {
        DictionaryDomainSelection selection = dictionaryDomainSelection;
        if (selection == null ||
                selection.ids() != ids ||
                selection.length() != size ||
                selection.domainSize() != domainSize) {
            return false;
        }

        long combinedBits = selection.selectedDomainBits() & selectedDomainBits;
        if (combinedBits == selection.selectedDomainBits()) {
            return true;
        }
        int count = 0;
        for (int dictionaryId = 0; dictionaryId < domainSize; dictionaryId++) {
            if (((combinedBits >>> dictionaryId) & 1L) != 0) {
                count += positions[dictionaryId];
            }
        }
        if (count == 0) {
            clear(size);
            return true;
        }
        selectedCount = count;
        dictionaryDomainSelection = new DictionaryDomainSelection(ids, size, domainSize, combinedBits);
        return true;
    }

    /**
     * Returns the encoded selection when {@code dictionary} is aligned to the exact mapping that produced it.
     * Otherwise returns {@code null}; callers must use ordinary logical positions.
     */
    public DictionaryDomainSelection dictionaryDomainSelection(DictionaryVector dictionary)
    {
        DictionaryDomainSelection selection = dictionaryDomainSelection;
        if (selection == null || dictionary == null || selection.length() != dictionary.length() || selection.ids() != dictionary.ids()) {
            return null;
        }
        return selection;
    }

    /**
     * Copies the exact logical-row frequency of every entry from the current compact dictionary-domain selection.
     * The returned immutable-by-contract snapshot can travel with a derived encoded vector after this mask is
     * released or reused.
     */
    public int[] copyDictionaryDomainFrequencies(DictionaryDomainSelection selection)
    {
        checkArgument(selection != null && selection == dictionaryDomainSelection, "Dictionary selection is not current");
        return Arrays.copyOf(positions, selection.domainSize());
    }

    private boolean dictionarySparseWrites(int dictionarySize, long keepBits, boolean wanted)
    {
        long domainBits = dictionarySize == Long.SIZE ? -1L : (1L << dictionarySize) - 1;
        long selectedBits = wanted ? keepBits : ~keepBits & domainBits;
        return Long.bitCount(selectedBits) <=
                dictionarySize * filteringPolicy.dictionarySparseWriteMaximumSelectedDomainFraction();
    }

    /**
     * Single-match specialization of {@link #retainDictionaryComparison(int[], boolean[], boolean[], boolean)}.
     * When a dictionary predicate matches exactly one dictionary entry, compare the row id directly instead of
     * loading a second, indirectly indexed boolean array. This is exact for both the matching and complement masks;
     * nullable positions remain excluded from either SQL predicate result.
     */
    public void retainDictionaryIdComparison(int[] ids, int matchingId, boolean[] nulls, boolean wanted)
    {
        if (none()) {
            return;
        }
        int[] buffer = positionsArray(allSelected ? size : selectedCount);
        boolean dense = allSelected;
        int iterations = dense ? size : selectedCount;
        int count = 0;
        if (nulls == null) {
            if (wanted) {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (ids[position] == matchingId) {
                        buffer[count++] = position;
                    }
                }
            }
            else {
                for (int index = 0; index < iterations; index++) {
                    int position = dense ? index : buffer[index];
                    if (ids[position] != matchingId) {
                        buffer[count++] = position;
                    }
                }
            }
        }
        else if (wanted) {
            for (int index = 0; index < iterations; index++) {
                int position = dense ? index : buffer[index];
                if (!nulls[position] && ids[position] == matchingId) {
                    buffer[count++] = position;
                }
            }
        }
        else {
            for (int index = 0; index < iterations; index++) {
                int position = dense ? index : buffer[index];
                if (!nulls[position] && ids[position] != matchingId) {
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
        materializeSelectedPositions();
        if (excludedPositions) {
            int first = Math.max(0, start);
            int last = Math.min(size - 1, end);
            for (int position = first; position <= last; position++) {
                if (!contains(position)) {
                    return true;
                }
            }
            return false;
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
        materializeSelectedPositions();
        if (excludedPositions) {
            int[] selected = new int[selectedCount];
            int index = 0;
            for (int position : this) {
                selected[index++] = position;
            }
            return Arrays.toString(selected);
        }
        return Arrays.toString(Arrays.copyOf(positions, selectedCount));
    }

    public Mask first(int n)
    {
        if (n >= selectedCount) {
            return this;
        }
        if (n <= 0) {
            return none(size, filteringPolicy);
        }
        materializeSelectedPositions();
        int[] result = new int[n];
        if (allSelected) {
            for (int index = 0; index < n; index++) {
                result[index] = index;
            }
        }
        else if (excludedPositions) {
            for (int index = 0; index < n; index++) {
                result[index] = position(index);
            }
        }
        else {
            System.arraycopy(positions, 0, result, 0, n);
        }
        return new Mask(size, n, n == size && isAllPositions(result, n), result, filteringPolicy);
    }

    public Mask last(int n)
    {
        if (n >= selectedCount) {
            return this;
        }
        if (n <= 0) {
            return none(size, filteringPolicy);
        }
        materializeSelectedPositions();
        int[] result = new int[n];
        if (allSelected) {
            for (int index = 0; index < n; index++) {
                result[index] = size - n + index;
            }
        }
        else if (excludedPositions) {
            int firstIndex = selectedCount - n;
            for (int index = 0; index < n; index++) {
                result[index] = position(firstIndex + index);
            }
        }
        else {
            System.arraycopy(positions, selectedCount - n, result, 0, n);
        }
        return new Mask(size, n, n == size && isAllPositions(result, n), result, filteringPolicy);
    }

    @Override
    public PrimitiveIterator.OfInt iterator()
    {
        materializeSelectedPositions();
        return new PrimitiveIterator.OfInt()
        {
            private int index;
            private int nextPosition;
            private int excludedIndex;

            @Override
            public boolean hasNext()
            {
                return index < selectedCount;
            }

            @Override
            public int nextInt()
            {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                if (allSelected) {
                    return index++;
                }
                if (!excludedPositions) {
                    return positions[index++];
                }
                while (excludedIndex < positionCount && positions[excludedIndex] == nextPosition) {
                    nextPosition++;
                    excludedIndex++;
                }
                index++;
                return nextPosition++;
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
            return none(size, filteringPolicy);
        }
        if (allSelected) {
            return other.complement(filteringPolicy);
        }
        materializeSelectedPositions();
        other.materializeSelectedPositions();
        if (excludedPositions || other.excludedPositions) {
            return genericDifference(other);
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
            return all(size, filteringPolicy);
        }
        if (other.none()) {
            return copy();
        }
        if (none()) {
            return other.copy(filteringPolicy);
        }
        materializeSelectedPositions();
        other.materializeSelectedPositions();
        if (excludedPositions || other.excludedPositions) {
            return genericUnion(other);
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
        materializeSelectedPositions();
        if (excludedPositions) {
            return Arrays.binarySearch(positions, 0, positionCount, position) < 0;
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
        materializeSelectedPositions();
        other.materializeSelectedPositions();
        if (excludedPositions || other.excludedPositions) {
            for (int position : other) {
                if (!contains(position)) {
                    return false;
                }
            }
            return true;
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
            return none(size, filteringPolicy);
        }
        materializeSelectedPositions();

        int[] result = new int[selectedCount];
        int outputIndex = 0;
        if (allSelected) {
            for (int position = 0; position < size; position++) {
                if (other.values()[position]) {
                    result[outputIndex++] = position;
                }
            }
        }
        else if (excludedPositions) {
            for (int position : this) {
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
        materializeSelectedPositions();

        int[] result = new int[selectedCount];
        int outputIndex = 0;
        if (allSelected) {
            for (int position = 0; position < size; position++) {
                if (!other.values()[position]) {
                    result[outputIndex++] = position;
                }
            }
        }
        else if (excludedPositions) {
            for (int position : this) {
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
        return complement(filteringPolicy);
    }

    Mask complement(AllocatorPolicy.MaskFiltering resultPolicy)
    {
        if (allSelected) {
            return none(size, resultPolicy);
        }
        if (none()) {
            return all(size, resultPolicy);
        }
        if (dictionaryDomainSelection != null) {
            Mask result = none(size, resultPolicy);
            long domainBits = dictionaryDomainSelection.domainSize() == Long.SIZE
                    ? -1L
                    : (1L << dictionaryDomainSelection.domainSize()) - 1;
            result.copyDictionaryDomainFrom(
                    this,
                    ~dictionaryDomainSelection.selectedDomainBits() & domainBits,
                    size - selectedCount);
            return result;
        }
        materializeSelectedPositions();
        if (excludedPositions) {
            return create(size, positions, positionCount, resultPolicy);
        }

        return allExcept(positions, selectedCount, size, resultPolicy);
    }

    /** Copies the complement of {@code source} without expanding a compact dictionary-domain selection. */
    void copyComplementFrom(Mask source)
    {
        if (source.all()) {
            clear(source.size);
            return;
        }
        if (source.none()) {
            selectAll(source.size);
            return;
        }
        if (source.dictionaryDomainSelection != null) {
            long domainBits = source.dictionaryDomainSelection.domainSize() == Long.SIZE
                    ? -1L
                    : (1L << source.dictionaryDomainSelection.domainSize()) - 1;
            copyDictionaryDomainFrom(
                    source,
                    ~source.dictionaryDomainSelection.selectedDomainBits() & domainBits,
                    source.size - source.selectedCount);
            return;
        }

        source.materializeSelectedPositions();
        size = source.size;
        selectedCount = source.size - source.selectedCount;
        allSelected = false;
        dictionaryDomainSelection = null;
        positionCount = source.positionCount;
        excludedPositions = !source.excludedPositions;
        ensureCapacity(positionCount);
        System.arraycopy(source.positions, 0, positions, 0, positionCount);
    }

    private void copyDictionaryDomainFrom(Mask source, long selectedDomainBits, int selectedCount)
    {
        DictionaryDomainSelection sourceSelection = source.dictionaryDomainSelection;
        checkArgument(sourceSelection != null, "Source has no dictionary-domain selection");
        size = source.size;
        this.selectedCount = selectedCount;
        allSelected = false;
        positionCount = 0;
        excludedPositions = false;
        ensureCapacity(sourceSelection.domainSize());
        System.arraycopy(source.positions, 0, positions, 0, sourceSelection.domainSize());
        dictionaryDomainSelection = new DictionaryDomainSelection(
                sourceSelection.ids(),
                sourceSelection.length(),
                sourceSelection.domainSize(),
                selectedDomainBits);
    }

    public Mask or(Mask other)
    {
        return union(other);
    }

    public Mask copy()
    {
        return copy(filteringPolicy);
    }

    Mask copy(AllocatorPolicy.MaskFiltering resultPolicy)
    {
        if (allSelected) {
            return all(size, resultPolicy);
        }
        materializeSelectedPositions();
        if (excludedPositions) {
            return new Mask(size, selectedCount, false, Arrays.copyOf(positions, positionCount), positionCount, true, resultPolicy);
        }
        return new Mask(size, selectedCount, false, Arrays.copyOf(positions, selectedCount), resultPolicy);
    }

    private void ensureCapacity(int capacity)
    {
        if (positions.length >= capacity) {
            return;
        }
        replacePositions(Arrays.copyOf(positions, capacity));
    }

    private void replacePositions(int[] replacement)
    {
        int oldCapacity = positions.length;
        positions = replacement;
        if (capacityListener != null && oldCapacity != replacement.length) {
            capacityListener.capacityChanged(oldCapacity, replacement.length);
        }
    }

    int capacity()
    {
        return positions.length;
    }

    private Mask genericDifference(Mask other)
    {
        int[] result = new int[selectedCount];
        int outputIndex = 0;
        for (int position : this) {
            if (!other.contains(position)) {
                result[outputIndex++] = position;
            }
        }
        return create(size, result, outputIndex);
    }

    private Mask genericUnion(Mask other)
    {
        int[] result = new int[Math.min(size, selectedCount + other.selectedCount)];
        PrimitiveIterator.OfInt left = iterator();
        PrimitiveIterator.OfInt right = other.iterator();
        Integer leftPosition = left.hasNext() ? left.next() : null;
        Integer rightPosition = right.hasNext() ? right.next() : null;
        int outputIndex = 0;
        while (leftPosition != null && rightPosition != null) {
            if (leftPosition < rightPosition) {
                result[outputIndex++] = leftPosition;
                leftPosition = left.hasNext() ? left.next() : null;
            }
            else if (leftPosition > rightPosition) {
                result[outputIndex++] = rightPosition;
                rightPosition = right.hasNext() ? right.next() : null;
            }
            else {
                result[outputIndex++] = leftPosition;
                leftPosition = left.hasNext() ? left.next() : null;
                rightPosition = right.hasNext() ? right.next() : null;
            }
        }
        while (leftPosition != null) {
            result[outputIndex++] = leftPosition;
            leftPosition = left.hasNext() ? left.next() : null;
        }
        while (rightPosition != null) {
            result[outputIndex++] = rightPosition;
            rightPosition = right.hasNext() ? right.next() : null;
        }
        return create(size, result, outputIndex);
    }

    private void materializeSelectedPositions()
    {
        if (dictionaryDomainSelection != null) {
            DictionaryDomainSelection selection = dictionaryDomainSelection;
            ensureCapacity(selectedCount);
            int outputIndex = 0;
            for (int position = 0; position < selection.length(); position++) {
                if (selection.selects(selection.ids()[position])) {
                    positions[outputIndex++] = position;
                }
            }
            if (outputIndex != selectedCount) {
                throw new IllegalStateException("Dictionary selection count changed while borrowed");
            }
            positionCount = selectedCount;
            excludedPositions = false;
            dictionaryDomainSelection = null;
            return;
        }
        if (!excludedPositions) {
            return;
        }

        int[] selected = new int[selectedCount];
        int outputIndex = 0;
        int excludedIndex = 0;
        for (int position = 0; position < size; position++) {
            if (excludedIndex < positionCount && positions[excludedIndex] == position) {
                excludedIndex++;
                continue;
            }
            selected[outputIndex++] = position;
        }
        replacePositions(selected);
        positionCount = selectedCount;
        excludedPositions = false;
    }

    boolean trackedInUse()
    {
        return trackedOwner != null;
    }

    boolean trackedBy(Object owner)
    {
        return trackedOwner == owner;
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

    void markTrackedInUse(Object owner)
    {
        trackedOwner = requireNonNull(owner, "owner is null");
    }

    void clearTrackedInUse()
    {
        trackedOwner = null;
        trackedPrevious = null;
        trackedNext = null;
    }

    boolean residentTracked()
    {
        return residentTracked;
    }

    void markResidentTracked(CapacityListener capacityListener)
    {
        residentTracked = true;
        this.capacityListener = requireNonNull(capacityListener, "capacityListener is null");
    }

    void clearResidentTracked()
    {
        residentTracked = false;
        capacityListener = null;
    }

    @FunctionalInterface
    interface CapacityListener
    {
        void capacityChanged(int oldCapacity, int newCapacity);
    }

    int[] positionsArray(int requiredCapacity)
    {
        materializeSelectedPositions();
        ensureCapacity(requiredCapacity);
        return positions;
    }

    /**
     * Returns this mask's backing position array for an allocator-controlled direct-fill operation. Callers may only
     * overwrite the active prefix established by the allocating operation and must not retain the array after release.
     * The previous selection is deliberately not materialized: a pooled mask may retain a compact dictionary-domain
     * selection whose borrowed mapping ended with the previous lease, and every caller of this method replaces that
     * selection before exposing the mask again.
     */
    public int[] positionsArrayForOverwrite(int requiredCapacity)
    {
        ensureCapacity(requiredCapacity);
        return positions;
    }

    /**
     * Completes an in-place retain kernel that compacted selected positions into this mask's existing position
     * buffer. Dense callers fill a buffer obtained from {@link #positionsArrayForOverwrite}; sparse callers compact
     * the array returned by {@link #selectedPositions}. The retained positions must remain sorted and in-domain.
     */
    public void finishRetain(int retainedCount)
    {
        setSelection(size, retainedCount, retainedCount == size);
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
        this.positionCount = allSelected ? 0 : selectedCount;
        this.excludedPositions = false;
        this.dictionaryDomainSelection = null;
    }

    void setExclusion(int size, int excludedCount)
    {
        checkArgument(size >= 0, "size is negative");
        checkArgument(excludedCount >= 0, "excludedCount is negative");
        checkArgument(excludedCount <= size, "excludedCount exceeds size");
        checkArgument(positions.length >= excludedCount, "positions capacity is too small");
        dictionaryDomainSelection = null;

        if (excludedCount == 0) {
            selectAll(size);
            return;
        }
        if (excludedCount == size && isAllPositions(positions, excludedCount)) {
            clear(size);
            return;
        }

        this.size = size;
        this.selectedCount = size - excludedCount;
        this.allSelected = false;
        this.positionCount = excludedCount;
        this.excludedPositions = true;
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

    private Mask create(int size, int[] positions, int selectedCount)
    {
        return create(size, positions, selectedCount, filteringPolicy);
    }

    private static Mask create(
            int size,
            int[] positions,
            int selectedCount,
            AllocatorPolicy.MaskFiltering filteringPolicy)
    {
        boolean allSelected = selectedCount == size && isAllPositions(positions, selectedCount);
        if (selectedCount == 0) {
            return none(size, filteringPolicy);
        }
        if (allSelected) {
            return all(size, filteringPolicy);
        }
        return new Mask(size, selectedCount, false, Arrays.copyOf(positions, selectedCount), filteringPolicy);
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

    private static int upperBound(int[] values, int count, int value)
    {
        int low = 0;
        int high = count;
        while (low < high) {
            int middle = (low + high) >>> 1;
            if (values[middle] <= value) {
                low = middle + 1;
            }
            else {
                high = middle;
            }
        }
        return low;
    }
}
