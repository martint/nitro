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
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;

public final class ConcatenatedBooleanVector
        implements Vector
{
    private static final boolean MONOTONIC_POSITION_COPY =
            Boolean.parseBoolean(System.getProperty("nitro.concatenatedBoolean.monotonicPositionCopy", "true"));
    private static final boolean DIRECT_POSITION_COPY =
            Boolean.parseBoolean(System.getProperty("nitro.concatenatedBoolean.directPositionCopy", "true"));
    private final Vector[] segments;
    private final int[] offsets;
    private final int length;

    public ConcatenatedBooleanVector(Vector[] segments)
    {
        checkArgument(segments.length > 0, "segments is empty");
        this.segments = Arrays.copyOf(segments, segments.length);
        this.offsets = new int[segments.length + 1];
        int totalLength = 0;
        for (int index = 0; index < segments.length; index++) {
            Vector segment = this.segments[index];
            checkArgument(segment != null, "segment is null");
            checkArgument(isBooleanBacked(segment), "segment is not boolean-backed: %s", segment.getClass().getSimpleName());
            offsets[index] = totalLength;
            totalLength += segment.length();
        }
        offsets[segments.length] = totalLength;
        this.length = totalLength;
    }

    public boolean value(int position)
    {
        int segmentIndex = segmentIndex(position);
        return booleanValue(segments[segmentIndex], position - offsets[segmentIndex]);
    }

    public int segmentCount()
    {
        return segments.length;
    }

    public Vector segment(int index)
    {
        return segments[index];
    }

    @Override
    public int length()
    {
        return length;
    }

    @Override
    public long retainedBytes()
    {
        return (long) offsets.length * Integer.BYTES;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext)
    {
        BooleanVector copy = allocator.allocate(allocationContext, BooleanVector.class, length, BooleanVector::new);
        copyInto(copy);
        return copy;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        BooleanVector copy = allocator.allocate(allocationContext, BooleanVector.class, positions.length, BooleanVector::new);
        for (int index = 0; index < positions.length; index++) {
            copy.values()[index] = value(positions[index]);
        }
        return copy;
    }

    @Override
    public Vector copyMasked(Allocator allocator, Allocator.Context allocationContext, Vector existing, Mask mask)
    {
        BooleanVector target = ensureBooleanCapacity(allocator, allocationContext, existing, length);
        for (int position : mask) {
            target.values()[position] = value(position);
        }
        return target;
    }

    @Override
    public Vector copyPositionsInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        BooleanVector target = ensureBooleanCapacity(allocator, allocationContext, existing, size);
        boolean orderedPositions = MONOTONIC_POSITION_COPY && isMostlyNonDecreasing(sourcePositions, sourceCount);
        if (DIRECT_POSITION_COPY && orderedPositions) {
            copyPositionsDirect(sourcePositions, sourceCount, target.values(), outputStart);
            return target;
        }
        int index = 0;
        int segmentHint = 0;
        while (index < sourceCount) {
            int sourcePosition = sourcePositions[index];
            int segmentIndex = orderedPositions ? segmentIndexFromHint(sourcePosition, segmentHint) : segmentIndex(sourcePosition);
            segmentHint = segmentIndex;
            int segmentOffset = offsets[segmentIndex];
            int groupStart = index;
            while (index < sourceCount) {
                int currentSegment = orderedPositions ?
                        segmentIndexFromHint(sourcePositions[index], segmentHint) :
                        segmentIndex(sourcePositions[index]);
                if (currentSegment != segmentIndex) {
                    segmentHint = currentSegment;
                    break;
                }
                index++;
            }
            int groupCount = index - groupStart;
            if (groupCount == 1) {
                segments[segmentIndex].copySinglePositionInto(allocator, allocationContext, target, sourcePosition - segmentOffset, outputStart + groupStart, size);
                continue;
            }
            int[] localPositions = new int[groupCount];
            for (int positionIndex = 0; positionIndex < groupCount; positionIndex++) {
                localPositions[positionIndex] = sourcePositions[groupStart + positionIndex] - segmentOffset;
            }
            segments[segmentIndex].copyPositionsInto(allocator, allocationContext, target, localPositions, groupCount, outputStart + groupStart, size);
        }
        return target;
    }

    private void copyPositionsDirect(int[] sourcePositions, int sourceCount, boolean[] output, int outputStart)
    {
        int segmentHint = 0;
        int runHint = 0;
        int previousSegment = -1;
        for (int index = 0; index < sourceCount; index++) {
            int sourcePosition = sourcePositions[index];
            int segmentIndex = segmentIndexFromHint(sourcePosition, segmentHint);
            if (segmentIndex != previousSegment) {
                runHint = 0;
                previousSegment = segmentIndex;
            }
            segmentHint = segmentIndex;
            int localPosition = sourcePosition - offsets[segmentIndex];
            Vector segment = segments[segmentIndex];
            if (segment instanceof BooleanVector values) {
                output[outputStart + index] = values.values()[localPosition];
            }
            else if (segment instanceof RleVector values) {
                int runIndex = values.runIndexFromHint(localPosition, runHint);
                output[outputStart + index] = booleanValue(values.values(), runIndex);
                runHint = runIndex;
            }
            else {
                output[outputStart + index] = booleanValue(segment, localPosition);
            }
        }
    }

    @Override
    public Vector copySelectedPositionsInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, SelectedPositions sourcePositions, int outputStart, int size)
    {
        BooleanVector target = ensureBooleanCapacity(allocator, allocationContext, existing, size);
        for (int index = 0; index < sourcePositions.count(); index++) {
            target.values()[outputStart + index] = value(sourcePositions.position(index));
        }
        return target;
    }

    @Override
    public Vector copySinglePositionInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        BooleanVector target = ensureBooleanCapacity(allocator, allocationContext, existing, size);
        target.values()[outputPosition] = value(sourcePosition);
        return target;
    }

    @Override
    public Vector emptyLike(Allocator allocator, Allocator.Context allocationContext)
    {
        return allocator.allocate(allocationContext, BooleanVector.class, 0, BooleanVector::new);
    }

    @Override
    public Vector materializeRows(Allocator allocator, Allocator.Context allocationContext, Vector[] rows)
    {
        BooleanVector result = allocator.allocate(allocationContext, BooleanVector.class, VectorSupport.totalLength(rows), BooleanVector::new);
        int outputStart = 0;
        for (Vector row : rows) {
            int rowLength = row.length();
            if (rowLength == 1) {
                row.copySinglePositionInto(allocator, allocationContext, result, 0, outputStart, result.length());
            }
            else if (rowLength > 1) {
                row.copyPositionsInto(allocator, allocationContext, result, VectorSupport.densePositions(rowLength), rowLength, outputStart, result.length());
            }
            outputStart += rowLength;
        }
        return result;
    }

    @Override
    public void copyInto(Vector target)
    {
        BooleanVector output = (BooleanVector) target;
        int outputStart = 0;
        for (Vector segment : segments) {
            int segmentLength = segment.length();
            if (segment instanceof BooleanVector values) {
                System.arraycopy(values.values(), 0, output.values(), outputStart, segmentLength);
            }
            else {
                for (int position = 0; position < segmentLength; position++) {
                    output.values()[outputStart + position] = booleanValue(segment, position);
                }
            }
            outputStart += segmentLength;
        }
    }

    @Override
    public void forEachChildVector(Consumer<Vector> consumer)
    {
        for (Vector segment : segments) {
            consumer.accept(segment);
        }
    }

    @Override
    public String toString()
    {
        return "ConcatenatedBooleanVector{length=" + length + ", segments=" + Arrays.toString(segments) + "}";
    }

    private int segmentIndex(int position)
    {
        checkArgument(position >= 0 && position < length, "position is out of bounds: %s", position);
        int index = Arrays.binarySearch(offsets, position);
        if (index >= 0) {
            return Math.min(index, segments.length - 1);
        }
        return -index - 2;
    }

    private int segmentIndexFromHint(int position, int hint)
    {
        checkArgument(position >= 0 && position < length, "position is out of bounds: %s", position);
        if (hint >= 0 && hint < segments.length && position >= offsets[hint]) {
            int index = hint;
            while (index + 1 < offsets.length && position >= offsets[index + 1]) {
                index++;
            }
            return Math.min(index, segments.length - 1);
        }
        return segmentIndex(position);
    }

    private static boolean isMostlyNonDecreasing(int[] positions, int count)
    {
        int maximumBackwardTransitions = Math.max(1, count >>> 4);
        int backwardTransitions = 0;
        for (int index = 1; index < count; index++) {
            if (positions[index] < positions[index - 1]) {
                backwardTransitions++;
                if (backwardTransitions > maximumBackwardTransitions) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean isBooleanBacked(Vector vector)
    {
        return switch (vector) {
            case BooleanVector _ -> true;
            case DictionaryVector values -> isBooleanBacked(values.values());
            case RleVector values -> isBooleanBacked(values.values());
            case ConcatenatedBooleanVector _ -> true;
            default -> false;
        };
    }

    private static boolean booleanValue(Vector vector, int position)
    {
        return switch (vector) {
            case BooleanVector values -> values.values()[position];
            case DictionaryVector values -> booleanValue(values.values(), values.ids()[position]);
            case RleVector values -> booleanValue(values.values(), values.runIndex(position));
            case ConcatenatedBooleanVector values -> values.value(position);
            default -> throw new IllegalArgumentException("Expected boolean-backed vector but found " + vector.getClass().getSimpleName());
        };
    }

    private static BooleanVector ensureBooleanCapacity(Allocator allocator, Allocator.Context allocationContext, Vector existing, int size)
    {
        if (existing instanceof BooleanVector vector) {
            return allocator.allocateOrGrow(allocationContext, vector, BooleanVector.class, size, BooleanVector::new);
        }

        BooleanVector result = allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new);
        if (existing != null) {
            int copied = Math.min(existing.length(), size);
            for (int position = 0; position < copied; position++) {
                result.values()[position] = booleanValue(existing, position);
            }
        }
        return result;
    }
}
