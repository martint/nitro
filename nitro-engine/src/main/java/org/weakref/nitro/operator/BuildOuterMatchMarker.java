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

import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.RegionVector;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.concurrent.atomic.AtomicLongArray;

import static org.weakref.nitro.operator.BuildOuterMatchMarkerGenerator.Shape;
import static org.weakref.nitro.operator.BuildOuterMatchMarkerGenerator.StepKind;

/** Resolves arbitrary encoded build identities once per batch and invokes a shape-generated marker loop. */
final class BuildOuterMatchMarker
        implements AutoCloseable
{
    private enum Carrier
    {
        LONG,
        BOOLEAN
    }

    private record BoundVector(
            Object values,
            boolean intValues,
            int[][] stepArrays,
            int[] stepOffsets,
            StepKind[] stepKinds) {}

    private static final int[][] NO_STEP_ARRAYS = new int[0][];
    private static final int[] NO_STEP_OFFSETS = new int[0];
    private static final StepKind[] NO_STEP_KINDS = new StepKind[0];

    private final BuildOuterMatchMarkerGenerator generator;
    private final PrimitiveArrayPool arrayPool;
    private final AtomicLongArray matched;
    private Shape currentShape;
    private BuildOuterMatchMarkerKernel currentKernel;
    private boolean closed;

    BuildOuterMatchMarker(
            BuildOuterMatchMarkerGenerator generator,
            PrimitiveArrayPool arrayPool,
            AtomicLongArray matched)
    {
        this.generator = generator;
        this.arrayPool = arrayPool;
        this.matched = matched;
    }

    void mark(Vector identities, Vector nulls, Mask mask)
    {
        if (closed) {
            throw new IllegalStateException("Build-outer match marker is closed");
        }
        BoundVector values = null;
        BoundVector nullValues = null;
        try {
            values = bind(identities, Carrier.LONG);
            if (!VectorAccess.isAllFalseNulls(nulls)) {
                nullValues = bind(nulls, Carrier.BOOLEAN);
            }
            Shape shape = Shape.of(values.intValues(), mask.all(), values.stepKinds(), nullValues == null ? null : nullValues.stepKinds());
            if (!shape.equals(currentShape)) {
                currentShape = shape;
                currentKernel = generator.create(shape);
            }
            currentKernel.mark(
                    matched,
                    values.values(),
                    values.stepArrays(),
                    values.stepOffsets(),
                    nullValues == null ? null : (boolean[]) nullValues.values(),
                    nullValues == null ? NO_STEP_ARRAYS : nullValues.stepArrays(),
                    nullValues == null ? NO_STEP_OFFSETS : nullValues.stepOffsets(),
                    mask.selectedPositions(),
                    mask.selectedCount());
        }
        finally {
            release(values);
            release(nullValues);
        }
    }

    @Override
    public void close()
    {
        closed = true;
        currentShape = null;
        currentKernel = null;
    }

    private BoundVector bind(Vector vector, Carrier carrier)
    {
        int depth = wrapperDepth(vector);
        int[][] stepArrays = depth == 0 ? NO_STEP_ARRAYS : new int[depth][];
        int[] stepOffsets = depth == 0 ? NO_STEP_OFFSETS : new int[depth];
        StepKind[] stepKinds = depth == 0 ? NO_STEP_KINDS : new StepKind[depth];
        Vector current = vector;
        int step = 0;
        try {
            while (step < depth) {
                switch (current) {
                    case RegionVector region -> {
                        stepKinds[step] = StepKind.REGION;
                        stepOffsets[step] = region.offset();
                        current = region.values();
                    }
                    case DictionaryVector dictionary -> {
                        stepKinds[step] = StepKind.DICTIONARY;
                        stepArrays[step] = dictionary.ids();
                        current = dictionary.values();
                    }
                    case RleVector rle -> {
                        stepKinds[step] = StepKind.RLE;
                        stepArrays[step] = runEnds(rle);
                        stepOffsets[step] = rle.counts().length;
                        current = rle.values();
                    }
                    default -> throw new AssertionError("Wrapper depth does not match vector shape");
                }
                step++;
            }

            return switch (current) {
                case I64Vector values when carrier == Carrier.LONG ->
                    new BoundVector(values.values(), false, stepArrays, stepOffsets, stepKinds);
                case I32Vector values when carrier == Carrier.LONG ->
                    new BoundVector(values.values(), true, stepArrays, stepOffsets, stepKinds);
                case BooleanVector values when carrier == Carrier.BOOLEAN ->
                    new BoundVector(values.values(), false, stepArrays, stepOffsets, stepKinds);
                default -> throw new IllegalArgumentException(
                        "No build-identity " + carrier.name().toLowerCase() + " binding for " + current.getClass().getSimpleName());
            };
        }
        catch (RuntimeException | Error e) {
            release(stepArrays, stepKinds, step);
            throw e;
        }
    }

    private static int wrapperDepth(Vector vector)
    {
        int depth = 0;
        Vector current = vector;
        while (true) {
            switch (current) {
                case RegionVector region -> current = region.values();
                case DictionaryVector dictionary -> current = dictionary.values();
                case RleVector rle -> current = rle.values();
                default -> {
                    return depth;
                }
            }
            depth++;
        }
    }

    private int[] runEnds(RleVector rle)
    {
        int[] ends = arrayPool.borrowInts(rle.counts().length);
        int end = 0;
        for (int run = 0; run < rle.counts().length; run++) {
            end += rle.counts()[run];
            ends[run] = end;
        }
        return ends;
    }

    private void release(BoundVector binding)
    {
        if (binding != null) {
            release(binding.stepArrays(), binding.stepKinds(), binding.stepKinds().length);
        }
    }

    private void release(int[][] stepArrays, StepKind[] stepKinds, int count)
    {
        for (int step = 0; step < count; step++) {
            if (stepKinds[step] == StepKind.RLE) {
                arrayPool.release(stepArrays[step]);
            }
        }
    }
}
