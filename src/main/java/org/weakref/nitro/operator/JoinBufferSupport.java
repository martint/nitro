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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.ArrayVector;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.MapVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.SelectedPositions;
import org.weakref.nitro.data.StructVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.builtin.VectorAccess;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import static java.lang.Math.toIntExact;

final class JoinBufferSupport
{
    private final Allocator allocator;
    private final Allocator.Context allocationContext;

    private static final boolean POSITION_MAPPING_CACHE =
            Boolean.parseBoolean(System.getProperty("nitro.join.positionMappingCache", "true"));
    private static final int POSITION_MAPPING_CACHE_SIZE = 8;
    private static final boolean FUSE_DICTIONARY_POSITION_COPIES =
            Boolean.parseBoolean(System.getProperty("nitro.join.fuseDictionaryPositionCopies", "true"));
    private static final boolean COPY_AND_COMPACT_RANGE_SELECTION =
            Boolean.parseBoolean(System.getProperty("nitro.join.copyAndCompactRangeSelection", "true"));
    private static final boolean PRESERVE_DICTIONARY_BINARY_POSITION_COPIES =
            Boolean.parseBoolean(System.getProperty("nitro.join.preserveDictionaryBinaryPositionCopies", "true"));
    private static final boolean COMPACT_ALL_FALSE_POSITION_COPIES =
            Boolean.parseBoolean(System.getProperty("nitro.join.compactAllFalsePositionCopies", "true"));
    private static final boolean INDEXED_VECTOR_TREE_TRAVERSAL =
            Boolean.parseBoolean(System.getProperty("nitro.allocator.indexedVectorTreeTraversal", "true"));

    JoinBufferSupport(Allocator allocator, Allocator.Context allocationContext)
    {
        this.allocator = allocator;
        this.allocationContext = allocationContext;
    }

    PrimitiveArrayPool primitiveArrays()
    {
        return allocator.primitiveArrays();
    }

    public Streams borrowStreams(Output output)
    {
        Streams.Builder streams = Streams.builder();
        if (output.hasValues()) {
            streams.put(Stream.VALUES, output.borrow(Stream.VALUES));
        }
        if (output.hasNulls()) {
            streams.put(Stream.NULLS, output.borrow(Stream.NULLS));
        }
        if (output.hasErrors()) {
            streams.put(Stream.ERRORS, output.borrow(Stream.ERRORS));
        }
        return streams.build();
    }

    Set<Vector> identities(Streams[] columns)
    {
        Set<Vector> identities = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Streams streams : columns) {
            if (streams == null) {
                continue;
            }
            for (Stream stream : Stream.values()) {
                Vector vector = streams.getOrNull(stream);
                if (vector != null) {
                    collectIdentities(vector, identities);
                }
            }
        }
        return identities;
    }

    void releaseUnreferenced(Streams[] columns, Set<Vector> retained)
    {
        if (columns == null) {
            return;
        }
        for (Streams streams : columns) {
            if (streams == null) {
                continue;
            }
            for (Stream stream : Stream.values()) {
                Vector vector = streams.getOrNull(stream);
                if (vector != null) {
                    allocator.releaseUnreferenced(allocationContext, vector, retained);
                }
            }
        }
    }

    private static void collectIdentities(Vector vector, Set<Vector> identities)
    {
        if (!identities.add(vector)) {
            return;
        }
        if (INDEXED_VECTOR_TREE_TRAVERSAL) {
            for (int index = 0; index < vector.childVectorCount(); index++) {
                collectIdentities(vector.childVector(index), identities);
            }
        }
        else {
            vector.forEachChildVector(child -> collectIdentities(child, identities));
        }
    }

    public Streams replicate(Streams existing, Streams input, int size, int start, int length, int position)
    {
        if (length == 1) {
            return copySinglePosition(existing, input, size, start, position);
        }
        return copyStreamsPositions(existing, input, repeatedPositions(position, length), start, size);
    }

    public Streams copyAndCompact(Output input, Mask mask, int maskStart, Streams existing, int outputStart, int copied, int size)
    {
        return copyAndCompact(input, mask, maskStart, existing, outputStart, copied, size, null, null);
    }

    public Streams copyAndCompact(Output input, Mask mask, int maskStart, Streams existing, int outputStart, int copied, int size, PositionBuffer positionBuffer)
    {
        return copyAndCompact(input, mask, maskStart, existing, outputStart, copied, size, positionBuffer, null);
    }

    public Streams copyAndCompact(
            Output input,
            Mask mask,
            int maskStart,
            Streams existing,
            int outputStart,
            int copied,
            int size,
            PositionBuffer positionBuffer,
            PositionMappingCache sharedPositionCache)
    {
        SelectedPositions rangePositions = COPY_AND_COMPACT_RANGE_SELECTION && mask.all()
                ? SelectedPositions.range(maskStart, copied)
                : null;
        int[] positions = null;
        PositionMappingCache positionCache = positionCache(input, sharedPositionCache);
        Streams.Builder result = Streams.builder();
        if (input.hasValues()) {
            Vector existingVector = existing != null ? existing.getOrNull(Stream.VALUES) : null;
            Vector source = input.borrow(Stream.VALUES);
            if (rangePositions != null && canCopyRangeWithoutMaterializing(source)) {
                result.put(Stream.VALUES, copyVectorPositions(existingVector, source, rangePositions, outputStart, size, false));
            }
            else {
                positions = positions != null ? positions : positions(positionBuffer, mask, maskStart, copied);
                result.put(Stream.VALUES, copyVectorPositions(existingVector, source, positions, 0, copied, outputStart, size, false, positionCache));
            }
        }
        if (hasConcreteStream(input, Stream.NULLS)) {
            Vector existingVector = existing != null ? existing.getOrNull(Stream.NULLS) : null;
            Vector source = input.borrow(Stream.NULLS);
            if (rangePositions != null && canCopyRangeWithoutMaterializing(source)) {
                result.put(Stream.NULLS, copyVectorPositions(existingVector, source, rangePositions, outputStart, size, false));
            }
            else {
                positions = positions != null ? positions : positions(positionBuffer, mask, maskStart, copied);
                result.put(Stream.NULLS, copyVectorPositions(existingVector, source, positions, 0, copied, outputStart, size, false, positionCache));
            }
        }
        if (hasConcreteStream(input, Stream.ERRORS)) {
            Vector existingVector = existing != null ? existing.getOrNull(Stream.ERRORS) : null;
            Vector source = input.borrow(Stream.ERRORS);
            if (rangePositions != null && canCopyRangeWithoutMaterializing(source)) {
                result.put(Stream.ERRORS, copyVectorPositions(existingVector, source, rangePositions, outputStart, size, false));
            }
            else {
                positions = positions != null ? positions : positions(positionBuffer, mask, maskStart, copied);
                result.put(Stream.ERRORS, copyVectorPositions(existingVector, source, positions, 0, copied, outputStart, size, false, positionCache));
            }
        }
        return result.build();
    }

    public Streams copyPosition(Output input, Streams existing, int position)
    {
        return copySinglePosition(input, existing, 1, 0, position);
    }

    public Streams copyPositions(Output input, Streams existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        return copyPositions(input, existing, sourcePositions, 0, sourceCount, outputStart, size);
    }

    public Streams copyPositions(Output input, Streams existing, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size)
    {
        return copyPositions(input, existing, sourcePositions, sourceStart, sourceCount, outputStart, size, false);
    }

    public Streams copyPositionsFresh(Output input, Streams existing, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size)
    {
        return copyPositions(input, existing, sourcePositions, sourceStart, sourceCount, outputStart, size, true, null);
    }

    Streams copyPositionsFresh(Output input, Streams existing, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size, PositionMappingCache positionCache)
    {
        return copyPositions(input, existing, sourcePositions, sourceStart, sourceCount, outputStart, size, true, positionCache);
    }

    private Streams copyPositions(Output input, Streams existing, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size, boolean assumeClearOutputRange)
    {
        return copyPositions(input, existing, sourcePositions, sourceStart, sourceCount, outputStart, size, assumeClearOutputRange, null);
    }

    private Streams copyPositions(Output input, Streams existing, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size, boolean assumeClearOutputRange, PositionMappingCache sharedPositionCache)
    {
        if (sourceCount == 1) {
            return copySinglePosition(input, existing, size, outputStart, sourcePositions[sourceStart], assumeClearOutputRange);
        }

        PositionMappingCache positionCache = positionCache(input, sharedPositionCache);
        if (input.isValuesOnly()) {
            Vector existingValues = existing != null ? existing.getOrNull(Stream.VALUES) : null;
            Vector copied = copyVectorPositions(existingValues, input.borrow(Stream.VALUES), sourcePositions, sourceStart, sourceCount, outputStart, size, false, positionCache);
            if (existing != null && copied == existingValues) {
                return existing;
            }
            if (existing != null && (existing.hasNulls() || existing.hasErrors())) {
                return existing.with(Stream.VALUES, copied);
            }
            return Streams.ofValues(copied);
        }

        if (existing != null) {
            Streams.Builder updated = null;
            boolean changed = false;
            if (input.hasValues()) {
                Vector existingVector = existing.getOrNull(Stream.VALUES);
                Vector copied = copyVectorPositions(existingVector, input.borrow(Stream.VALUES), sourcePositions, sourceStart, sourceCount, outputStart, size, false, positionCache);
                if (copied != existingVector) {
                    updated = ensureBuilder(updated, existing);
                    updated.put(Stream.VALUES, copied);
                    changed = true;
                }
            }
            if (hasConcreteStream(input, Stream.NULLS)) {
                Vector existingVector = existing.getOrNull(Stream.NULLS);
                Vector copied = copyVectorPositions(existingVector, input.borrow(Stream.NULLS), sourcePositions, sourceStart, sourceCount, outputStart, size, assumeClearOutputRange, positionCache);
                if (copied != existingVector) {
                    updated = ensureBuilder(updated, existing);
                    updated.put(Stream.NULLS, copied);
                    changed = true;
                }
            }
            if (hasConcreteStream(input, Stream.ERRORS)) {
                Vector existingVector = existing.getOrNull(Stream.ERRORS);
                Vector copied = copyVectorPositions(existingVector, input.borrow(Stream.ERRORS), sourcePositions, sourceStart, sourceCount, outputStart, size, assumeClearOutputRange, positionCache);
                if (copied != existingVector) {
                    updated = ensureBuilder(updated, existing);
                    updated.put(Stream.ERRORS, copied);
                    changed = true;
                }
            }
            return changed ? updated.build() : existing;
        }

        Streams.Builder result = Streams.builder();
        if (input.hasValues()) {
            result.put(Stream.VALUES, copyVectorPositions(null, input.borrow(Stream.VALUES), sourcePositions, sourceStart, sourceCount, outputStart, size, false, positionCache));
        }
        if (hasConcreteStream(input, Stream.NULLS)) {
            result.put(Stream.NULLS, copyVectorPositions(null, input.borrow(Stream.NULLS), sourcePositions, sourceStart, sourceCount, outputStart, size, assumeClearOutputRange, positionCache));
        }
        if (hasConcreteStream(input, Stream.ERRORS)) {
            result.put(Stream.ERRORS, copyVectorPositions(null, input.borrow(Stream.ERRORS), sourcePositions, sourceStart, sourceCount, outputStart, size, assumeClearOutputRange, positionCache));
        }
        return result.build();
    }

    public Streams copyPositions(Streams existing, Streams input, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        return copyPositions(existing, input, sourcePositions, 0, sourceCount, outputStart, size);
    }

    public Streams copyPositions(Streams existing, Streams input, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size)
    {
        return copyPositions(existing, input, sourcePositions, sourceStart, sourceCount, outputStart, size, false);
    }

    Streams copyPositions(Streams existing, Streams input, SelectedPositions sourcePositions, int outputStart, int size)
    {
        return copyPositions(existing, input, sourcePositions, outputStart, size, false);
    }

    public Streams copyPositionsFresh(Streams existing, Streams input, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size)
    {
        return copyPositions(existing, input, sourcePositions, sourceStart, sourceCount, outputStart, size, true, null);
    }

    Streams copyPositionsFresh(Streams existing, Streams input, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size, PositionMappingCache positionCache)
    {
        return copyPositions(existing, input, sourcePositions, sourceStart, sourceCount, outputStart, size, true, positionCache);
    }

    private Streams copyPositions(Streams existing, Streams input, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size, boolean assumeClearOutputRange)
    {
        return copyPositions(existing, input, sourcePositions, sourceStart, sourceCount, outputStart, size, assumeClearOutputRange, null);
    }

    private Streams copyPositions(Streams existing, Streams input, SelectedPositions sourcePositions, int outputStart, int size, boolean assumeClearOutputRange)
    {
        if (sourcePositions.count() == 1) {
            return copySinglePosition(existing, input, size, outputStart, sourcePositions.position(0), assumeClearOutputRange);
        }

        if (input.isValuesOnly()) {
            Vector existingValues = existing != null ? existing.getOrNull(Stream.VALUES) : null;
            Vector copied = copyVectorPositions(existingValues, input.values(), sourcePositions, outputStart, size, false);
            if (existing != null && copied == existingValues) {
                return existing;
            }
            if (existing != null && (existing.hasNulls() || existing.hasErrors())) {
                return existing.with(Stream.VALUES, copied);
            }
            return Streams.ofValues(copied);
        }

        if (existing != null) {
            Streams.Builder updated = null;
            boolean changed = false;
            if (input.hasValues()) {
                Vector existingVector = existing.getOrNull(Stream.VALUES);
                Vector copied = copyVectorPositions(existingVector, input.values(), sourcePositions, outputStart, size, false);
                if (copied != existingVector) {
                    updated = ensureBuilder(updated, existing);
                    updated.put(Stream.VALUES, copied);
                    changed = true;
                }
            }
            if (input.hasNulls()) {
                Vector existingVector = existing.getOrNull(Stream.NULLS);
                Vector copied = copyVectorPositions(existingVector, input.get(Stream.NULLS), sourcePositions, outputStart, size, assumeClearOutputRange);
                if (copied != existingVector) {
                    updated = ensureBuilder(updated, existing);
                    updated.put(Stream.NULLS, copied);
                    changed = true;
                }
            }
            if (input.hasErrors()) {
                Vector existingVector = existing.getOrNull(Stream.ERRORS);
                Vector copied = copyVectorPositions(existingVector, input.get(Stream.ERRORS), sourcePositions, outputStart, size, assumeClearOutputRange);
                if (copied != existingVector) {
                    updated = ensureBuilder(updated, existing);
                    updated.put(Stream.ERRORS, copied);
                    changed = true;
                }
            }
            return changed ? updated.build() : existing;
        }

        Streams.Builder result = Streams.builder();
        if (input.hasValues()) {
            result.put(Stream.VALUES, copyVectorPositions(null, input.values(), sourcePositions, outputStart, size, false));
        }
        if (input.hasNulls()) {
            result.put(Stream.NULLS, copyVectorPositions(null, input.get(Stream.NULLS), sourcePositions, outputStart, size, assumeClearOutputRange));
        }
        if (input.hasErrors()) {
            result.put(Stream.ERRORS, copyVectorPositions(null, input.get(Stream.ERRORS), sourcePositions, outputStart, size, assumeClearOutputRange));
        }
        return result.build();
    }

    private Streams copyPositions(Streams existing, Streams input, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size, boolean assumeClearOutputRange, PositionMappingCache sharedPositionCache)
    {
        if (sourceCount == 1) {
            return copySinglePosition(existing, input, size, outputStart, sourcePositions[sourceStart], assumeClearOutputRange);
        }

        PositionMappingCache positionCache = positionCache(input, sharedPositionCache);
        if (input.isValuesOnly()) {
            Vector existingValues = existing != null ? existing.getOrNull(Stream.VALUES) : null;
            Vector copied = copyVectorPositions(existingValues, input.values(), sourcePositions, sourceStart, sourceCount, outputStart, size, false, positionCache);
            if (existing != null && copied == existingValues) {
                return existing;
            }
            if (existing != null && (existing.hasNulls() || existing.hasErrors())) {
                return existing.with(Stream.VALUES, copied);
            }
            return Streams.ofValues(copied);
        }

        if (existing != null) {
            Streams.Builder updated = null;
            boolean changed = false;
            if (input.hasValues()) {
                Vector existingVector = existing.getOrNull(Stream.VALUES);
                Vector copied = copyVectorPositions(existingVector, input.values(), sourcePositions, sourceStart, sourceCount, outputStart, size, false, positionCache);
                if (copied != existingVector) {
                    updated = ensureBuilder(updated, existing);
                    updated.put(Stream.VALUES, copied);
                    changed = true;
                }
            }
            if (input.hasNulls()) {
                Vector existingVector = existing.getOrNull(Stream.NULLS);
                Vector copied = copyVectorPositions(existingVector, input.get(Stream.NULLS), sourcePositions, sourceStart, sourceCount, outputStart, size, assumeClearOutputRange, positionCache);
                if (copied != existingVector) {
                    updated = ensureBuilder(updated, existing);
                    updated.put(Stream.NULLS, copied);
                    changed = true;
                }
            }
            if (input.hasErrors()) {
                Vector existingVector = existing.getOrNull(Stream.ERRORS);
                Vector copied = copyVectorPositions(existingVector, input.get(Stream.ERRORS), sourcePositions, sourceStart, sourceCount, outputStart, size, assumeClearOutputRange, positionCache);
                if (copied != existingVector) {
                    updated = ensureBuilder(updated, existing);
                    updated.put(Stream.ERRORS, copied);
                    changed = true;
                }
            }
            return changed ? updated.build() : existing;
        }

        Streams.Builder result = Streams.builder();
        if (input.hasValues()) {
            result.put(Stream.VALUES, copyVectorPositions(null, input.values(), sourcePositions, sourceStart, sourceCount, outputStart, size, false, positionCache));
        }
        if (input.hasNulls()) {
            result.put(Stream.NULLS, copyVectorPositions(null, input.get(Stream.NULLS), sourcePositions, sourceStart, sourceCount, outputStart, size, assumeClearOutputRange, positionCache));
        }
        if (input.hasErrors()) {
            result.put(Stream.ERRORS, copyVectorPositions(null, input.get(Stream.ERRORS), sourcePositions, sourceStart, sourceCount, outputStart, size, assumeClearOutputRange, positionCache));
        }
        return result.build();
    }

    public Streams copySinglePosition(Output input, Streams existing, int size, int outputPosition, int sourcePosition)
    {
        return copySinglePosition(input, existing, size, outputPosition, sourcePosition, false);
    }

    public Streams copySinglePositionFresh(Output input, Streams existing, int size, int outputPosition, int sourcePosition)
    {
        return copySinglePosition(input, existing, size, outputPosition, sourcePosition, true);
    }

    private Streams copySinglePosition(Output input, Streams existing, int size, int outputPosition, int sourcePosition, boolean assumeClearOutputRange)
    {
        Streams specialized = input.copySinglePosition(existing, sourcePosition, outputPosition, size);
        if (specialized != null) {
            return specialized;
        }

        if (input.isValuesOnly()) {
            Vector existingValues = existing != null ? existing.getOrNull(Stream.VALUES) : null;
            Vector copied = copyVectorSinglePosition(existingValues, input.borrow(Stream.VALUES), sourcePosition, outputPosition, size);
            if (existing != null && copied == existingValues) {
                return existing;
            }
            if (existing != null && (existing.hasNulls() || existing.hasErrors())) {
                return existing.with(Stream.VALUES, copied);
            }
            return Streams.ofValues(copied);
        }

        if (existing != null) {
            Streams.Builder updated = null;
            boolean changed = false;
            if (input.hasValues()) {
                Vector existingValues = existing.getOrNull(Stream.VALUES);
                Vector copied = copyVectorSinglePosition(existingValues, input.borrow(Stream.VALUES), sourcePosition, outputPosition, size);
                if (copied != existingValues) {
                    updated = ensureBuilder(updated, existing);
                    updated.put(Stream.VALUES, copied);
                    changed = true;
                }
            }
            if (hasConcreteStream(input, Stream.NULLS)) {
                Vector existingNulls = existing.getOrNull(Stream.NULLS);
                Vector copied = copyVectorSinglePosition(existingNulls, input.borrow(Stream.NULLS), sourcePosition, outputPosition, size, assumeClearOutputRange);
                if (copied != existingNulls) {
                    updated = ensureBuilder(updated, existing);
                    updated.put(Stream.NULLS, copied);
                    changed = true;
                }
            }
            if (hasConcreteStream(input, Stream.ERRORS)) {
                Vector existingErrors = existing.getOrNull(Stream.ERRORS);
                Vector copied = copyVectorSinglePosition(existingErrors, input.borrow(Stream.ERRORS), sourcePosition, outputPosition, size, assumeClearOutputRange);
                if (copied != existingErrors) {
                    updated = ensureBuilder(updated, existing);
                    updated.put(Stream.ERRORS, copied);
                    changed = true;
                }
            }
            return changed ? updated.build() : existing;
        }

        Streams.Builder result = Streams.builder();
        if (input.hasValues()) {
            result.put(Stream.VALUES, copyVectorSinglePosition(null, input.borrow(Stream.VALUES), sourcePosition, outputPosition, size));
        }
        if (hasConcreteStream(input, Stream.NULLS)) {
            result.put(Stream.NULLS, copyVectorSinglePosition(null, input.borrow(Stream.NULLS), sourcePosition, outputPosition, size, assumeClearOutputRange));
        }
        if (hasConcreteStream(input, Stream.ERRORS)) {
            result.put(Stream.ERRORS, copyVectorSinglePosition(null, input.borrow(Stream.ERRORS), sourcePosition, outputPosition, size, assumeClearOutputRange));
        }
        return result.build();
    }

    private static boolean hasConcreteStream(Output output, Stream stream)
    {
        return output.has(stream) && !output.isKnownAllFalse(stream);
    }

    public Streams copySinglePosition(Streams existing, Streams input, int size, int outputPosition, int sourcePosition)
    {
        return copySinglePosition(existing, input, size, outputPosition, sourcePosition, false);
    }

    public Streams copySinglePositionFresh(Streams existing, Streams input, int size, int outputPosition, int sourcePosition)
    {
        return copySinglePosition(existing, input, size, outputPosition, sourcePosition, true);
    }

    private Streams copySinglePosition(Streams existing, Streams input, int size, int outputPosition, int sourcePosition, boolean assumeClearOutputRange)
    {
        if (input.isValuesOnly()) {
            Vector existingValues = existing != null ? existing.getOrNull(Stream.VALUES) : null;
            Vector copied = copyVectorSinglePosition(existingValues, input.values(), sourcePosition, outputPosition, size);
            if (existing != null && copied == existingValues) {
                return existing;
            }
            if (existing != null && (existing.hasNulls() || existing.hasErrors())) {
                return existing.with(Stream.VALUES, copied);
            }
            return Streams.ofValues(copied);
        }

        if (existing != null) {
            Streams.Builder updated = null;
            boolean changed = false;
            if (input.hasValues()) {
                Vector existingValues = existing.getOrNull(Stream.VALUES);
                Vector copied = copyVectorSinglePosition(existingValues, input.values(), sourcePosition, outputPosition, size);
                if (copied != existingValues) {
                    updated = ensureBuilder(updated, existing);
                    updated.put(Stream.VALUES, copied);
                    changed = true;
                }
            }
            if (input.hasNulls()) {
                Vector existingNulls = existing.getOrNull(Stream.NULLS);
                Vector copied = copyVectorSinglePosition(existingNulls, input.get(Stream.NULLS), sourcePosition, outputPosition, size, assumeClearOutputRange);
                if (copied != existingNulls) {
                    updated = ensureBuilder(updated, existing);
                    updated.put(Stream.NULLS, copied);
                    changed = true;
                }
            }
            if (input.hasErrors()) {
                Vector existingErrors = existing.getOrNull(Stream.ERRORS);
                Vector copied = copyVectorSinglePosition(existingErrors, input.get(Stream.ERRORS), sourcePosition, outputPosition, size, assumeClearOutputRange);
                if (copied != existingErrors) {
                    updated = ensureBuilder(updated, existing);
                    updated.put(Stream.ERRORS, copied);
                    changed = true;
                }
            }
            return changed ? updated.build() : existing;
        }

        Streams.Builder result = Streams.builder();
        if (input.hasValues()) {
            result.put(Stream.VALUES, copyVectorSinglePosition(null, input.values(), sourcePosition, outputPosition, size));
        }
        if (input.hasNulls()) {
            result.put(Stream.NULLS, copyVectorSinglePosition(null, input.get(Stream.NULLS), sourcePosition, outputPosition, size, assumeClearOutputRange));
        }
        if (input.hasErrors()) {
            result.put(Stream.ERRORS, copyVectorSinglePosition(null, input.get(Stream.ERRORS), sourcePosition, outputPosition, size, assumeClearOutputRange));
        }
        return result.build();
    }

    public Streams emptyLike(Streams schema)
    {
        Streams.Builder empty = Streams.builder();
        if (schema.hasValues()) {
            empty.put(Stream.VALUES, emptyVector(schema.values()));
        }
        if (schema.hasNulls()) {
            empty.put(Stream.NULLS, emptyVector(schema.get(Stream.NULLS)));
        }
        if (schema.hasErrors()) {
            empty.put(Stream.ERRORS, emptyVector(schema.get(Stream.ERRORS)));
        }
        return empty.build();
    }

    public Vector copyStreamVector(Streams streams, Stream stream)
    {
        return allocator.copyVector(allocationContext, streams.get(stream));
    }

    public Vector materializeStream(Vector sample, Vector[] rows)
    {
        return sample.materializeRows(allocator, allocationContext, rows);
    }

    PositionMappingCache newPositionMappingCache()
    {
        return POSITION_MAPPING_CACHE ? new PositionMappingCache(allocator.primitiveArrays(), false) : null;
    }

    PositionMappingCache newRecyclingPositionMappingCache()
    {
        return POSITION_MAPPING_CACHE ? new PositionMappingCache(allocator.primitiveArrays(), true) : null;
    }

    private Streams copyStreamsPositions(Streams existing, Streams source, int[] sourcePositions, int outputStart, int size)
    {
        return copyPositions(existing, source, sourcePositions, sourcePositions.length, outputStart, size);
    }

    private Vector copyVectorPositions(Vector existing, Vector source, int[] sourcePositions, int outputStart, int size)
    {
        return copyVectorPositions(existing, source, sourcePositions, sourcePositions.length, outputStart, size);
    }

    private Vector copyVectorPositions(Vector existing, Vector source, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        return copyVectorPositions(existing, source, sourcePositions, 0, sourceCount, outputStart, size);
    }

    private Vector copyVectorPositions(Vector existing, Vector source, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size)
    {
        return copyVectorPositions(existing, source, sourcePositions, sourceStart, sourceCount, outputStart, size, false);
    }

    private Vector copyVectorPositions(Vector existing, Vector source, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size, boolean assumeClearOutputRange)
    {
        return copyVectorPositions(existing, source, sourcePositions, sourceStart, sourceCount, outputStart, size, assumeClearOutputRange, null);
    }

    private Vector copyVectorPositions(Vector existing, Vector source, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size, boolean assumeClearOutputRange, PositionMappingCache positionCache)
    {
        Vector compactFalse = compactAllFalseCopy(existing, source, outputStart, sourceCount, size);
        if (compactFalse != null) {
            return compactFalse;
        }
        if (source instanceof DictionaryVector dictionaryValues) {
            Vector fused = copyDictionaryPositions(existing, dictionaryValues, sourcePositions, sourceStart, sourceCount, outputStart, size, assumeClearOutputRange);
            if (fused != null) {
                return fused;
            }
        }
        existing = compatibleExisting(existing, source);
        switch (source) {
            case DictionaryVector dictionaryValues -> {
                return copyVectorPositions(existing, dictionaryValues.values(), dictionaryPositions(positionCache, dictionaryValues.ids(), sourcePositions, sourceStart, sourceCount), 0, sourceCount, outputStart, size, assumeClearOutputRange, positionCache);
            }
            case RleVector rleValues -> {
                return copyVectorPositions(existing, rleValues.values(), rlePositions(positionCache, rleValues, sourcePositions, sourceStart, sourceCount), 0, sourceCount, outputStart, size, assumeClearOutputRange, positionCache);
            }
            case I64Vector longValues -> {
                return copyLongPositions(longValues, existing, sourcePositions, sourceStart, sourceCount, outputStart, size);
            }
            case I32Vector intValues -> {
                return copyIntPositions(intValues, existing, sourcePositions, sourceStart, sourceCount, outputStart, size);
            }
            case BooleanVector booleanValues -> {
                return assumeClearOutputRange ?
                        copyBooleanPositionsFresh(booleanValues, existing, sourcePositions, sourceStart, sourceCount, outputStart, size) :
                        copyBooleanPositions(booleanValues, existing, sourcePositions, sourceStart, sourceCount, outputStart, size);
            }
            case F64Vector doubleValues -> {
                return copyDoublePositions(doubleValues, existing, sourcePositions, sourceStart, sourceCount, outputStart, size);
            }
            case BinaryVector binaryValues -> {
                return copyBinaryPositions(binaryValues, existing, sourcePositions, sourceStart, sourceCount, outputStart, size);
            }
            case ArrayVector arrayValues -> {
                return copyArrayPositions(arrayValues, existing, sourcePositions, sourceCount, outputStart, size);
            }
            case MapVector mapValues -> {
                return copyMapPositions(mapValues, existing, sourcePositions, sourceCount, outputStart, size);
            }
            case StructVector structValues -> {
                return copyStructPositions(structValues, existing, sourcePositions, sourceCount, outputStart, size);
            }
            default -> {}
        }
        if (sourceStart == 0) {
            return source.copyPositionsInto(allocator, allocationContext, existing, sourcePositions, sourceCount, outputStart, size);
        }
        return source.copyPositionsInto(allocator, allocationContext, existing, Arrays.copyOfRange(sourcePositions, sourceStart, sourceStart + sourceCount), sourceCount, outputStart, size);
    }

    private Vector copyVectorPositions(Vector existing, Vector source, SelectedPositions sourcePositions, int outputStart, int size, boolean assumeClearOutputRange)
    {
        Vector compactFalse = compactAllFalseCopy(existing, source, outputStart, sourcePositions.count(), size);
        if (compactFalse != null) {
            return compactFalse;
        }
        if (source instanceof DictionaryVector dictionaryValues) {
            int[] positions = sourcePositions.backingArrayOrNull();
            if (positions != null) {
                Vector fused = copyDictionaryPositions(existing, dictionaryValues, positions, sourcePositions.backingArrayOffset(), sourcePositions.count(), outputStart, size, assumeClearOutputRange);
                if (fused != null) {
                    return fused;
                }
            }
        }
        existing = compatibleExisting(existing, source);
        switch (source) {
            case DictionaryVector dictionaryValues -> {
                int[] positions = sourcePositions.backingArrayOrNull();
                if (positions != null) {
                    int count = sourcePositions.count();
                    return copyVectorPositions(existing, dictionaryValues.values(), dictionaryPositions(dictionaryValues.ids(), positions, sourcePositions.backingArrayOffset(), count), 0, count, outputStart, size, assumeClearOutputRange);
                }
                return copyVectorPositions(existing, dictionaryValues.values(), SelectedPositions.map(dictionaryValues.ids(), sourcePositions), outputStart, size, assumeClearOutputRange);
            }
            case RleVector rleValues -> {
                int[] positions = sourcePositions.backingArrayOrNull();
                if (positions != null) {
                    int count = sourcePositions.count();
                    return copyVectorPositions(existing, rleValues.values(), rlePositions(rleValues, positions, sourcePositions.backingArrayOffset(), count), 0, count, outputStart, size, assumeClearOutputRange);
                }
                return copyVectorPositions(existing, rleValues.values(), SelectedPositions.map(sourcePositions, rleValues::runIndex), outputStart, size, assumeClearOutputRange);
            }
            case I64Vector longValues -> {
                return copyLongPositions(longValues, existing, sourcePositions, outputStart, size);
            }
            case I32Vector intValues -> {
                return copyIntPositions(intValues, existing, sourcePositions, outputStart, size);
            }
            case BooleanVector booleanValues -> {
                return assumeClearOutputRange ?
                        copyBooleanPositionsFresh(booleanValues, existing, sourcePositions, outputStart, size) :
                        copyBooleanPositions(booleanValues, existing, sourcePositions, outputStart, size);
            }
            case F64Vector doubleValues -> {
                return copyDoublePositions(doubleValues, existing, sourcePositions, outputStart, size);
            }
            case BinaryVector _ -> {
                return source.copySelectedPositionsInto(allocator, allocationContext, existing, sourcePositions, outputStart, size);
            }
            default -> {}
        }
        return source.copySelectedPositionsInto(allocator, allocationContext, existing, sourcePositions, outputStart, size);
    }

    private Vector compactAllFalseCopy(Vector existing, Vector source, int outputStart, int count, int size)
    {
        if (!COMPACT_ALL_FALSE_POSITION_COPIES || !isAllFalseBoolean(source)) {
            return null;
        }
        if (existing == null || (!(existing instanceof BooleanVector) && isAllFalseBoolean(existing) && existing.length() < size)) {
            BooleanVector sentinel = allocator.allocate(allocationContext, BooleanVector.class, 1, BooleanVector::new);
            sentinel.markAllFalse();
            return allocator.allocateSingleRunRle(allocationContext, size, sentinel);
        }
        // Do not call BooleanVector.isAllFalse() on the mutable output under construction: a later chunk can write
        // true bits directly into the same backing array, while that method's cache is valid only after publication.
        if (!(existing instanceof BooleanVector) && isAllFalseBoolean(existing)) {
            return existing;
        }
        if (existing instanceof BooleanVector booleans) {
            BooleanVector output = ensureBooleanCapacity(booleans, size);
            Arrays.fill(output.values(), outputStart, outputStart + count, false);
            return output;
        }
        return null;
    }

    private static boolean isAllFalseBoolean(Vector vector)
    {
        return switch (vector) {
            case BooleanVector booleans -> booleans.isAllFalse();
            case DictionaryVector dictionary -> isAllFalseBoolean(dictionary.values());
            case RleVector rle -> isAllFalseBoolean(rle.values());
            default -> false;
        };
    }

    boolean canCopyRangeWithoutMaterializing(Streams input)
    {
        if (input.hasValues() && !canCopyRangeWithoutMaterializing(input.values())) {
            return false;
        }
        if (input.hasNulls() && !canCopyRangeWithoutMaterializing(input.get(Stream.NULLS))) {
            return false;
        }
        if (input.hasErrors() && !canCopyRangeWithoutMaterializing(input.get(Stream.ERRORS))) {
            return false;
        }
        return true;
    }

    private static boolean canCopyRangeWithoutMaterializing(Vector source)
    {
        return switch (source) {
            case I64Vector _, I32Vector _, F64Vector _, BooleanVector _ -> true;
            case DictionaryVector dictionary -> canCopyRangeWithoutMaterializing(dictionary.values());
            case RleVector rle -> canCopyRangeWithoutMaterializing(rle.values());
            default -> false;
        };
    }

    private Vector copyVectorSinglePosition(Vector existing, Vector source, int sourcePosition, int outputPosition, int size)
    {
        return copyVectorSinglePosition(existing, source, sourcePosition, outputPosition, size, false);
    }

    private Vector copyVectorSinglePosition(Vector existing, Vector source, int sourcePosition, int outputPosition, int size, boolean assumeClearOutputRange)
    {
        existing = compatibleExisting(existing, source);
        switch (source) {
            case DictionaryVector dictionaryValues -> {
                return copyVectorSinglePosition(existing, dictionaryValues.values(), dictionaryValues.ids()[sourcePosition], outputPosition, size, assumeClearOutputRange);
            }
            case RleVector rleValues -> {
                return copyVectorSinglePosition(existing, rleValues.values(), rlePosition(rleValues, sourcePosition), outputPosition, size, assumeClearOutputRange);
            }
            case I64Vector longValues -> {
                return copyLongSinglePosition(longValues, existing, sourcePosition, outputPosition, size);
            }
            case I32Vector intValues -> {
                return copyIntSinglePosition(intValues, existing, sourcePosition, outputPosition, size);
            }
            case BooleanVector booleanValues -> {
                return assumeClearOutputRange ?
                        copyBooleanSinglePositionFresh(booleanValues, existing, sourcePosition, outputPosition, size) :
                        copyBooleanSinglePosition(booleanValues, existing, sourcePosition, outputPosition, size);
            }
            case F64Vector doubleValues -> {
                return copyDoubleSinglePosition(doubleValues, existing, sourcePosition, outputPosition, size);
            }
            case BinaryVector binaryValues -> {
                return copyBinarySinglePosition(binaryValues, existing, sourcePosition, outputPosition, size);
            }
            case ArrayVector arrayValues -> {
                return copyArraySinglePosition(arrayValues, existing, sourcePosition, outputPosition, size);
            }
            case MapVector mapValues -> {
                return copyMapSinglePosition(mapValues, existing, sourcePosition, outputPosition, size);
            }
            case StructVector structValues -> {
                return copyStructSinglePosition(structValues, existing, sourcePosition, outputPosition, size);
            }
            default -> {}
        }
        return source.copySinglePositionInto(allocator, allocationContext, existing, sourcePosition, outputPosition, size);
    }

    private Vector copyDictionaryPositions(Vector existing, DictionaryVector source, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size, boolean assumeClearOutputRange)
    {
        if (!FUSE_DICTIONARY_POSITION_COPIES) {
            return null;
        }

        return switch (source.values()) {
            case I64Vector values -> copyDictionaryLongPositions(values, source.ids(), existing, sourcePositions, sourceStart, sourceCount, outputStart, size);
            case I32Vector values -> copyDictionaryIntPositions(values, source.ids(), existing, sourcePositions, sourceStart, sourceCount, outputStart, size);
            case BooleanVector values -> assumeClearOutputRange ?
                    copyDictionaryBooleanPositionsFresh(values, source.ids(), existing, sourcePositions, sourceStart, sourceCount, outputStart, size) :
                    copyDictionaryBooleanPositions(values, source.ids(), existing, sourcePositions, sourceStart, sourceCount, outputStart, size);
            case F64Vector values -> copyDictionaryDoublePositions(values, source.ids(), existing, sourcePositions, sourceStart, sourceCount, outputStart, size);
            case BinaryVector values -> copyDictionaryBinaryPositions(values, source.ids(), existing, sourcePositions, sourceStart, sourceCount, outputStart, size);
            default -> null;
        };
    }

    private I64Vector copyDictionaryLongPositions(I64Vector source, int[] ids, Vector existing, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size)
    {
        if (existing != null && !(existing instanceof I64Vector)) {
            return null;
        }
        I64Vector output = ensureLongCapacity(existing instanceof I64Vector vector ? vector : null, size);
        long[] sourceValues = source.values();
        long[] outputValues = output.values();
        for (int index = 0; index < sourceCount; index++) {
            outputValues[outputStart + index] = sourceValues[ids[sourcePositions[sourceStart + index]]];
        }
        return output;
    }

    private I32Vector copyDictionaryIntPositions(I32Vector source, int[] ids, Vector existing, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size)
    {
        if (existing != null && !(existing instanceof I32Vector)) {
            return null;
        }
        I32Vector output = ensureIntCapacity(existing instanceof I32Vector vector ? vector : null, size);
        int[] sourceValues = source.values();
        int[] outputValues = output.values();
        for (int index = 0; index < sourceCount; index++) {
            outputValues[outputStart + index] = sourceValues[ids[sourcePositions[sourceStart + index]]];
        }
        return output;
    }

    private BooleanVector copyDictionaryBooleanPositions(BooleanVector source, int[] ids, Vector existing, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size)
    {
        if (existing != null && !(existing instanceof BooleanVector)) {
            return null;
        }
        BooleanVector output = ensureBooleanCapacity(existing instanceof BooleanVector vector ? vector : null, size);
        boolean[] sourceValues = source.values();
        boolean[] outputValues = output.values();
        for (int index = 0; index < sourceCount; index++) {
            outputValues[outputStart + index] = sourceValues[ids[sourcePositions[sourceStart + index]]];
        }
        return output;
    }

    private BooleanVector copyDictionaryBooleanPositionsFresh(BooleanVector source, int[] ids, Vector existing, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size)
    {
        if (existing != null && !(existing instanceof BooleanVector)) {
            return null;
        }
        BooleanVector output = ensureBooleanCapacity(existing instanceof BooleanVector vector ? vector : null, size);
        boolean[] sourceValues = source.values();
        boolean[] outputValues = output.values();
        for (int index = 0; index < sourceCount; index++) {
            if (sourceValues[ids[sourcePositions[sourceStart + index]]]) {
                outputValues[outputStart + index] = true;
            }
        }
        return output;
    }

    private F64Vector copyDictionaryDoublePositions(F64Vector source, int[] ids, Vector existing, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size)
    {
        if (existing != null && !(existing instanceof F64Vector)) {
            return null;
        }
        F64Vector output = ensureDoubleCapacity(existing instanceof F64Vector vector ? vector : null, size);
        double[] sourceValues = source.values();
        double[] outputValues = output.values();
        for (int index = 0; index < sourceCount; index++) {
            outputValues[outputStart + index] = sourceValues[ids[sourcePositions[sourceStart + index]]];
        }
        return output;
    }

    private Vector copyDictionaryBinaryPositions(BinaryVector source, int[] ids, Vector existing, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size)
    {
        if (existing != null && !(existing instanceof BinaryVector)) {
            return null;
        }
        if (PRESERVE_DICTIONARY_BINARY_POSITION_COPIES && existing == null && outputStart == 0 && sourceCount == size) {
            Vector encoded = copyCompactDictionaryBinaryPositions(source, ids, sourcePositions, sourceStart, sourceCount);
            if (encoded != null) {
                return encoded;
            }
        }

        int usedBytes = binaryWriteOffset(existing, outputStart);
        int byteCapacity = usedBytes;
        int[] sourceOffsets = source.offsets();
        for (int index = 0; index < sourceCount; index++) {
            int sourcePosition = ids[sourcePositions[sourceStart + index]];
            byteCapacity += sourceOffsets[sourcePosition + 1] - sourceOffsets[sourcePosition];
        }

        int requestedCapacity = byteCapacity;
        if (existing == null && outputStart == 0 && size > sourceCount) {
            requestedCapacity = Math.max(byteCapacity, estimatedBinaryCapacity(source, size));
        }

        BinaryVector output = BinaryVector.allocateOrGrow(allocator, allocationContext, existing instanceof BinaryVector vector ? vector : null, size, requestedCapacity, usedBytes);
        if (outputStart == 0) {
            Arrays.fill(output.offsets(), 0);
            output.clearTraits();
            output.addTraits(source.traits());
        }
        else {
            output.addTraits(source.traits());
        }

        int[] outputOffsets = output.offsets();
        byte[] outputData = output.data();
        byte[] sourceData = source.data();
        int currentOffset = prepareBinaryWriteOffset(output, outputStart);
        for (int index = 0; index < sourceCount; index++) {
            int targetPosition = outputStart + index;
            int sourcePosition = ids[sourcePositions[sourceStart + index]];
            int startOffset = sourceOffsets[sourcePosition];
            int endOffset = sourceOffsets[sourcePosition + 1];
            int length = endOffset - startOffset;
            outputOffsets[targetPosition] = currentOffset;
            if (length == 0) {
                outputOffsets[targetPosition + 1] = currentOffset;
            }
            else {
                System.arraycopy(sourceData, startOffset, outputData, currentOffset, length);
                currentOffset += length;
                outputOffsets[targetPosition + 1] = currentOffset;
            }
        }
        return output;
    }

    private Vector copyCompactDictionaryBinaryPositions(BinaryVector source, int[] ids, int[] sourcePositions, int sourceStart, int sourceCount)
    {
        if (sourceCount == 0) {
            return BinaryVector.allocate(allocator, allocationContext, 0, 0);
        }

        int dictionarySize = source.length();
        int[] remap = new int[dictionarySize];
        Arrays.fill(remap, -1);
        int[] dictionaryPositions = new int[Math.min(sourceCount, dictionarySize)];
        int[] outputIds = new int[sourceCount];
        int[] sourceOffsets = source.offsets();
        int selectedBytes = 0;
        int dictionaryBytes = 0;
        int uniqueCount = 0;
        for (int index = 0; index < sourceCount; index++) {
            int sourcePosition = ids[sourcePositions[sourceStart + index]];
            int length = sourceOffsets[sourcePosition + 1] - sourceOffsets[sourcePosition];
            selectedBytes += length;
            int mapped = remap[sourcePosition];
            if (mapped < 0) {
                mapped = uniqueCount;
                remap[sourcePosition] = mapped;
                dictionaryPositions[uniqueCount++] = sourcePosition;
                dictionaryBytes += length;
            }
            outputIds[index] = mapped;
        }

        if (dictionaryBytes + (long) uniqueCount * Integer.BYTES >= selectedBytes) {
            return null;
        }

        BinaryVector copiedValues = (BinaryVector) source.copy(allocator, allocationContext, Arrays.copyOf(dictionaryPositions, uniqueCount));
        return DictionaryVector.wrap(outputIds, sourceCount, copiedValues);
    }

    private Vector compatibleExisting(Vector existing, Vector source)
    {
        if (!(existing instanceof DictionaryVector dictionary)) {
            return existing;
        }
        if (source instanceof DictionaryVector sourceDictionary &&
                sourceDictionary.values() == dictionary.values() &&
                dictionary.length() == source.length()) {
            return existing;
        }
        return dictionary.values().copy(allocator, allocationContext, dictionary.ids());
    }

    private I64Vector copyLongPositions(I64Vector source, Vector existing, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size)
    {
        I64Vector output = ensureLongCapacity(existing instanceof I64Vector vector ? vector : null, size);
        for (int index = 0; index < sourceCount; index++) {
            output.values()[outputStart + index] = source.values()[sourcePositions[sourceStart + index]];
        }
        return output;
    }

    private I32Vector copyIntPositions(I32Vector source, Vector existing, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size)
    {
        I32Vector output = ensureIntCapacity(existing instanceof I32Vector vector ? vector : null, size);
        for (int index = 0; index < sourceCount; index++) {
            output.values()[outputStart + index] = source.values()[sourcePositions[sourceStart + index]];
        }
        return output;
    }

    private I64Vector copyLongPositions(I64Vector source, Vector existing, SelectedPositions sourcePositions, int outputStart, int size)
    {
        I64Vector output = ensureLongCapacity(existing instanceof I64Vector vector ? vector : null, size);
        int[] positions = sourcePositions.backingArrayOrNull();
        if (positions != null) {
            int offset = sourcePositions.backingArrayOffset();
            for (int index = 0; index < sourcePositions.count(); index++) {
                output.values()[outputStart + index] = source.values()[positions[offset + index]];
            }
            return output;
        }
        for (int index = 0; index < sourcePositions.count(); index++) {
            output.values()[outputStart + index] = source.values()[sourcePositions.position(index)];
        }
        return output;
    }

    private I32Vector copyIntPositions(I32Vector source, Vector existing, SelectedPositions sourcePositions, int outputStart, int size)
    {
        I32Vector output = ensureIntCapacity(existing instanceof I32Vector vector ? vector : null, size);
        int[] positions = sourcePositions.backingArrayOrNull();
        if (positions != null) {
            int offset = sourcePositions.backingArrayOffset();
            for (int index = 0; index < sourcePositions.count(); index++) {
                output.values()[outputStart + index] = source.values()[positions[offset + index]];
            }
            return output;
        }
        for (int index = 0; index < sourcePositions.count(); index++) {
            output.values()[outputStart + index] = source.values()[sourcePositions.position(index)];
        }
        return output;
    }

    private I32Vector copyIntSinglePosition(I32Vector source, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        I32Vector output = ensureIntCapacity(existing instanceof I32Vector vector ? vector : null, size);
        output.values()[outputPosition] = source.values()[sourcePosition];
        return output;
    }

    private I64Vector copyLongSinglePosition(I64Vector source, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        I64Vector output = ensureLongCapacity(existing instanceof I64Vector vector ? vector : null, size);
        output.values()[outputPosition] = source.values()[sourcePosition];
        return output;
    }

    private BooleanVector copyBooleanPositions(BooleanVector source, Vector existing, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size)
    {
        BooleanVector output = ensureBooleanCapacity(existing instanceof BooleanVector vector ? vector : null, size);
        for (int index = 0; index < sourceCount; index++) {
            output.values()[outputStart + index] = source.values()[sourcePositions[sourceStart + index]];
        }
        return output;
    }

    private BooleanVector copyBooleanPositionsFresh(BooleanVector source, Vector existing, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size)
    {
        BooleanVector output = ensureBooleanCapacity(existing instanceof BooleanVector vector ? vector : null, size);
        boolean[] sourceValues = source.values();
        boolean[] outputValues = output.values();
        if (sourceCount >= 32 && sampleMostlyTrue(sourceValues, sourcePositions, sourceStart, sourceCount)) {
            for (int index = 0; index < sourceCount; index++) {
                outputValues[outputStart + index] = sourceValues[sourcePositions[sourceStart + index]];
            }
            return output;
        }
        for (int index = 0; index < sourceCount; index++) {
            if (sourceValues[sourcePositions[sourceStart + index]]) {
                outputValues[outputStart + index] = true;
            }
        }
        return output;
    }

    private BooleanVector copyBooleanPositions(BooleanVector source, Vector existing, SelectedPositions sourcePositions, int outputStart, int size)
    {
        BooleanVector output = ensureBooleanCapacity(existing instanceof BooleanVector vector ? vector : null, size);
        int[] positions = sourcePositions.backingArrayOrNull();
        if (positions != null) {
            int offset = sourcePositions.backingArrayOffset();
            for (int index = 0; index < sourcePositions.count(); index++) {
                output.values()[outputStart + index] = source.values()[positions[offset + index]];
            }
            return output;
        }
        for (int index = 0; index < sourcePositions.count(); index++) {
            output.values()[outputStart + index] = source.values()[sourcePositions.position(index)];
        }
        return output;
    }

    private BooleanVector copyBooleanPositionsFresh(BooleanVector source, Vector existing, SelectedPositions sourcePositions, int outputStart, int size)
    {
        BooleanVector output = ensureBooleanCapacity(existing instanceof BooleanVector vector ? vector : null, size);
        boolean[] sourceValues = source.values();
        boolean[] outputValues = output.values();
        int sourceCount = sourcePositions.count();
        int[] positions = sourcePositions.backingArrayOrNull();
        int offset = sourcePositions.backingArrayOffset();
        if (sourceCount >= 32 && sampleMostlyTrue(sourceValues, sourcePositions)) {
            if (positions != null) {
                for (int index = 0; index < sourceCount; index++) {
                    outputValues[outputStart + index] = sourceValues[positions[offset + index]];
                }
                return output;
            }
            for (int index = 0; index < sourceCount; index++) {
                outputValues[outputStart + index] = sourceValues[sourcePositions.position(index)];
            }
            return output;
        }
        if (positions != null) {
            for (int index = 0; index < sourceCount; index++) {
                if (sourceValues[positions[offset + index]]) {
                    outputValues[outputStart + index] = true;
                }
            }
            return output;
        }
        for (int index = 0; index < sourceCount; index++) {
            if (sourceValues[sourcePositions.position(index)]) {
                outputValues[outputStart + index] = true;
            }
        }
        return output;
    }

    private boolean sampleMostlyTrue(boolean[] sourceValues, int[] sourcePositions, int sourceStart, int sourceCount)
    {
        int sampleCount = Math.min(sourceCount, 16);
        int trueCount = 0;
        for (int index = 0; index < sampleCount; index++) {
            if (sourceValues[sourcePositions[sourceStart + index]]) {
                trueCount++;
            }
        }
        return trueCount * 8 >= sampleCount * 7;
    }

    private boolean sampleMostlyTrue(boolean[] sourceValues, SelectedPositions sourcePositions)
    {
        int sampleCount = Math.min(sourcePositions.count(), 16);
        int trueCount = 0;
        int[] positions = sourcePositions.backingArrayOrNull();
        int offset = sourcePositions.backingArrayOffset();
        if (positions != null) {
            for (int index = 0; index < sampleCount; index++) {
                if (sourceValues[positions[offset + index]]) {
                    trueCount++;
                }
            }
            return trueCount * 8 >= sampleCount * 7;
        }
        for (int index = 0; index < sampleCount; index++) {
            if (sourceValues[sourcePositions.position(index)]) {
                trueCount++;
            }
        }
        return trueCount * 8 >= sampleCount * 7;
    }

    private BooleanVector copyBooleanSinglePosition(BooleanVector source, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        BooleanVector output = ensureBooleanCapacity(existing instanceof BooleanVector vector ? vector : null, size);
        output.values()[outputPosition] = source.values()[sourcePosition];
        return output;
    }

    private BooleanVector copyBooleanSinglePositionFresh(BooleanVector source, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        BooleanVector output = ensureBooleanCapacity(existing instanceof BooleanVector vector ? vector : null, size);
        if (source.values()[sourcePosition]) {
            output.values()[outputPosition] = true;
        }
        return output;
    }

    private F64Vector copyDoublePositions(F64Vector source, Vector existing, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size)
    {
        F64Vector output = ensureDoubleCapacity(existing instanceof F64Vector vector ? vector : null, size);
        for (int index = 0; index < sourceCount; index++) {
            output.values()[outputStart + index] = source.values()[sourcePositions[sourceStart + index]];
        }
        return output;
    }

    private F64Vector copyDoublePositions(F64Vector source, Vector existing, SelectedPositions sourcePositions, int outputStart, int size)
    {
        F64Vector output = ensureDoubleCapacity(existing instanceof F64Vector vector ? vector : null, size);
        int[] positions = sourcePositions.backingArrayOrNull();
        if (positions != null) {
            int offset = sourcePositions.backingArrayOffset();
            for (int index = 0; index < sourcePositions.count(); index++) {
                output.values()[outputStart + index] = source.values()[positions[offset + index]];
            }
            return output;
        }
        for (int index = 0; index < sourcePositions.count(); index++) {
            output.values()[outputStart + index] = source.values()[sourcePositions.position(index)];
        }
        return output;
    }

    private F64Vector copyDoubleSinglePosition(F64Vector source, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        F64Vector output = ensureDoubleCapacity(existing instanceof F64Vector vector ? vector : null, size);
        output.values()[outputPosition] = source.values()[sourcePosition];
        return output;
    }

    private BinaryVector copyBinaryPositions(BinaryVector source, Vector existing, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size)
    {
        int usedBytes = binaryWriteOffset(existing, outputStart);
        int byteCapacity = usedBytes;
        int[] sourceOffsets = source.offsets();
        for (int index = 0; index < sourceCount; index++) {
            int sourcePosition = sourcePositions[sourceStart + index];
            byteCapacity += sourceOffsets[sourcePosition + 1] - sourceOffsets[sourcePosition];
        }

        int requestedCapacity = byteCapacity;
        if (existing == null && outputStart == 0 && size > sourceCount) {
            requestedCapacity = Math.max(byteCapacity, estimatedBinaryCapacity(source, size));
        }

        BinaryVector output = BinaryVector.allocateOrGrow(allocator, allocationContext, existing instanceof BinaryVector vector ? vector : null, size, requestedCapacity, usedBytes);
        if (outputStart == 0) {
            Arrays.fill(output.offsets(), 0);
            output.clearTraits();
            output.addTraits(source.traits());
        }
        else {
            output.addTraits(source.traits());
        }

        int[] outputOffsets = output.offsets();
        byte[] outputData = output.data();
        byte[] sourceData = source.data();
        int currentOffset = prepareBinaryWriteOffset(output, outputStart);
        for (int index = 0; index < sourceCount; index++) {
            int targetPosition = outputStart + index;
            int sourcePosition = sourcePositions[sourceStart + index];
            int startOffset = sourceOffsets[sourcePosition];
            int endOffset = sourceOffsets[sourcePosition + 1];
            int length = endOffset - startOffset;
            outputOffsets[targetPosition] = currentOffset;
            if (length == 0) {
                outputOffsets[targetPosition + 1] = currentOffset;
            }
            else {
                System.arraycopy(sourceData, startOffset, outputData, currentOffset, length);
                currentOffset += length;
                outputOffsets[targetPosition + 1] = currentOffset;
            }
        }
        return output;
    }

    private BinaryVector copyBinarySinglePosition(BinaryVector source, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        int[] sourceOffsets = source.offsets();
        int startOffset = sourceOffsets[sourcePosition];
        int endOffset = sourceOffsets[sourcePosition + 1];
        int length = endOffset - startOffset;
        int usedBytes = binaryWriteOffset(existing, outputPosition);
        int requiredCapacity = usedBytes + length;
        int requestedCapacity = requiredCapacity;
        if (existing == null && outputPosition == 0 && size > 1) {
            requestedCapacity = Math.max(requiredCapacity, estimatedBinaryCapacity(source, size));
        }
        BinaryVector output = BinaryVector.allocateOrGrow(allocator, allocationContext, existing instanceof BinaryVector vector ? vector : null, size, requestedCapacity, usedBytes);
        if (outputPosition == 0) {
            Arrays.fill(output.offsets(), 0);
            output.clearTraits();
            output.addTraits(source.traits());
        }
        else {
            output.addTraits(source.traits());
        }

        int[] outputOffsets = output.offsets();
        byte[] outputData = output.data();
        byte[] sourceData = source.data();
        int currentOffset = prepareBinaryWriteOffset(output, outputPosition);
        if (length == 0) {
            outputOffsets[outputPosition + 1] = currentOffset;
            return output;
        }

        System.arraycopy(sourceData, startOffset, outputData, currentOffset, length);
        outputOffsets[outputPosition + 1] = currentOffset + length;
        return output;
    }

    private static int prepareBinaryWriteOffset(BinaryVector output, int outputPosition)
    {
        if (outputPosition == 0) {
            return 0;
        }

        int[] offsets = output.offsets();
        int search = outputPosition;
        while (search > 0 && offsets[search] == 0) {
            search--;
        }
        int currentOffset = offsets[search];
        for (int index = search + 1; index <= outputPosition; index++) {
            offsets[index] = currentOffset;
        }
        return currentOffset;
    }

    private ArrayVector copyArrayPositions(ArrayVector source, Vector existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        ArrayVector output = ensureArrayCapacity(existing instanceof ArrayVector vector ? vector : null, size);
        if (outputStart == 0) {
            output.offsets()[0] = 0;
        }

        int childOutputStart = output.offsets()[outputStart];
        int currentOffset = childOutputStart;
        int totalElements = 0;
        for (int index = 0; index < sourceCount; index++) {
            int sourcePosition = sourcePositions[index];
            output.offsets()[outputStart + index] = currentOffset;
            int length = source.length(sourcePosition);
            currentOffset += length;
            totalElements += length;
        }
        output.offsets()[outputStart + sourceCount] = currentOffset;
        output.setElements(copyStreamsPositions(
                existing instanceof ArrayVector vector ? vector.elements() : null,
                source.elements(),
                nestedPositions(source.offsets(), sourcePositions, sourceCount, totalElements),
                childOutputStart,
                currentOffset));
        return output;
    }

    private ArrayVector copyArraySinglePosition(ArrayVector source, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        ArrayVector output = ensureArrayCapacity(existing instanceof ArrayVector vector ? vector : null, size);
        if (outputPosition == 0) {
            output.offsets()[0] = 0;
        }

        int childOutputStart = output.offsets()[outputPosition];
        int length = source.length(sourcePosition);
        output.offsets()[outputPosition] = childOutputStart;
        output.offsets()[outputPosition + 1] = childOutputStart + length;
        output.setElements(copyStreamsPositions(
                existing instanceof ArrayVector vector ? vector.elements() : null,
                source.elements(),
                nestedPositions(source.offsets(), new int[] {sourcePosition}, 1, length),
                childOutputStart,
                childOutputStart + length));
        return output;
    }

    private MapVector copyMapPositions(MapVector source, Vector existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        MapVector output = ensureMapCapacity(existing instanceof MapVector vector ? vector : null, size);
        if (outputStart == 0) {
            output.offsets()[0] = 0;
        }

        int childOutputStart = output.offsets()[outputStart];
        int currentOffset = childOutputStart;
        int totalEntries = 0;
        for (int index = 0; index < sourceCount; index++) {
            int sourcePosition = sourcePositions[index];
            output.offsets()[outputStart + index] = currentOffset;
            int length = source.length(sourcePosition);
            currentOffset += length;
            totalEntries += length;
        }
        output.offsets()[outputStart + sourceCount] = currentOffset;

        int[] entryPositions = nestedPositions(source.offsets(), sourcePositions, sourceCount, totalEntries);
        output.setEntries(
                copyStreamsPositions(
                        existing instanceof MapVector vector ? vector.keys() : null,
                        source.keys(),
                        entryPositions,
                        childOutputStart,
                        currentOffset),
                copyStreamsPositions(
                        existing instanceof MapVector vector ? vector.values() : null,
                        source.values(),
                        entryPositions,
                        childOutputStart,
                        currentOffset));
        return output;
    }

    private MapVector copyMapSinglePosition(MapVector source, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        MapVector output = ensureMapCapacity(existing instanceof MapVector vector ? vector : null, size);
        if (outputPosition == 0) {
            output.offsets()[0] = 0;
        }

        int childOutputStart = output.offsets()[outputPosition];
        int length = source.length(sourcePosition);
        output.offsets()[outputPosition] = childOutputStart;
        output.offsets()[outputPosition + 1] = childOutputStart + length;

        int[] entryPositions = nestedPositions(source.offsets(), new int[] {sourcePosition}, 1, length);
        output.setEntries(
                copyStreamsPositions(
                        existing instanceof MapVector vector ? vector.keys() : null,
                        source.keys(),
                        entryPositions,
                        childOutputStart,
                        childOutputStart + length),
                copyStreamsPositions(
                        existing instanceof MapVector vector ? vector.values() : null,
                        source.values(),
                        entryPositions,
                        childOutputStart,
                        childOutputStart + length));
        return output;
    }

    private StructVector copyStructPositions(StructVector source, Vector existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        StructVector output = ensureStructCapacity(existing instanceof StructVector vector ? vector : null, size);
        if (outputStart == 0) {
            output.clearFields();
        }
        Map<String, Streams> existingFields = existing instanceof StructVector vector ? vector.fields() : Map.of();
        for (Map.Entry<String, Streams> entry : source.fields().entrySet()) {
            output.setField(entry.getKey(), copyPositions(existingFields.get(entry.getKey()), entry.getValue(), sourcePositions, sourceCount, outputStart, size));
        }
        return output;
    }

    private StructVector copyStructSinglePosition(StructVector source, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        StructVector output = ensureStructCapacity(existing instanceof StructVector vector ? vector : null, size);
        if (outputPosition == 0) {
            output.clearFields();
        }
        Map<String, Streams> existingFields = existing instanceof StructVector vector ? vector.fields() : Map.of();
        for (Map.Entry<String, Streams> entry : source.fields().entrySet()) {
            output.setField(entry.getKey(), copySinglePosition(existingFields.get(entry.getKey()), entry.getValue(), size, outputPosition, sourcePosition));
        }
        return output;
    }

    private I64Vector materializeLongs(Vector[] rows)
    {
        I64Vector result = allocator.allocate(allocationContext, I64Vector.class, totalLength(rows), I64Vector::new);
        long[] target = result.values();
        int outputPosition = 0;
        for (Vector row : rows) {
            // Hoist Vector type dispatch once per row rather than once per position.
            VectorAccess.LongValues rowValues = VectorAccess.longValues(row);
            int rowLength = row.length();
            for (int position = 0; position < rowLength; position++) {
                target[outputPosition++] = rowValues.value(position);
            }
        }
        return result;
    }

    private I32Vector materializeInts(Vector[] rows)
    {
        I32Vector result = allocator.allocate(allocationContext, I32Vector.class, totalLength(rows), I32Vector::new);
        int[] target = result.values();
        int outputPosition = 0;
        for (Vector row : rows) {
            VectorAccess.LongValues rowValues = VectorAccess.longValues(row);
            int rowLength = row.length();
            for (int position = 0; position < rowLength; position++) {
                target[outputPosition++] = (int) rowValues.value(position);
            }
        }
        return result;
    }

    private BooleanVector materializeBooleans(Vector[] rows)
    {
        BooleanVector result = allocator.allocate(allocationContext, BooleanVector.class, totalLength(rows), BooleanVector::new);
        boolean[] target = result.values();
        int outputPosition = 0;
        for (Vector row : rows) {
            VectorAccess.BooleanValues rowValues = VectorAccess.booleanValues(row);
            int rowLength = row.length();
            for (int position = 0; position < rowLength; position++) {
                target[outputPosition++] = rowValues.value(position);
            }
        }
        return result;
    }

    private F64Vector materializeDoubles(Vector[] rows)
    {
        F64Vector result = allocator.allocate(allocationContext, F64Vector.class, totalLength(rows), F64Vector::new);
        double[] target = result.values();
        int outputPosition = 0;
        for (Vector row : rows) {
            VectorAccess.DoubleValues rowValues = VectorAccess.doubleValues(row);
            int rowLength = row.length();
            for (int position = 0; position < rowLength; position++) {
                target[outputPosition++] = rowValues.value(position);
            }
        }
        return result;
    }

    private BinaryVector materializeBinary(BinaryVector sample, Vector[] rows)
    {
        int totalBytes = 0;
        int totalPositions = 0;
        for (Vector row : rows) {
            totalPositions += row.length();
            for (int position = 0; position < row.length(); position++) {
                totalBytes += OperatorVectorSupport.binaryLength(row, position);
            }
        }

        BinaryVector result = BinaryVector.allocate(allocator, allocationContext, totalPositions, totalBytes);
        result.addTraits(sample.traits());
        int outputPosition = 0;
        for (Vector row : rows) {
            for (int position = 0; position < row.length(); position++) {
                copyBinaryValue(result, outputPosition++, row, position);
            }
        }
        return result;
    }

    private ArrayVector materializeArrays(ArrayVector sample, Vector[] rows)
    {
        ArrayVector result = allocator.allocateArray(allocationContext, totalLength(rows));
        int outputPosition = 0;
        int elementCount = 0;
        for (Vector row : rows) {
            ArrayVector arrays = (ArrayVector) row;
            for (int position = 0; position < arrays.length(); position++) {
                result.offsets()[outputPosition++] = elementCount;
                elementCount += arrays.length(position);
            }
        }
        result.offsets()[outputPosition] = elementCount;

        Streams.Builder elements = Streams.builder();
        for (Map.Entry<Stream, Vector> entry : sample.elements().asMap().entrySet()) {
            Vector[] childRows = new Vector[rows.length];
            for (int index = 0; index < rows.length; index++) {
                childRows[index] = ((ArrayVector) rows[index]).elements().get(entry.getKey());
            }
            elements.put(entry.getKey(), materializeStream(entry.getValue(), childRows));
        }
        result.setElements(elements.build());
        return result;
    }

    private MapVector materializeMaps(MapVector sample, Vector[] rows)
    {
        MapVector result = allocator.allocateMap(allocationContext, totalLength(rows));
        int outputPosition = 0;
        int entryCount = 0;
        for (Vector row : rows) {
            MapVector maps = (MapVector) row;
            for (int position = 0; position < maps.length(); position++) {
                result.offsets()[outputPosition++] = entryCount;
                entryCount += maps.length(position);
            }
        }
        result.offsets()[outputPosition] = entryCount;

        Streams.Builder keys = Streams.builder();
        for (Map.Entry<Stream, Vector> entry : sample.keys().asMap().entrySet()) {
            Vector[] childRows = new Vector[rows.length];
            for (int index = 0; index < rows.length; index++) {
                childRows[index] = ((MapVector) rows[index]).keys().get(entry.getKey());
            }
            keys.put(entry.getKey(), materializeStream(entry.getValue(), childRows));
        }

        Streams.Builder values = Streams.builder();
        for (Map.Entry<Stream, Vector> entry : sample.values().asMap().entrySet()) {
            Vector[] childRows = new Vector[rows.length];
            for (int index = 0; index < rows.length; index++) {
                childRows[index] = ((MapVector) rows[index]).values().get(entry.getKey());
            }
            values.put(entry.getKey(), materializeStream(entry.getValue(), childRows));
        }
        result.setEntries(keys.build(), values.build());
        return result;
    }

    private StructVector materializeStructs(StructVector sample, Vector[] rows)
    {
        StructVector result = allocator.allocate(allocationContext, StructVector.class, totalLength(rows), StructVector::new);
        for (Map.Entry<String, Streams> field : sample.fields().entrySet()) {
            Streams.Builder streams = Streams.builder();
            for (Map.Entry<Stream, Vector> entry : field.getValue().asMap().entrySet()) {
                Vector[] childRows = new Vector[rows.length];
                for (int index = 0; index < rows.length; index++) {
                    childRows[index] = ((StructVector) rows[index]).field(field.getKey()).get(entry.getKey());
                }
                streams.put(entry.getKey(), materializeStream(entry.getValue(), childRows));
            }
            result.setField(field.getKey(), streams.build());
        }
        return result;
    }

    private static void copyBinaryValue(BinaryVector target, int targetPosition, Vector source, int sourcePosition)
    {
        switch (source) {
            case BinaryVector values -> {
                int length = values.length(sourcePosition);
                if (length == 0) {
                    target.setNull(targetPosition);
                }
                else {
                    target.setBytes(targetPosition, values.data(), values.startOffset(sourcePosition), length);
                }
            }
            case DictionaryVector values -> copyBinaryValue(target, targetPosition, values.values(), values.ids()[sourcePosition]);
            case RleVector values -> copyBinaryValue(target, targetPosition, values.values(), OperatorVectorSupport.runIndex(values, sourcePosition));
            default -> throw new IllegalArgumentException("Expected binary vector but found " + source.getClass().getSimpleName());
        }
    }

    private Vector emptyVector(Vector source)
    {
        return source.emptyLike(allocator, allocationContext);
    }

    private static int[] positions(Mask mask, int maskStart, int copied)
    {
        int[] positions = new int[copied];
        if (mask.all()) {
            for (int index = 0; index < copied; index++) {
                positions[index] = maskStart + index;
            }
            return positions;
        }
        for (int index = 0; index < copied; index++) {
            positions[index] = mask.position(maskStart + index);
        }
        return positions;
    }

    private static int[] positions(PositionBuffer positionBuffer, Mask mask, int maskStart, int copied)
    {
        return positionBuffer == null ? positions(mask, maskStart, copied) : positionBuffer.positions(mask, maskStart, copied);
    }

    private static int[] repeatedPositions(int position, int length)
    {
        int[] positions = new int[length];
        Arrays.fill(positions, position);
        return positions;
    }

    private static int[] nestedPositions(int[] offsets, int[] sourcePositions, int sourceCount, int totalNestedPositions)
    {
        int[] positions = new int[totalNestedPositions];
        int outputIndex = 0;
        for (int index = 0; index < sourceCount; index++) {
            int sourcePosition = sourcePositions[index];
            for (int nested = offsets[sourcePosition]; nested < offsets[sourcePosition + 1]; nested++) {
                positions[outputIndex++] = nested;
            }
        }
        return positions;
    }

    private static int[] dictionaryPositions(int[] ids, int[] sourcePositions, int sourceStart, int sourceCount)
    {
        int[] positions = new int[sourceCount];
        for (int index = 0; index < sourceCount; index++) {
            positions[index] = ids[sourcePositions[sourceStart + index]];
        }
        return positions;
    }

    private static int[] dictionaryPositions(PositionMappingCache positionCache, int[] ids, int[] sourcePositions, int sourceStart, int sourceCount)
    {
        return positionCache != null ? positionCache.dictionaryPositions(ids, sourcePositions, sourceStart, sourceCount) : dictionaryPositions(ids, sourcePositions, sourceStart, sourceCount);
    }

    private static int[] rlePositions(RleVector values, int[] sourcePositions, int sourceStart, int sourceCount)
    {
        int[] positions = new int[sourceCount];
        for (int index = 0; index < sourceCount; index++) {
            positions[index] = rlePosition(values, sourcePositions[sourceStart + index]);
        }
        return positions;
    }

    private static int[] rlePositions(PositionMappingCache positionCache, RleVector values, int[] sourcePositions, int sourceStart, int sourceCount)
    {
        return positionCache != null ? positionCache.rlePositions(values, sourcePositions, sourceStart, sourceCount) : rlePositions(values, sourcePositions, sourceStart, sourceCount);
    }

    private static int rlePosition(RleVector values, int sourcePosition)
    {
        return OperatorVectorSupport.runIndex(values, sourcePosition);
    }

    private static int totalLength(Vector[] rows)
    {
        int totalLength = 0;
        for (Vector row : rows) {
            totalLength += row.length();
        }
        return totalLength;
    }

    private I32Vector ensureIntCapacity(I32Vector existing, int size)
    {
        if (existing == null) {
            return allocator.allocate(allocationContext, I32Vector.class, size, I32Vector::new);
        }
        if (existing.length() >= size) {
            return existing;
        }

        I32Vector grown = allocator.allocate(allocationContext, I32Vector.class, size, I32Vector::new);
        System.arraycopy(existing.values(), 0, grown.values(), 0, existing.length());
        return grown;
    }

    private I64Vector ensureLongCapacity(I64Vector existing, int size)
    {
        if (existing == null) {
            return allocator.allocate(allocationContext, I64Vector.class, size, I64Vector::new);
        }
        if (existing.length() >= size) {
            return existing;
        }

        I64Vector grown = allocator.allocate(allocationContext, I64Vector.class, size, I64Vector::new);
        System.arraycopy(existing.values(), 0, grown.values(), 0, existing.length());
        return grown;
    }

    private BooleanVector ensureBooleanCapacity(BooleanVector existing, int size)
    {
        if (existing == null) {
            return allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new);
        }
        if (existing.length() >= size) {
            return existing;
        }

        BooleanVector grown = allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new);
        System.arraycopy(existing.values(), 0, grown.values(), 0, existing.length());
        return grown;
    }

    private F64Vector ensureDoubleCapacity(F64Vector existing, int size)
    {
        if (existing == null) {
            return allocator.allocate(allocationContext, F64Vector.class, size, F64Vector::new);
        }
        if (existing.length() >= size) {
            return existing;
        }

        F64Vector grown = allocator.allocate(allocationContext, F64Vector.class, size, F64Vector::new);
        System.arraycopy(existing.values(), 0, grown.values(), 0, existing.length());
        return grown;
    }

    private ArrayVector ensureArrayCapacity(ArrayVector existing, int size)
    {
        if (existing == null) {
            return allocator.allocateArray(allocationContext, size);
        }
        if (existing.length() >= size) {
            return existing;
        }

        ArrayVector grown = allocator.allocateArray(allocationContext, size);
        System.arraycopy(existing.offsets(), 0, grown.offsets(), 0, existing.offsets().length);
        grown.setElements(allocator.copyStreams(allocationContext, existing.elements()));
        return grown;
    }

    private MapVector ensureMapCapacity(MapVector existing, int size)
    {
        if (existing == null) {
            return allocator.allocateMap(allocationContext, size);
        }
        if (existing.length() >= size) {
            return existing;
        }

        MapVector grown = allocator.allocateMap(allocationContext, size);
        System.arraycopy(existing.offsets(), 0, grown.offsets(), 0, existing.offsets().length);
        grown.setEntries(
                allocator.copyStreams(allocationContext, existing.keys()),
                allocator.copyStreams(allocationContext, existing.values()));
        return grown;
    }

    private StructVector ensureStructCapacity(StructVector existing, int size)
    {
        if (existing == null) {
            return allocator.allocate(allocationContext, StructVector.class, size, StructVector::new);
        }
        if (existing.length() >= size) {
            return existing;
        }

        StructVector grown = allocator.allocate(allocationContext, StructVector.class, size, StructVector::new);
        for (Map.Entry<String, Streams> entry : existing.fields().entrySet()) {
            grown.setField(entry.getKey(), allocator.copyStreams(allocationContext, entry.getValue()));
        }
        return grown;
    }

    private static int binaryCapacity(Vector existing, int positionCount)
    {
        if (existing instanceof BinaryVector values) {
            return values.offsets()[positionCount];
        }
        return 0;
    }

    private static int binaryWriteOffset(Vector existing, int outputPosition)
    {
        if (!(existing instanceof BinaryVector values)) {
            return 0;
        }
        if (outputPosition == 0) {
            return 0;
        }
        int[] offsets = values.offsets();
        int search = outputPosition;
        while (search > 0 && offsets[search] == 0) {
            search--;
        }
        return offsets[search];
    }

    private static int estimatedBinaryCapacity(BinaryVector source, int positionCount)
    {
        if (source.length() == 0 || positionCount <= 0) {
            return 0;
        }

        int usedBytes = source.offsets()[source.length()];
        long estimated = Math.max(1L, (usedBytes + (long) source.length() - 1) / source.length()) * positionCount;
        return toIntExact(Math.min(Integer.MAX_VALUE, estimated));
    }

    private static Streams.Builder ensureBuilder(Streams.Builder builder, Streams existing)
    {
        if (builder != null) {
            return builder;
        }
        return Streams.builder().putAll(existing);
    }

    private PositionMappingCache positionCache(Output input, PositionMappingCache sharedPositionCache)
    {
        if (!POSITION_MAPPING_CACHE) {
            return null;
        }
        if (sharedPositionCache != null) {
            return sharedPositionCache;
        }
        int streamCount = input.hasValues() ? 1 : 0;
        if (hasConcreteStream(input, Stream.NULLS)) {
            streamCount++;
        }
        if (hasConcreteStream(input, Stream.ERRORS)) {
            streamCount++;
        }
        return streamCount > 1 ? new PositionMappingCache(allocator.primitiveArrays(), false) : null;
    }

    private PositionMappingCache positionCache(Streams input, PositionMappingCache sharedPositionCache)
    {
        if (!POSITION_MAPPING_CACHE) {
            return null;
        }
        if (sharedPositionCache != null) {
            return sharedPositionCache;
        }
        int streamCount = input.hasValues() ? 1 : 0;
        if (input.hasNulls()) {
            streamCount++;
        }
        if (input.hasErrors()) {
            streamCount++;
        }
        return streamCount > 1 ? new PositionMappingCache(allocator.primitiveArrays(), false) : null;
    }

    static final class PositionMappingCache
    {
        private final PrimitiveArrayPool arrayPool;
        private final boolean recycling;
        private final Object[] mappings = new Object[POSITION_MAPPING_CACHE_SIZE];
        private final int[][] sourcePositions = new int[POSITION_MAPPING_CACHE_SIZE][];
        private final int[] sourceStarts = new int[POSITION_MAPPING_CACHE_SIZE];
        private final int[] sourceCounts = new int[POSITION_MAPPING_CACHE_SIZE];
        private final int[][] mappedPositions = new int[POSITION_MAPPING_CACHE_SIZE][];
        private int count;

        private PositionMappingCache(PrimitiveArrayPool arrayPool, boolean recycling)
        {
            this.arrayPool = arrayPool;
            this.recycling = recycling;
        }

        void reset()
        {
            Arrays.fill(mappings, 0, count, null);
            Arrays.fill(sourcePositions, 0, count, null);
            count = 0;
        }

        void release()
        {
            reset();
            if (!recycling) {
                return;
            }
            for (int index = 0; index < mappedPositions.length; index++) {
                arrayPool.release(mappedPositions[index]);
                mappedPositions[index] = null;
            }
        }

        private int[] dictionaryPositions(int[] ids, int[] positions, int sourceStart, int sourceCount)
        {
            int[] mapped = get(ids, positions, sourceStart, sourceCount);
            if (mapped != null) {
                return mapped;
            }

            mapped = acquire(sourceCount);
            for (int index = 0; index < sourceCount; index++) {
                mapped[index] = ids[positions[sourceStart + index]];
            }
            put(ids, positions, sourceStart, sourceCount, mapped);
            return mapped;
        }

        private int[] rlePositions(RleVector values, int[] positions, int sourceStart, int sourceCount)
        {
            int[] mapped = get(values, positions, sourceStart, sourceCount);
            if (mapped != null) {
                return mapped;
            }

            mapped = acquire(sourceCount);
            for (int index = 0; index < sourceCount; index++) {
                mapped[index] = rlePosition(values, positions[sourceStart + index]);
            }
            put(values, positions, sourceStart, sourceCount, mapped);
            return mapped;
        }

        private int[] get(Object mapping, int[] positions, int sourceStart, int sourceCount)
        {
            for (int index = 0; index < count; index++) {
                if (mappings[index] == mapping &&
                        sourcePositions[index] == positions &&
                        sourceStarts[index] == sourceStart &&
                        sourceCounts[index] == sourceCount) {
                    return mappedPositions[index];
                }
            }
            return null;
        }

        private void put(Object mapping, int[] positions, int sourceStart, int sourceCount, int[] mapped)
        {
            if (count == POSITION_MAPPING_CACHE_SIZE) {
                return;
            }
            int index = count++;
            mappings[index] = mapping;
            sourcePositions[index] = positions;
            sourceStarts[index] = sourceStart;
            sourceCounts[index] = sourceCount;
            mappedPositions[index] = mapped;
        }

        private int[] acquire(int requiredCapacity)
        {
            if (!recycling || count == POSITION_MAPPING_CACHE_SIZE) {
                return new int[requiredCapacity];
            }
            int[] mapped = mappedPositions[count];
            if (mapped == null || mapped.length < requiredCapacity) {
                arrayPool.release(mapped);
                mapped = arrayPool.borrowInts(requiredCapacity);
                mappedPositions[count] = mapped;
            }
            return mapped;
        }
    }
}
