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

import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.RegionVector;
import org.weakref.nitro.data.RepeatedVector;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.Arrays;

import static java.lang.Math.toIntExact;

/** Batch-resolved direct physical sources for one repeated key field. */
final class RepeatedKeyBatchBinding
{
    private record BoundVector(Vector vector, int[] mapping, int mappingOffset, int baseOffset, int[] ownedMapping) {}

    private final ResolvedRepeatedKeyLayout layout;
    private final PrimitiveArrayPool arrayPool;
    final Object[] outputArrays;
    final int[][] outputBinaryOffsets;
    final int[][] outputMappings;
    final int[] outputMappingOffsets;
    final int[] outputBaseOffsets;
    private final int[][] ownedOutputMappings;
    final boolean[][][] nullArrays;
    final int[][][] nullMappings;
    final int[][] nullMappingOffsets;
    final int[][] nullBaseOffsets;
    private final int[][][] ownedNullMappings;
    private RepeatedVector repeated;
    int[] repeatedOffsets;
    int[] repeatedMapping;
    int repeatedMappingOffset;
    int repeatedBaseOffset;
    private int[] ownedRepeatedMapping;
    int[] orderScratch;
    int[] canonicalOffsets;
    byte[] canonicalBytes;

    RepeatedKeyBatchBinding(ResolvedRepeatedKeyLayout layout, PrimitiveArrayPool arrayPool)
    {
        this.layout = layout;
        this.arrayPool = arrayPool;
        int outputs = layout.outputs().size();
        outputArrays = new Object[outputs];
        outputBinaryOffsets = new int[outputs][];
        outputMappings = new int[outputs][];
        outputMappingOffsets = new int[outputs];
        outputBaseOffsets = new int[outputs];
        ownedOutputMappings = new int[outputs][];
        nullArrays = new boolean[outputs][][];
        nullMappings = new int[outputs][][];
        nullMappingOffsets = new int[outputs][];
        nullBaseOffsets = new int[outputs][];
        ownedNullMappings = new int[outputs][][];
        for (int output = 0; output < outputs; output++) {
            int nullSources = layout.outputs().get(output).nullPaths().size();
            nullArrays[output] = new boolean[nullSources][];
            nullMappings[output] = new int[nullSources][];
            nullMappingOffsets[output] = new int[nullSources];
            nullBaseOffsets[output] = new int[nullSources];
            ownedNullMappings[output] = new int[nullSources][];
        }
    }

    void bind(Vector value)
    {
        release();
        try {
            BoundVector parent = bindVector(value, true);
            repeated = (RepeatedVector) parent.vector();
            repeatedOffsets = repeated.offsets();
            repeatedMapping = parent.mapping();
            repeatedMappingOffset = parent.mappingOffset();
            repeatedBaseOffset = parent.baseOffset();
            ownedRepeatedMapping = parent.ownedMapping();

            for (int output = 0; output < outputArrays.length; output++) {
                ResolvedRepeatedKeyLayout.Output descriptor = layout.outputs().get(output);
                Streams root = repeated.repeatedOutput(descriptor.output());
                if (!descriptor.presence()) {
                    BoundVector child = bindVector(streamsAtPath(root.values(), descriptor.fieldPath()).values(), false);
                    switch (descriptor.storage()) {
                        case PRESENCE -> throw new AssertionError();
                        case I32 -> outputArrays[output] = ((I32Vector) child.vector()).values();
                        case I64 -> outputArrays[output] = ((I64Vector) child.vector()).values();
                        case BOOLEAN -> outputArrays[output] = ((BooleanVector) child.vector()).values();
                        case F64 -> outputArrays[output] = ((F64Vector) child.vector()).values();
                        case BINARY -> {
                            BinaryVector binary = (BinaryVector) child.vector();
                            outputArrays[output] = binary.data();
                            outputBinaryOffsets[output] = binary.offsets();
                        }
                    }
                    outputMappings[output] = child.mapping();
                    outputMappingOffsets[output] = child.mappingOffset();
                    outputBaseOffsets[output] = child.baseOffset();
                    ownedOutputMappings[output] = child.ownedMapping();
                }

                for (int source = 0; source < descriptor.nullPaths().size(); source++) {
                    java.util.List<String> path = descriptor.nullPaths().get(source);
                    Vector nullVector = path.isEmpty()
                            ? root.getOrNull(Stream.NULLS)
                            : streamsAtPath(root.values(), path).getOrNull(Stream.NULLS);
                    if (!VectorAccess.isAllFalseNulls(nullVector)) {
                        BoundVector childNulls = bindVector(nullVector, false);
                        nullArrays[output][source] = ((BooleanVector) childNulls.vector()).values();
                        nullMappings[output][source] = childNulls.mapping();
                        nullMappingOffsets[output][source] = childNulls.mappingOffset();
                        nullBaseOffsets[output][source] = childNulls.baseOffset();
                        ownedNullMappings[output][source] = childNulls.ownedMapping();
                    }
                }
            }
        }
        catch (RuntimeException | Error e) {
            release();
            throw e;
        }
    }

    long hash(int position)
    {
        throw new UnsupportedOperationException("Repeated key hashing requires a generated physical kernel");
    }

    void write(int position, byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena arena)
    {
        throw new UnsupportedOperationException("Repeated key writes require a generated physical kernel");
    }

    boolean identical(byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena arena, int position)
    {
        throw new UnsupportedOperationException("Repeated key equality requires a generated physical kernel");
    }

    void release()
    {
        arrayPool.release(ownedRepeatedMapping);
        ownedRepeatedMapping = null;
        repeated = null;
        repeatedOffsets = null;
        repeatedMapping = null;
        for (int output = 0; output < outputArrays.length; output++) {
            arrayPool.release(ownedOutputMappings[output]);
            ownedOutputMappings[output] = null;
            outputArrays[output] = null;
            outputBinaryOffsets[output] = null;
            outputMappings[output] = null;
            for (int source = 0; source < ownedNullMappings[output].length; source++) {
                arrayPool.release(ownedNullMappings[output][source]);
                ownedNullMappings[output][source] = null;
                nullArrays[output][source] = null;
                nullMappings[output][source] = null;
            }
        }
        arrayPool.release(orderScratch);
        orderScratch = null;
        arrayPool.release(canonicalOffsets);
        canonicalOffsets = null;
        arrayPool.release(canonicalBytes);
        canonicalBytes = null;
    }

    int[] orderScratch(int required)
    {
        if (orderScratch == null || orderScratch.length < required) {
            arrayPool.release(orderScratch);
            orderScratch = arrayPool.borrowInts(growthCapacity(orderScratch == null ? 0 : orderScratch.length, required));
        }
        return orderScratch;
    }

    int[] canonicalOffsets(int required)
    {
        if (canonicalOffsets == null || canonicalOffsets.length < required) {
            arrayPool.release(canonicalOffsets);
            canonicalOffsets = arrayPool.borrowInts(growthCapacity(canonicalOffsets == null ? 0 : canonicalOffsets.length, required));
        }
        return canonicalOffsets;
    }

    byte[] canonicalBytes(int required)
    {
        if (canonicalBytes == null || canonicalBytes.length < required) {
            arrayPool.release(canonicalBytes);
            canonicalBytes = arrayPool.borrowBytes(growthCapacity(canonicalBytes == null ? 0 : canonicalBytes.length, required));
        }
        return canonicalBytes;
    }

    private static int growthCapacity(int current, int required)
    {
        if (required < 0) {
            throw new IllegalArgumentException("required capacity is negative");
        }
        long grown = Math.max(required, Math.max(16L, (long) current + (current >> 1)));
        return toIntExact(Math.min(Integer.MAX_VALUE, grown));
    }

    private BoundVector bindVector(Vector vector, boolean requireRepeated)
    {
        return switch (vector) {
            case RepeatedVector repeated when requireRepeated -> new BoundVector(repeated, null, 0, 0, null);
            case I64Vector values when !requireRepeated -> new BoundVector(values, null, 0, 0, null);
            case I32Vector values when !requireRepeated -> new BoundVector(values, null, 0, 0, null);
            case BooleanVector values when !requireRepeated -> new BoundVector(values, null, 0, 0, null);
            case F64Vector values when !requireRepeated -> new BoundVector(values, null, 0, 0, null);
            case BinaryVector values when !requireRepeated -> new BoundVector(values, null, 0, 0, null);
            case RegionVector region -> regionBinding(region, requireRepeated);
            case DictionaryVector dictionary -> dictionaryBinding(dictionary, requireRepeated);
            case RleVector rle -> rleBinding(rle, requireRepeated);
            default -> throw new IllegalArgumentException("No repeated-key loader for " + vector.getClass().getSimpleName());
        };
    }

    private static Streams streamsAtPath(Vector value, java.util.List<String> path)
    {
        Streams streams = Streams.ofValues(value);
        for (String field : path) {
            streams = VectorAccess.structField(streams.values(), field);
        }
        return streams;
    }

    private BoundVector regionBinding(RegionVector region, boolean requireRepeated)
    {
        BoundVector child = bindVector(region.values(), requireRepeated);
        if (child.mapping() == null) {
            return new BoundVector(child.vector(), null, 0, child.baseOffset() + region.offset(), child.ownedMapping());
        }
        return new BoundVector(child.vector(), child.mapping(), child.mappingOffset() + region.offset(), child.baseOffset(), child.ownedMapping());
    }

    private BoundVector dictionaryBinding(DictionaryVector dictionary, boolean requireRepeated)
    {
        BoundVector child = bindVector(dictionary.values(), requireRepeated);
        if (child.mapping() == null) {
            return new BoundVector(child.vector(), dictionary.ids(), 0, child.baseOffset(), child.ownedMapping());
        }
        int[] mapping = arrayPool.borrowInts(dictionary.length());
        for (int position = 0; position < dictionary.length(); position++) {
            mapping[position] = physical(child.mapping(), child.mappingOffset(), child.baseOffset(), dictionary.ids()[position]);
        }
        arrayPool.release(child.ownedMapping());
        return new BoundVector(child.vector(), mapping, 0, 0, mapping);
    }

    private BoundVector rleBinding(RleVector rle, boolean requireRepeated)
    {
        BoundVector child = bindVector(rle.values(), requireRepeated);
        int[] mapping = arrayPool.borrowInts(rle.length());
        int output = 0;
        for (int run = 0; run < rle.counts().length; run++) {
            int physical = physical(child.mapping(), child.mappingOffset(), child.baseOffset(), run);
            Arrays.fill(mapping, output, output + rle.counts()[run], physical);
            output += rle.counts()[run];
        }
        arrayPool.release(child.ownedMapping());
        return new BoundVector(child.vector(), mapping, 0, 0, mapping);
    }

    private static int physical(int[] mapping, int mappingOffset, int baseOffset, int position)
    {
        return (mapping == null ? position : mapping[position + mappingOffset]) + baseOffset;
    }
}
