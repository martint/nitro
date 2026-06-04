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
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.builtin.VectorAccess;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Set;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Hand-coded {@link FlatKeyLayout} specialization for the {@code (BIGINT, BIGINT)} shape.
 *
 * <p>Resolves Vector type dispatch ONCE per batch in {@link #beginBatch(Vector[], Vector[])} and
 * caches typed accessors as fields. The per-position hot methods ({@link #hash},
 * {@link #writeRecord}, {@link #identicalRecordToInput}) read directly from those cached
 * accessors with no runtime type check — matching the architectural rule that Vector type checks
 * must be hoisted outside per-position loops and inner loops must stay tight. This is the
 * hand-written equivalent of a bytecode-generated strategy for the type list
 * {@code List.of(BIGINT, BIGINT)}.
 */
final class BigintPairFlatKeyLayout
        extends FlatKeyLayout
{
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, LITTLE_ENDIAN);

    private final int firstKeyOffset;
    private final int secondKeyOffset;
    private final boolean nullable;

    // Accessors resolved once per batch by beginBatch, read directly by the per-position hot
    // methods. All fields are valid only between a beginBatch call and the matching endBatch.
    private VectorAccess.LongValues firstKeyAccessor;
    private VectorAccess.LongValues secondKeyAccessor;
    private VectorAccess.BooleanValues firstNullAccessor;
    private VectorAccess.BooleanValues secondNullAccessor;

    private BigintPairFlatKeyLayout(Field[] fields, int[] inputChannels, FlatTypeHandler[] handlers, int[] fixedOffsets, int[] comparisonOrder, int nullByteCount, int fixedRecordSize)
    {
        super(fields, inputChannels, handlers, fixedOffsets, comparisonOrder, nullByteCount, fixedRecordSize, false);
        this.firstKeyOffset = fixedOffsets[0];
        this.secondKeyOffset = fixedOffsets[1];
        this.nullable = nullByteCount > 0;
    }

    static BigintPairFlatKeyLayout create(Vector[] values, boolean nullable)
    {
        if (values.length != 2) {
            throw new IllegalArgumentException("BigintPairFlatKeyLayout requires exactly two columns");
        }
        FlatTypeHandler handler0 = FlatTypeHandlers.forVector(values[0]);
        FlatTypeHandler handler1 = FlatTypeHandlers.forVector(values[1]);
        if (handler0 == null || handler1 == null
                || handler0.kind() != FlatTypeHandler.Kind.LONG
                || handler1.kind() != FlatTypeHandler.Kind.LONG) {
            throw new IllegalArgumentException("BigintPairFlatKeyLayout requires two LONG-kind columns");
        }
        FlatTypeHandler handler = handler0;
        int nullByteCount = nullable ? 1 : 0;
        int keySize = handler.fixedSize();
        int firstOffset = nullByteCount;
        int secondOffset = firstOffset + keySize;
        Field[] fields = new Field[] {
                new Field(0, handler, firstOffset, Set.<BinaryVector.Trait>of()),
                new Field(1, handler, secondOffset, Set.<BinaryVector.Trait>of()),
        };
        int[] inputChannels = new int[] {0, 1};
        FlatTypeHandler[] handlers = new FlatTypeHandler[] {handler, handler};
        int[] fixedOffsets = new int[] {firstOffset, secondOffset};
        int[] comparisonOrder = new int[] {0, 1};
        return new BigintPairFlatKeyLayout(fields, inputChannels, handlers, fixedOffsets, comparisonOrder, nullByteCount, secondOffset + keySize);
    }

    @Override
    public void beginBatch(Vector[] values, Vector[] nulls)
    {
        // Hoist type dispatch once here; VectorAccess.longValues / booleanValues pattern-match
        // the Vector shape a single time and return a typed accessor whose per-position call
        // is a monomorphic lambda invocation.
        firstKeyAccessor = VectorAccess.longValues(values[0]);
        secondKeyAccessor = VectorAccess.longValues(values[1]);
        firstNullAccessor = (nulls != null && nulls.length > 0) ? VectorAccess.booleanValues(nulls[0]) : null;
        secondNullAccessor = (nulls != null && nulls.length > 1) ? VectorAccess.booleanValues(nulls[1]) : null;
    }

    @Override
    public void endBatch()
    {
        firstKeyAccessor = null;
        secondKeyAccessor = null;
        firstNullAccessor = null;
        secondNullAccessor = null;
    }

    @Override
    public long hash(Vector[] values, Vector[] nulls, int position)
    {
        // Hot path: direct calls into hoisted accessors. No Vector[] indexing, no OVS switch,
        // no instanceof branching per position.
        long first = firstKeyAccessor.value(position);
        long second = secondKeyAccessor.value(position);
        long nullTag = 0;
        if (nullable) {
            boolean firstIsNull = firstNullAccessor != null && firstNullAccessor.value(position);
            boolean secondIsNull = secondNullAccessor != null && secondNullAccessor.value(position);
            if (firstIsNull) {
                first = 0;
                nullTag |= 1;
            }
            if (secondIsNull) {
                second = 0;
                nullTag |= 2;
            }
        }
        long hash = first * 0x9E3779B97F4A7C15L
                + second * 0xC4CEB9FE1A85EC53L
                + nullTag;
        hash ^= hash >>> 33;
        hash *= 0xFF51AFD7ED558CCDL;
        hash ^= hash >>> 33;
        hash *= 0xC4CEB9FE1A85EC53L;
        hash ^= hash >>> 33;
        return hash;
    }

    @Override
    public void writeRecord(byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena, Vector[] values, Vector[] nulls, int position, int recordIndex)
    {
        long first = firstKeyAccessor.value(position);
        long second = secondKeyAccessor.value(position);
        if (nullable) {
            boolean firstIsNull = firstNullAccessor != null && firstNullAccessor.value(position);
            boolean secondIsNull = secondNullAccessor != null && secondNullAccessor.value(position);
            byte nullByte = (byte) ((firstIsNull ? 1 : 0) | (secondIsNull ? 2 : 0));
            fixedChunk[fixedOffset] = nullByte;
            if (firstIsNull) {
                first = 0;
            }
            if (secondIsNull) {
                second = 0;
            }
        }
        LONG_HANDLE.set(fixedChunk, fixedOffset + firstKeyOffset, first);
        LONG_HANDLE.set(fixedChunk, fixedOffset + secondKeyOffset, second);
    }

    @Override
    public boolean identicalRecordToInput(byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena, Vector[] values, Vector[] nulls, int position, int recordIndex)
    {
        long first = firstKeyAccessor.value(position);
        long second = secondKeyAccessor.value(position);
        if (nullable) {
            byte storedNullByte = fixedChunk[fixedOffset];
            boolean storedFirstNull = (storedNullByte & 1) != 0;
            boolean storedSecondNull = (storedNullByte & 2) != 0;
            boolean inputFirstNull = firstNullAccessor != null && firstNullAccessor.value(position);
            boolean inputSecondNull = secondNullAccessor != null && secondNullAccessor.value(position);
            if (storedFirstNull != inputFirstNull || storedSecondNull != inputSecondNull) {
                return false;
            }
            if (!storedFirstNull && (long) LONG_HANDLE.get(fixedChunk, fixedOffset + firstKeyOffset) != first) {
                return false;
            }
            if (!storedSecondNull && (long) LONG_HANDLE.get(fixedChunk, fixedOffset + secondKeyOffset) != second) {
                return false;
            }
            return true;
        }
        return (long) LONG_HANDLE.get(fixedChunk, fixedOffset + firstKeyOffset) == first
                && (long) LONG_HANDLE.get(fixedChunk, fixedOffset + secondKeyOffset) == second;
    }
}
