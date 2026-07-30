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

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongLists;
import org.weakref.nitro.data.Vector;

import java.util.HashMap;
import java.util.Map;

final class StructuralHashJoinIndex
        extends JoinIndex
{
    private final Map<StructuralHashRowKey, LongArrayList> rowsByKey;
    private final StructuralKeyKernel[] kernels;
    private final StructuralHashRowKey reusableProbeKey;
    private final boolean ownsStorage;

    StructuralHashJoinIndex(StructuralKeyKernel[] kernels)
    {
        this.kernels = kernels;
        this.rowsByKey = new HashMap<>();
        this.reusableProbeKey = new StructuralHashRowKey(kernels);
        this.ownsStorage = true;
    }

    private StructuralHashJoinIndex(StructuralHashJoinIndex prepared)
    {
        this.kernels = prepared.kernels;
        this.rowsByKey = prepared.rowsByKey;
        this.reusableProbeKey = new StructuralHashRowKey(kernels);
        this.ownsStorage = false;
    }

    StructuralHashJoinIndex newProbeView()
    {
        return new StructuralHashJoinIndex(this);
    }

    @Override
    public boolean isEmpty()
    {
        return rowsByKey.isEmpty();
    }

    @Override
    public void add(Vector[] values, Vector[] nulls, int position, long rowReference)
    {
        if (hasNull(values, nulls, position)) {
            return;
        }
        StructuralHashRowKey key = new StructuralHashRowKey(kernels);
        key.set(values, nulls, position);
        rowsByKey.computeIfAbsent(key, ignored -> new LongArrayList()).add(rowReference);
    }

    @Override
    public LongList matches(Vector[] values, Vector[] nulls, int position)
    {
        if (hasNull(values, nulls, position)) {
            return LongLists.emptyList();
        }
        reusableProbeKey.set(values, nulls, position);
        LongArrayList rows = rowsByKey.get(reusableProbeKey);
        return rows == null ? LongLists.emptyList() : rows;
    }

    @Override
    long retainedBytes()
    {
        if (!ownsStorage) {
            return 0;
        }
        long bytes = 0;
        for (LongArrayList rows : rowsByKey.values()) {
            bytes += (long) rows.elements().length * Long.BYTES;
        }
        return bytes;
    }

    @Override
    void releaseBuffers()
    {
        if (ownsStorage) {
            rowsByKey.clear();
        }
    }

    private static boolean hasNull(Vector[] values, Vector[] nulls, int position)
    {
        if (nulls.length == 0) {
            return false;
        }
        for (int keyIndex = 0; keyIndex < values.length; keyIndex++) {
            if (OperatorVectorSupport.isNull(nulls[keyIndex], position)) {
                return true;
            }
        }
        return false;
    }

    private static final class StructuralHashRowKey
    {
        private final StructuralKeyKernel[] kernels;
        private Vector[] values;
        private Vector[] nulls;
        private int position;
        private int hash;

        private StructuralHashRowKey(StructuralKeyKernel[] kernels)
        {
            this.kernels = kernels;
        }

        private void set(Vector[] values, Vector[] nulls, int position)
        {
            this.values = values;
            this.nulls = nulls;
            this.position = position;
            int result = 1;
            for (int keyIndex = 0; keyIndex < values.length; keyIndex++) {
                result = 31 * result + Long.hashCode(
                        kernels[keyIndex].hash(values[keyIndex], nulls(nulls, keyIndex), position));
            }
            hash = result;
        }

        @Override
        public boolean equals(Object object)
        {
            if (this == object) {
                return true;
            }
            if (!(object instanceof StructuralHashRowKey other) || kernels != other.kernels) {
                return false;
            }
            for (int keyIndex = 0; keyIndex < values.length; keyIndex++) {
                if (!kernels[keyIndex].identical(
                        values[keyIndex], nulls(nulls, keyIndex), position,
                        other.values[keyIndex], nulls(other.nulls, keyIndex), other.position)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode()
        {
            return hash;
        }

        private static Vector nulls(Vector[] nulls, int keyIndex)
        {
            return nulls.length == 0 ? null : nulls[keyIndex];
        }
    }
}
