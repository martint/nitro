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

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Vector;

import java.util.Arrays;

final class DistinctKeySet
{
    private final DistinctIndex index;

    private DistinctKeySet(DistinctIndex index)
    {
        this.index = index;
    }

    public static DistinctKeySet create(Vector[] samples)
    {
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(samples);
        if (layout != null) {
            return new DistinctKeySet(new FlatDistinctIndex(layout, Math.max(16, samples[0].length())));
        }
        return new DistinctKeySet(new ObjectDistinctIndex(samples.length));
    }

    public boolean add(Vector[] values, BooleanVector[] nulls, int position)
    {
        return index.add(values, nulls, position);
    }

    private interface DistinctIndex
    {
        boolean add(Vector[] values, BooleanVector[] nulls, int position);
    }

    private static final class FlatDistinctIndex
            implements DistinctIndex
    {
        private final FlatGroupingTable table;
        private long nextGroupId;

        private FlatDistinctIndex(FlatKeyLayout layout, int expectedSize)
        {
            this.table = new FlatGroupingTable(layout, expectedSize);
        }

        @Override
        public boolean add(Vector[] values, BooleanVector[] nulls, int position)
        {
            if (hasNull(nulls, position)) {
                return false;
            }
            long newGroupId = nextGroupId;
            long assigned = table.assignGroup(values, position, newGroupId);
            if (assigned == newGroupId) {
                nextGroupId++;
                return true;
            }
            return false;
        }

        private static boolean hasNull(BooleanVector[] nulls, int position)
        {
            for (BooleanVector nullsVector : nulls) {
                if (OperatorVectorSupport.isNull(nullsVector, position)) {
                    return true;
                }
            }
            return false;
        }
    }

    private static final class ObjectDistinctIndex
            implements DistinctIndex
    {
        private final ObjectOpenHashSet<Object> keys = new ObjectOpenHashSet<>();
        private final OperatorKeySemantics.Key[] probeKeys;
        private final OperatorKeySemantics.CompositeProbeKey compositeProbeKey;

        private ObjectDistinctIndex(int keyCount)
        {
            this.probeKeys = new OperatorKeySemantics.Key[keyCount];
            this.compositeProbeKey = keyCount > 1 ? OperatorKeySemantics.reusableCompositeProbeKey(keyCount) : null;
        }

        @Override
        public boolean add(Vector[] values, BooleanVector[] nulls, int position)
        {
            OperatorKeySemantics.Key key = keyForPosition(values, nulls, position);
            if (key == null) {
                return false;
            }
            if (keys.contains(key)) {
                return false;
            }
            keys.add(OperatorKeySemantics.ownedKey(key));
            return true;
        }

        private OperatorKeySemantics.Key keyForPosition(Vector[] values, BooleanVector[] nulls, int position)
        {
            for (int keyIndex = 0; keyIndex < values.length; keyIndex++) {
                if (probeKeys[keyIndex] == null) {
                    probeKeys[keyIndex] = OperatorKeySemantics.reusableProbeKey(values[keyIndex]);
                }
                OperatorKeySemantics.Key key = OperatorKeySemantics.probeKey(values[keyIndex], nulls[keyIndex], position, probeKeys[keyIndex]);
                if (key == null) {
                    return null;
                }
                probeKeys[keyIndex] = key;
            }
            if (probeKeys.length == 1) {
                return probeKeys[0];
            }
            return OperatorKeySemantics.probeCompositeKey(Arrays.copyOf(probeKeys, probeKeys.length), compositeProbeKey);
        }
    }
}
