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

final class ObjectJoinIndex
        extends JoinIndex
{
    private final Map<OperatorKeySemantics.Key, LongArrayList> rowsByKey = new HashMap<>();
    private final OperatorKeySemantics.Key[] innerProbeKeys;
    private final OperatorKeySemantics.Key[] outerProbeKeys;
    private final OperatorKeySemantics.CompositeProbeKey innerCompositeProbeKey;
    private final OperatorKeySemantics.CompositeProbeKey outerCompositeProbeKey;

    ObjectJoinIndex(int keyCount)
    {
        this.innerProbeKeys = new OperatorKeySemantics.Key[keyCount];
        this.outerProbeKeys = new OperatorKeySemantics.Key[keyCount];
        this.innerCompositeProbeKey = keyCount > 1 ? OperatorKeySemantics.reusableCompositeProbeKey(keyCount) : null;
        this.outerCompositeProbeKey = keyCount > 1 ? OperatorKeySemantics.reusableCompositeProbeKey(keyCount) : null;
    }

    @Override
    public boolean isEmpty()
    {
        return rowsByKey.isEmpty();
    }

    @Override
    public void add(Vector[] values, Vector[] nulls, int position, long rowReference)
    {
        OperatorKeySemantics.Key key = keyForPosition(values, nulls, position, innerProbeKeys, innerCompositeProbeKey);
        if (key == null) {
            return;
        }
        LongArrayList rows = rowsByKey.get(key);
        if (rows == null) {
            rows = new LongArrayList();
            rowsByKey.put(OperatorKeySemantics.ownedKey(key), rows);
        }
        rows.add(rowReference);
    }

    @Override
    public LongList matches(Vector[] values, Vector[] nulls, int position)
    {
        OperatorKeySemantics.Key key = keyForPosition(values, nulls, position, outerProbeKeys, outerCompositeProbeKey);
        if (key == null) {
            return LongLists.emptyList();
        }
        LongArrayList rows = rowsByKey.get(key);
        return rows == null ? LongLists.emptyList() : rows;
    }

    private static OperatorKeySemantics.Key keyForPosition(
            Vector[] values,
            Vector[] nulls,
            int position,
            OperatorKeySemantics.Key[] reusableProbeKeys,
            OperatorKeySemantics.CompositeProbeKey reusableCompositeProbeKey)
    {
        for (int keyIndex = 0; keyIndex < values.length; keyIndex++) {
            if (reusableProbeKeys[keyIndex] == null) {
                reusableProbeKeys[keyIndex] = OperatorKeySemantics.reusableProbeKey(values[keyIndex]);
            }
            OperatorKeySemantics.Key key = OperatorKeySemantics.probeKey(
                    values[keyIndex],
                    nulls[keyIndex],
                    position,
                    reusableProbeKeys[keyIndex]);
            if (key == null) {
                return null;
            }
            reusableProbeKeys[keyIndex] = key;
        }
        return OperatorKeySemantics.probeCompositeKey(reusableProbeKeys, reusableCompositeProbeKey);
    }
}
