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

import it.unimi.dsi.fastutil.longs.AbstractLongList;

/**
 * Reusable view over one key's build rows, threaded through a shared chain ({@code next}) starting at
 * {@code head}. Reading is cursor-cached so sequential access is O(1) per element; out-of-order access walks
 * again from the head.
 */
final class ChainLongList
        extends AbstractLongList
{
    private long[] rows;
    private int[] compactRows;
    private int[] next;
    private int head;
    private int length;
    private int cursorIndex;
    private int cursorOrdinal;
    private int compactBatchIndex = -1;
    private boolean rangeMode;
    private boolean repeatedMode;
    private long repeatedValue;

    ChainLongList reset(long[] rows, int[] next, int head, int length)
    {
        this.rows = rows;
        this.compactRows = null;
        this.next = next;
        this.head = head;
        this.length = length;
        this.cursorIndex = 0;
        this.cursorOrdinal = head;
        this.rangeMode = false;
        this.repeatedMode = false;
        return this;
    }

    ChainLongList resetCompact(int[] rows, int[] next, int head, int length)
    {
        this.rows = null;
        this.compactRows = rows;
        this.next = next;
        this.head = head;
        this.length = length;
        this.cursorIndex = 0;
        this.cursorOrdinal = head;
        this.rangeMode = false;
        this.repeatedMode = false;
        this.compactBatchIndex = -1;
        return this;
    }

    ChainLongList resetCompactSingleBatch(int[] rows, int[] next, int head, int length, int batchIndex)
    {
        resetCompact(rows, next, head, length);
        this.compactBatchIndex = batchIndex;
        return this;
    }

    ChainLongList resetRange(long[] rows, int base, int length)
    {
        this.rows = rows;
        this.compactRows = null;
        this.head = base;
        this.length = length;
        this.rangeMode = true;
        this.repeatedMode = false;
        return this;
    }

    ChainLongList resetRepeated(long value, int length)
    {
        this.repeatedValue = value;
        this.length = length;
        this.rangeMode = false;
        this.repeatedMode = true;
        return this;
    }

    @Override
    public long getLong(int index)
    {
        if (index < 0 || index >= length) {
            throw new IndexOutOfBoundsException("index " + index);
        }
        if (repeatedMode) {
            return repeatedValue;
        }
        if (rangeMode) {
            return rows[head + index];
        }
        if (index < cursorIndex) {
            cursorIndex = 0;
            cursorOrdinal = head;
        }
        while (cursorIndex < index) {
            cursorOrdinal = next[cursorOrdinal];
            cursorIndex++;
        }
        if (compactRows == null) {
            return rows[cursorOrdinal];
        }
        return compactBatchIndex >= 0
                ? JoinRowReference.pack(compactBatchIndex, compactRows[cursorOrdinal])
                : JoinRowReference.unpackCompact(compactRows[cursorOrdinal]);
    }

    @Override
    public int size()
    {
        return length;
    }

    int storageIndex(int index)
    {
        if (repeatedMode) {
            return -1;
        }
        if (rangeMode) {
            return head + index;
        }
        if (index < cursorIndex) {
            cursorIndex = 0;
            cursorOrdinal = head;
        }
        while (cursorIndex < index) {
            cursorOrdinal = next[cursorOrdinal];
            cursorIndex++;
        }
        return cursorOrdinal;
    }

    static ChainLongList[] createArray(int size)
    {
        ChainLongList[] matches = new ChainLongList[size];
        for (int index = 0; index < size; index++) {
            matches[index] = new ChainLongList();
        }
        return matches;
    }
}
