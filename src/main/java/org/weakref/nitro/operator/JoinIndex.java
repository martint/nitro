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

import it.unimi.dsi.fastutil.longs.LongList;
import org.weakref.nitro.data.Vector;

abstract class JoinIndex
{
    private final Vector[] noNullStreams = new Vector[0];

    abstract boolean isEmpty();

    abstract void add(Vector[] values, Vector[] nulls, int position, long rowReference);

    abstract LongList matches(Vector[] values, Vector[] nulls, int position);

    void addNoNulls(Vector[] values, int position, long rowReference)
    {
        add(values, noNullStreams, position, rowReference);
    }

    LongList matchesNoNulls(Vector[] values, int position)
    {
        return matches(values, noNullStreams, position);
    }

    void matchRows(
            Vector[] values,
            Vector[] nulls,
            boolean hasNulls,
            int[] positions,
            int positionCount,
            LongList[] matches,
            SingleLongList[] singleMatches)
    {
        for (int index = 0; index < positionCount; index++) {
            int position = positions[index];
            LongList result = hasNulls ? matches(values, nulls, position) : matchesNoNulls(values, position);
            if (result instanceof SingleLongList single) {
                matches[index] = singleMatches[index].withValue(single.getLong(0));
            }
            else {
                matches[index] = result;
            }
        }
    }

    boolean matchRowRanges(
            Vector[] values,
            Vector[] nulls,
            boolean hasNulls,
            int[] positions,
            int positionCount,
            int[] starts,
            int[] counts)
    {
        return false;
    }

    boolean supportsRowRanges()
    {
        return false;
    }

    void copyRowRange(int start, long[] output, int outputOffset, int length)
    {
        throw new UnsupportedOperationException();
    }

    boolean supportsSingleMatchRefs()
    {
        return false;
    }

    void matchSingleRows(
            Vector[] values,
            Vector[] nulls,
            boolean hasNulls,
            int[] positions,
            int positionCount,
            long[] refs)
    {
        throw new UnsupportedOperationException();
    }

    boolean supportsCompactSingleMatchRefs()
    {
        return false;
    }

    boolean supportsSingleMatchPositions()
    {
        return false;
    }

    boolean supportsSingleMatchPositionRange()
    {
        return false;
    }

    int singleMatchPositionBatchIndex()
    {
        throw new UnsupportedOperationException();
    }

    void matchSingleRowsPositions(
            Vector[] values,
            Vector[] nulls,
            boolean hasNulls,
            int[] positions,
            int positionCount,
            int[] logicalPositions)
    {
        throw new UnsupportedOperationException();
    }

    void matchSingleRowsPositionsRange(
            Vector[] values,
            Vector[] nulls,
            boolean hasNulls,
            int startPosition,
            int positionCount,
            int[] logicalPositions)
    {
        throw new UnsupportedOperationException();
    }

    boolean supportsDirectSingleMatchPositionRangeOutput()
    {
        return false;
    }

    int emitSingleRowsPositionsRange(
            Vector[] values,
            Vector[] nulls,
            boolean hasNulls,
            int startPosition,
            int positionCount,
            int[] outputOuterPositions,
            int[] outputInnerLogicalPositions,
            int outputStart)
    {
        throw new UnsupportedOperationException();
    }

    void matchSingleRowsCompact(
            Vector[] values,
            Vector[] nulls,
            boolean hasNulls,
            int[] positions,
            int positionCount,
            int[] refs)
    {
        throw new UnsupportedOperationException();
    }

    long unpackCompactSingleMatchRef(int ref)
    {
        throw new UnsupportedOperationException();
    }

    void releaseBuffers() {}
}
