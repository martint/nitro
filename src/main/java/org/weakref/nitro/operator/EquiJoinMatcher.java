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

import org.weakref.nitro.operator.evaluator.ir.Stream;

final class EquiJoinMatcher
        implements JoinMatcher
{
    private final int[] outerJoinColumns;
    private final int[] innerJoinColumns;

    EquiJoinMatcher(int outerJoinColumn, int innerJoinColumn)
    {
        this(new int[] {outerJoinColumn}, new int[] {innerJoinColumn});
    }

    EquiJoinMatcher(int[] outerJoinColumns, int[] innerJoinColumns)
    {
        if (outerJoinColumns.length != innerJoinColumns.length) {
            throw new IllegalArgumentException("Join key counts must match");
        }
        if (outerJoinColumns.length == 0) {
            throw new IllegalArgumentException("Equi-join requires at least one join key");
        }

        this.outerJoinColumns = outerJoinColumns.clone();
        this.innerJoinColumns = innerJoinColumns.clone();
    }

    @Override
    public boolean isCrossJoin()
    {
        return false;
    }

    @Override
    public boolean matches(Batch outerBatch, int outerPosition, BufferedJoinInput.InnerBatch innerBatch, int innerPosition)
    {
        for (int keyIndex = 0; keyIndex < outerJoinColumns.length; keyIndex++) {
            Output outerOutput = outerBatch.output(outerJoinColumns[keyIndex]);
            VectorAndNulls innerStreams = innerStreams(innerBatch, innerJoinColumns[keyIndex]);
            if (!OperatorEqualitySemantics.equal(
                    outerOutput.borrow(Stream.VALUES),
                    (org.weakref.nitro.data.BooleanVector) outerOutput.borrowOrNull(Stream.NULLS),
                    outerPosition,
                    innerStreams.values(),
                    innerStreams.nulls(),
                    innerBatch.sourcePosition(innerPosition))) {
                return false;
            }
        }
        return true;
    }

    private static VectorAndNulls innerStreams(BufferedJoinInput.InnerBatch innerBatch, int outputIndex)
    {
        if (innerBatch.retained()) {
            Output output = innerBatch.retainedBatch().output(outputIndex);
            return new VectorAndNulls(
                    output.borrow(Stream.VALUES),
                    (org.weakref.nitro.data.BooleanVector) output.borrowOrNull(Stream.NULLS));
        }

        Streams innerStreams = innerBatch.columns()[outputIndex];
        return new VectorAndNulls(
                innerStreams.values(),
                (org.weakref.nitro.data.BooleanVector) innerStreams.getOrNull(Stream.NULLS));
    }

    private record VectorAndNulls(org.weakref.nitro.data.Vector values, org.weakref.nitro.data.BooleanVector nulls) {}
}
