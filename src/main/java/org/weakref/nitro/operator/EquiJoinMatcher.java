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
    private final int outerJoinColumn;
    private final int innerJoinColumn;

    EquiJoinMatcher(int outerJoinColumn, int innerJoinColumn)
    {
        this.outerJoinColumn = outerJoinColumn;
        this.innerJoinColumn = innerJoinColumn;
    }

    @Override
    public boolean isCrossJoin()
    {
        return false;
    }

    @Override
    public boolean matches(Batch outerBatch, int outerPosition, Streams[] innerColumns, int innerPosition)
    {
        Output outerOutput = outerBatch.output(outerJoinColumn);
        Streams innerStreams = innerColumns[innerJoinColumn];
        return OperatorEqualitySemantics.equal(
                outerOutput.borrow(Stream.VALUES),
                (org.weakref.nitro.data.BooleanVector) outerOutput.borrowOrNull(Stream.NULLS),
                outerPosition,
                innerStreams.values(),
                (org.weakref.nitro.data.BooleanVector) innerStreams.getOrNull(Stream.NULLS),
                innerPosition);
    }
}
