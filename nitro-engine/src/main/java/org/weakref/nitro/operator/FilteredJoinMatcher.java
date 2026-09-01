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

import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.operator.HashJoinOperator.JoinFilter;

import static java.util.Objects.requireNonNull;

/** Evaluates registry-bound predicates before a nested-loop candidate is emitted. */
final class FilteredJoinMatcher
        implements JoinMatcher
{
    private final JoinFilter[] filters;
    private boolean[] dictionaryScratch = new boolean[0];

    FilteredJoinMatcher(JoinFilter[] filters)
    {
        this.filters = requireNonNull(filters, "filters is null").clone();
        if (filters.length == 0) {
            throw new IllegalArgumentException("filtered nested-loop join requires at least one predicate");
        }
    }

    @Override
    public boolean producesFullCrossProduct()
    {
        return false;
    }

    @Override
    public boolean supportsPerPositionEmission()
    {
        return true;
    }

    @Override
    public boolean supportsOuterMaskPruning(Batch outerBatch, BufferedJoinInput.InnerBatch innerBatch)
    {
        if (filters.length != 1 || filters[0].encodedBinaryEquals()) {
            return false;
        }
        JoinFilter filter = filters[0];
        Output outer = outerBatch.output(filter.outerColumn());
        InnerStreams inner = innerStreams(innerBatch, filter.innerColumn());
        if (!allValid(outer.borrowOrNull(Stream.NULLS)) ||
                !allValid(outer.borrowOrNull(Stream.ERRORS)) ||
                !allValid(inner.nulls()) ||
                !allValid(inner.errors())) {
            return false;
        }
        return filter.function().supportsOuterMaskPruning(outer.borrow(Stream.VALUES));
    }

    @Override
    public void pruneOuterMask(Batch outerBatch, Mask mask, BufferedJoinInput.InnerBatch innerBatch, int innerPosition)
    {
        JoinFilter filter = filters[0];
        Vector outer = outerBatch.output(filter.outerColumn()).borrow(Stream.VALUES);
        InnerStreams inner = innerStreams(innerBatch, filter.innerColumn());
        int dictionarySize = outer instanceof DictionaryVector dictionary ? dictionary.values().length() : 0;
        if (dictionaryScratch.length != dictionarySize) {
            dictionaryScratch = new boolean[dictionarySize];
        }
        if (!filter.function().pruneOuterMask(outer, mask, inner.values(), innerBatch.sourcePosition(innerPosition), dictionaryScratch)) {
            throw new IllegalStateException("join filter declined advertised outer-mask pruning");
        }
    }

    @Override
    public boolean matches(Batch outerBatch, int outerPosition, BufferedJoinInput.InnerBatch innerBatch, int innerPosition)
    {
        int sourceInnerPosition = innerBatch.sourcePosition(innerPosition);
        for (JoinFilter filter : filters) {
            Output outer = outerBatch.output(filter.outerColumn());
            InnerStreams inner = innerStreams(innerBatch, filter.innerColumn());
            Vector outerNulls = outer.borrowOrNull(Stream.NULLS);
            if ((outerNulls != null && VectorAccess.isNull(outerNulls, outerPosition)) ||
                    (inner.nulls() != null && VectorAccess.isNull(inner.nulls(), sourceInnerPosition))) {
                return false;
            }
            if (filter.encodedBinaryEquals()) {
                if (!OperatorVectorSupport.binaryEquals(
                        outer.borrow(Stream.VALUES),
                        outerPosition,
                        inner.values(),
                        sourceInnerPosition)) {
                    return false;
                }
            }
            else if (!filter.function().test(
                    outer.borrow(Stream.VALUES),
                    outerPosition,
                    inner.values(),
                    sourceInnerPosition)) {
                return false;
            }
        }
        return true;
    }

    private static InnerStreams innerStreams(BufferedJoinInput.InnerBatch batch, int column)
    {
        if (batch.retained()) {
            Output output = batch.retainedBatch().output(column);
            return new InnerStreams(output.borrow(Stream.VALUES), output.borrowOrNull(Stream.NULLS), output.borrowOrNull(Stream.ERRORS));
        }
        return new InnerStreams(batch.columns()[column].values(), batch.columns()[column].getOrNull(Stream.NULLS), batch.columns()[column].getOrNull(Stream.ERRORS));
    }

    private static boolean allValid(Vector vector)
    {
        return vector == null || VectorAccess.isAllFalseNulls(vector);
    }

    private record InnerStreams(Vector values, Vector nulls, Vector errors) {}
}
