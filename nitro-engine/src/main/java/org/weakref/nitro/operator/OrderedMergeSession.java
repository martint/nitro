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

import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.UnorderedPlacement;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;

import java.util.PriorityQueue;

import static java.util.Objects.checkIndex;
import static java.util.Objects.requireNonNull;

/**
 * Host-driven streaming merge of independently ordered batch sources.
 *
 * <p>The host supplies at most one batch for each source. Output becomes available only when every unfinished source
 * has a head row, since a missing head could precede every buffered row. Produced batches own destination buffers and
 * are therefore independent of subsequent source progress.
 */
public final class OrderedMergeSession
        implements AutoCloseable
{
    private final Allocator allocator;
    private final Schema schema;
    private final Ordering[] orderings;
    private final StructuralComparisonKernel[] comparisonKernels;
    private final JoinBufferPolicy bufferPolicy;
    private final SourceCursor[] sources;
    private final int maxOutputRows;
    private final Object outputPoolGroup = new Object();

    private Batch outstandingOutput;
    private boolean closed;

    public OrderedMergeSession(
            Allocator allocator,
            int[] orderingColumns,
            boolean[] descending,
            boolean[] nullsFirst,
            Schema schema,
            int sourceCount,
            int maxOutputRows,
            OperatorResources resources)
    {
        this(allocator, schema, sourceCount, orderings(orderingColumns, descending, nullsFirst), maxOutputRows, resources);
    }

    private OrderedMergeSession(
            Allocator allocator,
            Schema schema,
            int sourceCount,
            Ordering[] orderings,
            int maxOutputRows,
            OperatorResources resources)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.schema = requireNonNull(schema, "schema is null");
        if (sourceCount < 0) {
            throw new IllegalArgumentException("sourceCount must be non-negative");
        }
        this.orderings = requireNonNull(orderings, "orderings is null").clone();
        if (orderings.length == 0) {
            throw new IllegalArgumentException("orderings is empty");
        }
        if (maxOutputRows <= 0) {
            throw new IllegalArgumentException("maxOutputRows must be positive");
        }
        this.maxOutputRows = maxOutputRows;
        this.sources = new SourceCursor[sourceCount];
        this.comparisonKernels = new StructuralComparisonKernel[orderings.length];
        OperatorResources operatorResources = requireNonNull(resources, "resources is null");
        bufferPolicy = operatorResources.joinBufferPolicy();
        StructuralTypeKernelFactory structuralTypes = operatorResources
                .codeGeneration()
                .structuralTypes();
        for (int index = 0; index < orderings.length; index++) {
            Ordering ordering = requireNonNull(orderings[index], "ordering is null");
            checkIndex(ordering.column(), schema.size());
            comparisonKernels[index] = structuralTypes.comparison(
                    schema.field(ordering.column()).type(),
                    ordering.nullsFirst() ? UnorderedPlacement.FIRST : UnorderedPlacement.LAST);
        }
    }

    private static Ordering[] orderings(int[] columns, boolean[] descending, boolean[] nullsFirst)
    {
        requireNonNull(columns, "orderingColumns is null");
        requireNonNull(descending, "descending is null");
        requireNonNull(nullsFirst, "nullsFirst is null");
        if (columns.length == 0 || columns.length != descending.length || columns.length != nullsFirst.length) {
            throw new IllegalArgumentException("ordering arrays must be nonempty and have the same length");
        }
        Ordering[] result = new Ordering[columns.length];
        for (int index = 0; index < columns.length; index++) {
            result[index] = new Ordering(columns[index], descending[index], nullsFirst[index]);
        }
        return result;
    }

    public int sourceCount()
    {
        return sources.length;
    }

    public boolean needsInput(int source)
    {
        checkOpen();
        SourceCursor cursor = sources[checkIndex(source, sources.length)];
        return cursor == null || (cursor.batch == null && !cursor.finished);
    }

    /** Takes ownership of {@code batch}. Empty batches are closed immediately. */
    public void addInput(int source, Batch batch)
    {
        checkOpen();
        int sourceIndex = checkIndex(source, sources.length);
        requireNonNull(batch, "batch is null");
        SourceCursor cursor = sources[sourceIndex];
        if (cursor == null) {
            cursor = new SourceCursor(sourceIndex);
            sources[sourceIndex] = cursor;
        }
        if (cursor.finished) {
            batch.close();
            throw new IllegalStateException("source is finished: " + sourceIndex);
        }
        if (cursor.batch != null) {
            batch.close();
            throw new IllegalStateException("source already has input: " + sourceIndex);
        }
        if (batch.borrowMask().none()) {
            batch.close();
            return;
        }
        cursor.setBatch(batch);
    }

    /** Declares that no batch will follow the currently buffered batch, if any. */
    public void finishSource(int source)
    {
        checkOpen();
        int sourceIndex = checkIndex(source, sources.length);
        SourceCursor cursor = sources[sourceIndex];
        if (cursor == null) {
            cursor = new SourceCursor(sourceIndex);
            sources[sourceIndex] = cursor;
        }
        cursor.finished = true;
    }

    public boolean hasOutput()
    {
        checkOpen();
        if (outstandingOutput != null) {
            return false;
        }
        boolean hasRows = false;
        for (SourceCursor cursor : sources) {
            if (cursor == null || (cursor.batch == null && !cursor.finished)) {
                return false;
            }
            hasRows |= cursor.batch != null;
        }
        return hasRows;
    }

    public boolean isFinished()
    {
        checkOpen();
        if (outstandingOutput != null) {
            return false;
        }
        for (SourceCursor cursor : sources) {
            if (cursor == null || !cursor.finished || cursor.batch != null) {
                return false;
            }
        }
        return true;
    }

    public Batch getOutput()
    {
        checkOpen();
        if (!hasOutput()) {
            throw new IllegalStateException("ordered merge has no output");
        }

        Allocator.Context outputContext = new Allocator.Context("OrderedMergeSession.output", outputPoolGroup);
        JoinBufferSupport buffers = new JoinBufferSupport(
                bufferPolicy,
                allocator,
                outputContext);
        Streams[] columns = new Streams[schema.size()];
        PriorityQueue<SourceCursor> queue = new PriorityQueue<>(this::compare);
        for (SourceCursor cursor : sources) {
            if (cursor.batch != null) {
                queue.add(cursor);
            }
        }

        int outputCount = 0;
        try {
            while (outputCount < maxOutputRows && !queue.isEmpty()) {
                SourceCursor cursor = queue.remove();
                int sourcePosition = cursor.position();
                for (int column = 0; column < columns.length; column++) {
                    columns[column] = buffers.copySinglePositionFresh(
                            cursor.batch.output(column),
                            columns[column],
                            maxOutputRows,
                            outputCount,
                            sourcePosition);
                }
                outputCount++;

                if (cursor.advance()) {
                    queue.add(cursor);
                    continue;
                }
                if (!cursor.finished) {
                    // The next batch from this source may contain the next global row.
                    break;
                }
            }

            int positions = outputCount;
            Mask mask = allocator.allocateRangeMask(outputContext, 0, positions);
            Output[] outputs = new Output[columns.length];
            for (int column = 0; column < outputs.length; column++) {
                Streams streams = columns[column];
                outputs[column] = new Output(
                        streams.streams(),
                        streams::get,
                        (_, vector) -> allocator.transfer(outputContext, vector),
                        (_, _) -> {});
            }
            Batch[] holder = new Batch[1];
            Batch result = new Batch(
                    mask,
                    _ -> {},
                    taken -> allocator.transfer(outputContext, taken),
                    ignored -> {},
                    () -> {
                        allocator.release(outputContext);
                        if (outstandingOutput == holder[0]) {
                            outstandingOutput = null;
                        }
                    },
                    outputs);
            holder[0] = result;
            outstandingOutput = result;
            return result;
        }
        catch (RuntimeException | Error failure) {
            allocator.release(outputContext);
            throw failure;
        }
    }

    private int compare(SourceCursor left, SourceCursor right)
    {
        int leftPosition = left.position();
        int rightPosition = right.position();
        for (int index = 0; index < orderings.length; index++) {
            Ordering ordering = orderings[index];
            Vector leftNulls = left.nulls[index];
            Vector rightNulls = right.nulls[index];
            boolean leftNull = OperatorVectorSupport.isNull(leftNulls, leftPosition);
            boolean rightNull = OperatorVectorSupport.isNull(rightNulls, rightPosition);
            if (leftNull || rightNull) {
                if (leftNull == rightNull) {
                    continue;
                }
                return leftNull == ordering.nullsFirst() ? -1 : 1;
            }
            int comparison = comparisonKernels[index].compare(
                    left.values[index],
                    leftNulls,
                    leftPosition,
                    right.values[index],
                    rightNulls,
                    rightPosition);
            if (comparison != 0) {
                if (!ordering.descending()) {
                    return comparison;
                }
                // A comparison kernel is allowed to return Integer.MIN_VALUE, which cannot be safely negated.
                return comparison < 0 ? 1 : -1;
            }
        }
        return Integer.compare(left.source, right.source);
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("ordered merge session is closed");
        }
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        RuntimeException failure = null;
        if (outstandingOutput != null) {
            Batch output = outstandingOutput;
            outstandingOutput = null;
            try {
                output.close();
            }
            catch (RuntimeException e) {
                failure = e;
            }
        }
        for (SourceCursor cursor : sources) {
            if (cursor != null) {
                try {
                    cursor.closeBatch();
                }
                catch (RuntimeException e) {
                    if (failure == null) {
                        failure = e;
                    }
                    else {
                        failure.addSuppressed(e);
                    }
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    public record Ordering(int column, boolean descending, boolean nullsFirst) {}

    private final class SourceCursor
    {
        private final int source;
        private final Vector[] values = new Vector[orderings.length];
        private final Vector[] nulls = new Vector[orderings.length];
        private Batch batch;
        private int selectedIndex;
        private boolean finished;

        private SourceCursor(int source)
        {
            this.source = source;
        }

        private void setBatch(Batch batch)
        {
            this.batch = batch;
            selectedIndex = 0;
            for (int index = 0; index < orderings.length; index++) {
                Output output = batch.output(orderings[index].column());
                values[index] = output.borrow(Stream.VALUES);
                nulls[index] = output.borrowOrNull(Stream.NULLS);
            }
        }

        private int position()
        {
            return batch.borrowMask().position(selectedIndex);
        }

        /** Returns whether this cursor still has a row. */
        private boolean advance()
        {
            selectedIndex++;
            if (selectedIndex < batch.borrowMask().selectedCount()) {
                return true;
            }
            closeBatch();
            return false;
        }

        private void closeBatch()
        {
            Batch current = batch;
            batch = null;
            java.util.Arrays.fill(values, null);
            java.util.Arrays.fill(nulls, null);
            selectedIndex = 0;
            if (current != null) {
                current.close();
            }
        }
    }
}
