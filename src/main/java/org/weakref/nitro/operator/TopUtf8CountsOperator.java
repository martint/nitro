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

import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

public class TopUtf8CountsOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("TopUtf8CountsOperator");

    private final Allocator allocator;
    private final int limit;
    private final int keyColumn;
    private final Operator source;

    private boolean done;
    private Mask resultMask;
    private BinaryVector outputValues;
    private BooleanVector outputNulls;
    private I64Vector outputCounts;
    private OperatorKeySemantics.Key reusableProbeKey;

    public TopUtf8CountsOperator(Allocator allocator, int limit, int keyColumn, Operator source)
    {
        this.allocator = allocator;
        this.limit = limit;
        this.keyColumn = keyColumn;
        this.source = source;
    }

    @Override
    public int outputCount()
    {
        return 2;
    }

    @Override
    public boolean hasNext()
    {
        return !done;
    }

    @Override
    public Batch next()
    {
        computeResults();

        Output keyOutput;
        if (outputNulls == null) {
            keyOutput = new Output(
                    Set.of(Stream.VALUES),
                    stream -> outputValues,
                    (stream, vector) -> allocator.transfer(ALLOCATION_CONTEXT, vector));
        }
        else {
            keyOutput = new Output(
                    Set.of(Stream.VALUES, Stream.NULLS),
                    stream -> switch (stream) {
                        case VALUES -> outputValues;
                        case NULLS -> outputNulls;
                        default -> throw new IllegalArgumentException("Unsupported stream: " + stream);
                    },
                    (stream, vector) -> allocator.transfer(ALLOCATION_CONTEXT, vector));
        }

        Output countOutput = new Output(
                Set.of(Stream.VALUES),
                stream -> outputCounts,
                (stream, vector) -> allocator.transfer(ALLOCATION_CONTEXT, vector));

        return new Batch(
                resultMask,
                takenMask -> allocator.transfer(ALLOCATION_CONTEXT, takenMask),
                keyOutput,
                countOutput);
    }

    @Override
    public void constrain(Mask mask)
    {
        // Results are fully materialized in next().
    }

    @Override
    public void close()
    {
        source.close();
        allocator.release(ALLOCATION_CONTEXT);
    }

    private void computeResults()
    {
        if (done) {
            return;
        }

        Object2LongOpenHashMap<OperatorKeySemantics.Key> counts = new Object2LongOpenHashMap<>();
        long nullCount = 0;

        while (source.hasNext()) {
            Batch batch = source.next();
            Mask mask = batch.borrowMask();
            if (mask.none()) {
                continue;
            }

            Output keyOutput = batch.output(keyColumn);
            Vector values = keyOutput.borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) keyOutput.borrowOrNull(Stream.NULLS);
            if (reusableProbeKey == null) {
                reusableProbeKey = OperatorKeySemantics.reusableProbeKey(values);
            }

            for (int position : mask) {
                OperatorKeySemantics.Key key = OperatorKeySemantics.probeKey(values, nulls, position, reusableProbeKey);
                if (key == null) {
                    nullCount++;
                    continue;
                }

                long currentCount = counts.getLong(key);
                if (currentCount == 0 && !counts.containsKey(key)) {
                    counts.put(OperatorKeySemantics.ownedKey(key), 1);
                }
                else {
                    counts.put(key, currentCount + 1);
                }
            }
        }

        List<TopEntry> topEntries = topEntries(counts, nullCount);
        materializeOutputs(topEntries);
        done = true;
    }

    private List<TopEntry> topEntries(Object2LongOpenHashMap<OperatorKeySemantics.Key> counts, long nullCount)
    {
        PriorityQueue<TopEntry> queue = new PriorityQueue<>(limit, (left, right) -> {
            int compare = Long.compare(left.count(), right.count());
            if (compare != 0) {
                return compare;
            }
            return compareKeys(left.bytes(), right.bytes());
        });

        for (var entry : counts.object2LongEntrySet()) {
            consider(queue, new TopEntry(bytes(entry.getKey()), entry.getLongValue(), false));
        }
        if (nullCount > 0) {
            consider(queue, new TopEntry(null, nullCount, true));
        }

        ArrayList<TopEntry> ordered = new ArrayList<>(queue);
        ordered.sort((left, right) -> {
            int compare = Long.compare(right.count(), left.count());
            if (compare != 0) {
                return compare;
            }
            return compareKeys(left.bytes(), right.bytes());
        });
        return ordered;
    }

    private void consider(PriorityQueue<TopEntry> queue, TopEntry candidate)
    {
        if (limit <= 0) {
            return;
        }
        if (queue.size() < limit) {
            queue.add(candidate);
            return;
        }
        if (queue.comparator().compare(candidate, queue.peek()) > 0) {
            queue.poll();
            queue.add(candidate);
        }
    }

    private void materializeOutputs(List<TopEntry> topEntries)
    {
        int size = topEntries.size();
        int totalBytes = 0;
        boolean asciiOnly = true;
        boolean hasNull = false;
        for (TopEntry entry : topEntries) {
            if (entry.isNull()) {
                hasNull = true;
                continue;
            }
            totalBytes += entry.bytes().length;
            asciiOnly &= isAscii(entry.bytes());
        }

        outputValues = allocator.allocateBinary(ALLOCATION_CONTEXT, size, totalBytes);
        outputValues.addTrait(BinaryVector.Trait.UTF8_STRING);
        if (asciiOnly) {
            outputValues.addTrait(BinaryVector.Trait.ASCII_ONLY);
        }
        outputCounts = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, size, I64Vector::new);
        outputNulls = hasNull ? allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, size, BooleanVector::new) : null;

        for (int index = 0; index < size; index++) {
            TopEntry entry = topEntries.get(index);
            outputCounts.values()[index] = entry.count();
            if (entry.isNull()) {
                outputValues.setNull(index);
                outputNulls.values()[index] = true;
            }
            else {
                outputValues.setBytes(index, entry.bytes());
            }
        }

        resultMask = allocator.allocateRangeMask(ALLOCATION_CONTEXT, 0, size);
    }

    private static byte[] bytes(OperatorKeySemantics.Key key)
    {
        return switch (key) {
            case OperatorKeySemantics.BinaryKey binary -> binary.bytes();
            default -> throw new IllegalArgumentException("Expected binary key but found " + key.getClass().getSimpleName());
        };
    }

    private static int compareKeys(byte[] left, byte[] right)
    {
        if (left == null || right == null) {
            if (left == right) {
                return 0;
            }
            return left == null ? 1 : -1;
        }

        int sharedLength = Math.min(left.length, right.length);
        for (int index = 0; index < sharedLength; index++) {
            int compare = Integer.compare(Byte.toUnsignedInt(left[index]), Byte.toUnsignedInt(right[index]));
            if (compare != 0) {
                return compare;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    private static boolean isAscii(byte[] value)
    {
        for (byte current : value) {
            if ((current & 0x80) != 0) {
                return false;
            }
        }
        return true;
    }

    private record TopEntry(byte[] bytes, long count, boolean isNull) {}
}
