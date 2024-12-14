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
package org.weakref.nitro.data;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

// TODO: support hierarchical contexts
public class Allocator
{
    private final Map<Context, Stats> stats = new HashMap<>();

    /**
     * Calculates the capacity of a vector that can hold the desired size, plus some extra space.
     * The extra space is roughly ~2x for small vector sizes and decreases logarithmically as the size of the vector increases
     * to avoid over-allocating too much unused space.
     */
    public static int computeCapacity(int desiredSize)
    {
        // TODO: verify formula
        double growthFactor = 1 + 1.0 / (Math.log(desiredSize + 1) - 6);
        return (int) (desiredSize + desiredSize * growthFactor);
    }

    public Vector allocate(Context context, int size)
    {
        recordAllocation(context, size);

        return new LongVector(size);
    }

    public Vector allocateOrGrow(Context context, Vector vector, int size)
    {
        if (vector == null) {
            vector = allocate(context, size);
        }
        else if (vector.length() < size) {
            recordAllocation(context, -vector.length());
            recordAllocation(context, size);

            LongVector longVector = (LongVector) vector;
            vector = new LongVector(
                    Arrays.copyOf(longVector.nulls(), size),
                    Arrays.copyOf(longVector.values(), size));
        }

        return vector;
    }

    public Vector reallocateIfNecessary(Context context, Vector vector, int count)
    {
        if (vector == null) {
            return allocate(context, count);
        }

        if (vector.length() < count) {
            recordAllocation(context, -vector.length());
            vector = allocate(context, count);
        }

        return vector;
    }

    private void recordAllocation(Context context, int size)
    {
        stats.computeIfAbsent(context, _ -> new Stats()).record(size * Long.BYTES);
    }

    @Override
    public String toString()
    {
        return stats.entrySet().stream()
                .map(e -> "%s: total=%s, peak=%s, current=%s".formatted(
                        e.getKey().name(),
                        e.getValue().total(),
                        e.getValue().peak(),
                        e.getValue().current()))
                .collect(Collectors.joining("\n"));
    }

    public void release(Context context)
    {
        stats.computeIfAbsent(context, _ -> new Stats()).release();
    }

    // TODO: track amount of reallocated memory (i.e., how much effort is wasted due to potentially poor allocation strategies)
    private static class Stats
    {
        private long total;
        private long peak;
        private long current;

        public void record(long bytes)
        {
            if (bytes > 0) {
                total += bytes;
            }
            current += bytes;
            peak = Math.max(peak, current);
        }

        public void release()
        {
            current = 0;
        }

        public long total()
        {
            return total;
        }

        public long current()
        {
            return current;
        }

        public long peak()
        {
            return peak;
        }
    }

    public record Context(String name) {}
}
