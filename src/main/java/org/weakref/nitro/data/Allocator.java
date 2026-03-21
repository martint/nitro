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

import org.weakref.nitro.operator.Streams;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

// TODO: support hierarchical contexts
public class Allocator
{
    private static final int MAX_POOLED_MASKS_PER_BUCKET = 4;

    private final Map<Context, ContextState> states = new HashMap<>();

    /**
     * Calculates the capacity of a vector that can hold the desired size, plus some extra space.
     * The extra space is roughly ~2x for small vector sizes and decreases logarithmically as the size of the vector increases
     * to avoid over-allocating too much unused space.
     */
    public static int computeCapacity(int desiredSize)
    {
        if (desiredSize <= 0) {
            return 0;
        }
        // TODO: verify formula
        double growthFactor = 1 + 1.0 / (Math.log(desiredSize + 1) - 6);
        return Math.max(desiredSize, (int) (desiredSize + desiredSize * growthFactor));
    }

    public <T extends Vector> T allocate(Context context, Class<T> vectorType, int size, IntFunction<T> allocator)
    {
        ContextState state = state(context);
        Vector vector = state.borrowVector(new Vector.PoolRequest(vectorType, size, true), vectorType);
        boolean reused = vector != null;
        if (!reused) {
            vector = allocator.apply(size);
        }
        else {
            vector.clearForReuse();
        }

        @SuppressWarnings("unchecked")
        T typedVector = (T) vectorType.cast(vector);
        state.trackVector(typedVector, reused);
        return typedVector;
    }

    public <T extends Vector> T adopt(Context context, T vector)
    {
        state(context).trackVector(requireNonNull(vector, "vector is null"), false);
        return vector;
    }

    public BinaryVector allocateBinary(Context context, int positionCount, int byteCapacity)
    {
        ContextState state = state(context);
        BinaryVector vector = state.borrowVector(new Vector.PoolRequest(BinaryVector.poolFamily(positionCount), byteCapacity, false), BinaryVector.class);
        boolean reused = vector != null;
        if (!reused) {
            vector = new BinaryVector(positionCount, byteCapacity);
        }
        else {
            vector.clearForReuse();
        }
        state.trackVector(vector, reused);
        return vector;
    }

    public DictionaryVector allocateDictionary(Context context, int[] ids, Vector values)
    {
        DictionaryVector vector = new DictionaryVector(ids, values);
        state(context).trackVector(vector, false);
        return vector;
    }

    public RleVector allocateRle(Context context, int[] counts, Vector values)
    {
        RleVector vector = new RleVector(Arrays.copyOf(counts, counts.length), values);
        state(context).trackVector(vector, false);
        return vector;
    }

    public BinaryVector allocateOrGrowBinary(Context context, BinaryVector vector, int positionCount, int byteCapacity)
    {
        if (vector == null) {
            return allocateBinary(context, positionCount, growthCapacity(byteCapacity));
        }
        if (vector.length() < positionCount || vector.byteCapacity() < byteCapacity) {
            BinaryVector grown = allocateBinary(context, positionCount, growthCapacity(byteCapacity));
            System.arraycopy(vector.offsets(), 0, grown.offsets(), 0, vector.length() + 1);
            int bytesUsed = Arrays.stream(vector.offsets()).max().orElse(0);
            System.arraycopy(vector.data(), 0, grown.data(), 0, bytesUsed);
            grown.addTraits(vector.traits());
            discardVector(context, vector);
            return grown;
        }
        return vector;
    }

    public ArrayVector allocateArray(Context context, int positionCount)
    {
        return allocate(context, ArrayVector.class, positionCount, ArrayVector::new);
    }

    public MapVector allocateMap(Context context, int positionCount)
    {
        return allocate(context, MapVector.class, positionCount, MapVector::new);
    }

    public <T extends Vector> T allocateOrGrow(Context context, T vector, Class<T> vectorType, int size, IntFunction<T> vectorAllocator)
    {
        if (vector == null) {
            return allocate(context, vectorType, size, vectorAllocator);
        }
        if (vector.length() < size) {
            T grown = allocate(context, vectorType, size, vectorAllocator);
            vector.copyInto(grown);
            discardVector(context, vector);
            return grown;
        }
        return vector;
    }

    public <T extends Vector> T reallocateIfNecessary(Context context, T vector, Class<T> vectorType, int count, IntFunction<T> vectorAllocator)
    {
        if (vector == null) {
            return allocate(context, vectorType, count, vectorAllocator);
        }

        if (vector.length() < count) {
            T grown = allocate(context, vectorType, count, vectorAllocator);
            vector.copyInto(grown);
            discardVector(context, vector);
            return grown;
        }

        return vector;
    }

    public Mask allocateAllMask(Context context, int size)
    {
        ContextState state = state(context);
        Mask mask = state.borrowMask(0);
        boolean reused = mask != null;
        if (!reused) {
            mask = Mask.all(size);
        }
        else {
            mask.selectAll(size);
        }
        state.trackMask(mask, reused);
        return mask;
    }

    public Mask allocateRangeMask(Context context, int start, int length)
    {
        ContextState state = state(context);
        Mask mask = state.borrowMask(length);
        boolean reused = mask != null;
        if (!reused) {
            mask = Mask.range(start, length);
        }
        else if (start == 0) {
            mask.selectAll(length);
        }
        else {
            int[] positions = mask.positionsArray(length);
            for (int index = 0; index < length; index++) {
                positions[index] = start + index;
            }
            mask.setSelection(start + length, length, false);
        }
        state.trackMask(mask, reused);
        return mask;
    }

    public Mask allocateSparseMask(Context context, int[] activePositions, int totalPositions)
    {
        ContextState state = state(context);
        Mask mask = state.borrowMask(activePositions.length);
        boolean reused = mask != null;
        if (!reused) {
            mask = Mask.sparse(activePositions, totalPositions);
        }
        else if (activePositions.length == totalPositions && isAllPositions(activePositions, totalPositions)) {
            mask.selectAll(totalPositions);
        }
        else {
            int[] positions = mask.positionsArray(activePositions.length);
            System.arraycopy(activePositions, 0, positions, 0, activePositions.length);
            mask.setSelection(totalPositions, activePositions.length, false);
        }
        state.trackMask(mask, reused);
        return mask;
    }

    public Mask intersectMask(Context context, Mask mask, BooleanVector other)
    {
        if (other.length() == 0 || mask.none()) {
            return allocateSparseMask(context, new int[0], mask.size());
        }

        ContextState state = state(context);
        Mask result = state.borrowMask(mask.selectedCount());
        boolean reused = result != null;
        if (!reused) {
            result = mask.and(other);
        }
        else {
            int[] positions = result.positionsArray(mask.selectedCount());
            int selectedCount = 0;
            if (mask.all()) {
                for (int position = 0; position < mask.size(); position++) {
                    if (other.values()[position]) {
                        positions[selectedCount++] = position;
                    }
                }
            }
            else {
                for (int index = 0; index < mask.selectedCount(); index++) {
                    int position = mask.position(index);
                    if (other.values()[position]) {
                        positions[selectedCount++] = position;
                    }
                }
            }
            if (selectedCount == mask.size() && isAllPositions(positions, selectedCount)) {
                result.selectAll(mask.size());
            }
            else {
                result.setSelection(mask.size(), selectedCount, false);
            }
        }
        state.trackMask(result, reused);
        return result;
    }

    public Mask intersectMask(Context context, Mask left, Mask right)
    {
        ContextState state = state(context);
        Mask result = state.borrowMask(Math.min(left.selectedCount(), right.selectedCount()));
        boolean reused = result != null;
        if (!reused) {
            result = left.difference(left.difference(right));
        }
        else if (left.none() || right.none()) {
            result.clear(left.size());
        }
        else if (left.all()) {
            copyMask(result, right);
        }
        else if (right.all()) {
            copyMask(result, left);
        }
        else {
            int[] positions = result.positionsArray(Math.min(left.selectedCount(), right.selectedCount()));
            int leftIndex = 0;
            int rightIndex = 0;
            int outputIndex = 0;
            while (leftIndex < left.selectedCount() && rightIndex < right.selectedCount()) {
                int leftPosition = left.position(leftIndex);
                int rightPosition = right.position(rightIndex);
                if (leftPosition < rightPosition) {
                    leftIndex++;
                }
                else if (leftPosition > rightPosition) {
                    rightIndex++;
                }
                else {
                    positions[outputIndex++] = leftPosition;
                    leftIndex++;
                    rightIndex++;
                }
            }
            if (outputIndex == left.size() && isAllPositions(positions, outputIndex)) {
                result.selectAll(left.size());
            }
            else {
                result.setSelection(left.size(), outputIndex, false);
            }
        }
        state.trackMask(result, reused);
        return result;
    }

    public Mask differenceMask(Context context, Mask left, Mask right)
    {
        ContextState state = state(context);
        Mask result = state.borrowMask(left.selectedCount());
        boolean reused = result != null;
        if (!reused) {
            result = left.difference(right);
        }
        else if (right.none() || left.none()) {
            copyMask(result, left);
        }
        else if (right.all()) {
            result.clear(left.size());
        }
        else if (left.all()) {
            int[] positions = result.positionsArray(left.size() - right.selectedCount());
            int outputIndex = 0;
            int position = 0;
            for (int index = 0; index < right.selectedCount(); index++) {
                int rightPosition = right.position(index);
                while (position < rightPosition) {
                    positions[outputIndex++] = position++;
                }
                position++;
            }
            while (position < left.size()) {
                positions[outputIndex++] = position++;
            }
            result.setSelection(left.size(), outputIndex, false);
        }
        else {
            int[] positions = result.positionsArray(left.selectedCount());
            int leftIndex = 0;
            int rightIndex = 0;
            int outputIndex = 0;
            while (leftIndex < left.selectedCount() && rightIndex < right.selectedCount()) {
                int leftPosition = left.position(leftIndex);
                int rightPosition = right.position(rightIndex);
                if (leftPosition < rightPosition) {
                    positions[outputIndex++] = leftPosition;
                    leftIndex++;
                }
                else if (leftPosition > rightPosition) {
                    rightIndex++;
                }
                else {
                    leftIndex++;
                    rightIndex++;
                }
            }
            while (leftIndex < left.selectedCount()) {
                positions[outputIndex++] = left.position(leftIndex++);
            }
            result.setSelection(left.size(), outputIndex, false);
        }
        state.trackMask(result, reused);
        return result;
    }

    public Mask differenceMask(Context context, Mask mask, BooleanVector other)
    {
        if (other.length() == 0 || mask.none()) {
            return allocateSparseMask(context, positions(mask), mask.size());
        }

        ContextState state = state(context);
        Mask result = state.borrowMask(mask.selectedCount());
        boolean reused = result != null;
        if (!reused) {
            result = mask.andNot(other);
        }
        else {
            int[] positions = result.positionsArray(mask.selectedCount());
            int selectedCount = 0;
            if (mask.all()) {
                for (int position = 0; position < mask.size(); position++) {
                    if (!other.values()[position]) {
                        positions[selectedCount++] = position;
                    }
                }
            }
            else {
                for (int index = 0; index < mask.selectedCount(); index++) {
                    int position = mask.position(index);
                    if (!other.values()[position]) {
                        positions[selectedCount++] = position;
                    }
                }
            }
            if (selectedCount == mask.size() && isAllPositions(positions, selectedCount)) {
                result.selectAll(mask.size());
            }
            else {
                result.setSelection(mask.size(), selectedCount, false);
            }
        }
        state.trackMask(result, reused);
        return result;
    }

    public Mask unionMask(Context context, Mask left, Mask right)
    {
        ContextState state = state(context);
        Mask result = state.borrowMask(Math.min(left.size(), left.selectedCount() + right.selectedCount()));
        boolean reused = result != null;
        if (!reused) {
            result = left.or(right);
        }
        else if (left.all() || right.all()) {
            result.selectAll(left.size());
        }
        else if (left.none()) {
            copyMask(result, right);
        }
        else if (right.none()) {
            copyMask(result, left);
        }
        else {
            int[] positions = result.positionsArray(Math.min(left.size(), left.selectedCount() + right.selectedCount()));
            int leftIndex = 0;
            int rightIndex = 0;
            int outputIndex = 0;
            while (leftIndex < left.selectedCount() && rightIndex < right.selectedCount()) {
                int leftPosition = left.position(leftIndex);
                int rightPosition = right.position(rightIndex);
                if (leftPosition < rightPosition) {
                    positions[outputIndex++] = leftPosition;
                    leftIndex++;
                }
                else if (leftPosition > rightPosition) {
                    positions[outputIndex++] = rightPosition;
                    rightIndex++;
                }
                else {
                    positions[outputIndex++] = leftPosition;
                    leftIndex++;
                    rightIndex++;
                }
            }
            while (leftIndex < left.selectedCount()) {
                positions[outputIndex++] = left.position(leftIndex++);
            }
            while (rightIndex < right.selectedCount()) {
                positions[outputIndex++] = right.position(rightIndex++);
            }
            if (outputIndex == left.size() && isAllPositions(positions, outputIndex)) {
                result.selectAll(left.size());
            }
            else {
                result.setSelection(left.size(), outputIndex, false);
            }
        }
        state.trackMask(result, reused);
        return result;
    }

    public Mask lastMask(Context context, Mask mask, int count)
    {
        if (count >= mask.selectedCount()) {
            return mask;
        }

        ContextState state = state(context);
        Mask result = state.borrowMask(count);
        boolean reused = result != null;
        if (!reused) {
            result = mask.last(count);
        }
        else if (count <= 0) {
            result.clear(mask.size());
        }
        else {
            int[] positions = result.positionsArray(count);
            for (int index = 0; index < count; index++) {
                positions[index] = mask.position(mask.selectedCount() - count + index);
            }
            if (count == mask.size() && isAllPositions(positions, count)) {
                result.selectAll(mask.size());
            }
            else {
                result.setSelection(mask.size(), count, false);
            }
        }
        state.trackMask(result, reused);
        return result;
    }

    public Mask firstMask(Context context, Mask mask, int count)
    {
        if (count >= mask.selectedCount()) {
            return mask;
        }

        ContextState state = state(context);
        Mask result = state.borrowMask(count);
        boolean reused = result != null;
        if (!reused) {
            result = mask.first(count);
        }
        else if (count <= 0) {
            result.clear(mask.size());
        }
        else {
            int[] positions = result.positionsArray(count);
            for (int index = 0; index < count; index++) {
                positions[index] = mask.position(index);
            }
            if (count == mask.size() && isAllPositions(positions, count)) {
                result.selectAll(mask.size());
            }
            else {
                result.setSelection(mask.size(), count, false);
            }
        }
        state.trackMask(result, reused);
        return result;
    }

    @Override
    public String toString()
    {
        return states.entrySet().stream()
                .map(e -> "%s: total=%s, peak=%s, current=%s".formatted(
                        e.getKey().name(),
                        e.getValue().stats().total(),
                        e.getValue().stats().peak(),
                        e.getValue().stats().current()))
                .collect(Collectors.joining("\n"));
    }

    public long totalBytes(Context context)
    {
        return state(context).stats().total();
    }

    public long currentBytes(Context context)
    {
        return state(context).stats().current();
    }

    public long peakBytes(Context context)
    {
        return state(context).stats().peak();
    }

    public void release(Context context)
    {
        state(context).release();
    }

    public void release(Context context, Mask mask)
    {
        state(context).releaseMask(mask);
    }

    public void release(Context context, Vector vector)
    {
        releaseVectorTree(context, vector);
    }

    public void discard(Context context, Vector vector)
    {
        discardVector(context, vector);
    }

    public void releaseIfPresent(Context context)
    {
        ContextState state = states.get(context);
        if (state != null) {
            state.release();
        }
    }

    public Mask transfer(Context context, Mask mask)
    {
        transferMask(mask, context);
        return mask;
    }

    public <T extends Vector> T transfer(Context context, T vector)
    {
        transferVector(vector, context);
        return vector;
    }

    public Streams copyStreams(Context context, Streams streams)
    {
        Streams.Builder copied = Streams.builder();
        for (Map.Entry<org.weakref.nitro.operator.evaluator.ir.Stream, Vector> entry : streams.asMap().entrySet()) {
            copied.put(entry.getKey(), copyVector(context, entry.getValue()));
        }
        return copied.build();
    }

    public Streams copyStreams(Context context, Streams streams, int[] positions)
    {
        Streams.Builder copied = Streams.builder();
        for (Map.Entry<org.weakref.nitro.operator.evaluator.ir.Stream, Vector> entry : streams.asMap().entrySet()) {
            copied.put(entry.getKey(), copyVector(context, entry.getValue(), positions));
        }
        return copied.build();
    }

    public Vector copyVector(Context context, Vector vector)
    {
        return vector.copy(this, context);
    }

    public Vector copyVector(Context context, Vector vector, int[] positions)
    {
        return vector.copy(this, context, positions);
    }

    private void transferMask(Mask mask, Context preferredContext)
    {
        ContextState preferredState = states.get(preferredContext);
        if (preferredState != null && preferredState.transferMask(mask)) {
            return;
        }
        for (Map.Entry<Context, ContextState> entry : states.entrySet()) {
            if (entry.getKey().equals(preferredContext)) {
                continue;
            }
            if (entry.getValue().transferMask(mask)) {
                return;
            }
        }
    }

    private void transferVector(Vector vector, Context preferredContext)
    {
        vector.forEachChildVector(child -> transferVector(child, preferredContext));

        ContextState preferredState = states.get(preferredContext);
        if (preferredState != null && preferredState.transferVector(vector)) {
            return;
        }
        for (Map.Entry<Context, ContextState> entry : states.entrySet()) {
            if (entry.getKey().equals(preferredContext)) {
                continue;
            }
            if (entry.getValue().transferVector(vector)) {
                return;
            }
        }
    }

    private void transferStreams(Streams streams, Context preferredContext)
    {
        for (Vector child : streams.asMap().values()) {
            transferVector(child, preferredContext);
        }
    }

    private void releaseVector(Context context, Vector vector)
    {
        state(context).releaseVector(vector);
    }

    private void releaseVectorTree(Context context, Vector vector)
    {
        vector.forEachChildVector(child -> releaseVectorTree(context, child));
        releaseVector(context, vector);
    }

    private void discardVector(Context context, Vector vector)
    {
        state(context).discardVector(vector);
    }

    private ContextState state(Context context)
    {
        return states.computeIfAbsent(context, _ -> new ContextState());
    }

    private static int[] positions(Mask mask)
    {
        int[] positions = new int[mask.selectedCount()];
        for (int index = 0; index < positions.length; index++) {
            positions[index] = mask.position(index);
        }
        return positions;
    }

    private static boolean isAllPositions(int[] positions, int selectedCount)
    {
        for (int index = 0; index < selectedCount; index++) {
            if (positions[index] != index) {
                return false;
            }
        }
        return true;
    }

    private static int growthCapacity(int desiredSize)
    {
        if (desiredSize <= 0) {
            return 0;
        }
        return Math.max(desiredSize, computeCapacity(desiredSize));
    }

    private static void copyMask(Mask target, Mask source)
    {
        if (source.all()) {
            target.selectAll(source.size());
            return;
        }

        int[] positions = target.positionsArray(source.selectedCount());
        for (int index = 0; index < source.selectedCount(); index++) {
            positions[index] = source.position(index);
        }
        target.setSelection(source.size(), source.selectedCount(), false);
    }

    private static long maskBytes(Mask mask)
    {
        if (mask.all()) {
            return 0;
        }
        return (long) mask.capacity() * Integer.BYTES;
    }

    private static final class ContextState
    {
        private final Stats stats = new Stats();
        private final Map<Object, TreeMap<Integer, ArrayDeque<Vector>>> vectorPool = new HashMap<>();
        private final Map<Object, ArrayDeque<Vector>> vectorPoolOrder = new HashMap<>();
        private final TreeMap<Integer, ArrayDeque<Mask>> maskPool = new TreeMap<>();
        private final Set<Vector> inUseVectors = Collections.newSetFromMap(new IdentityHashMap<>());
        private final Set<Mask> inUseMasks = Collections.newSetFromMap(new IdentityHashMap<>());

        public Stats stats()
        {
            return stats;
        }

        public <T extends Vector> T borrowVector(Vector.PoolRequest request, Class<T> vectorType)
        {
            TreeMap<Integer, ArrayDeque<Vector>> pool = vectorPool.get(request.family());
            if (pool == null) {
                return null;
            }

            Map.Entry<Integer, ArrayDeque<Vector>> entry = pool.ceilingEntry(request.minimumCapacity());
            if (entry == null) {
                return null;
            }
            if (request.exactCapacityMatch() && entry.getKey() != request.minimumCapacity()) {
                return null;
            }

            T vector = vectorType.cast(entry.getValue().removeFirst());
            if (entry.getValue().isEmpty()) {
                pool.remove(entry.getKey());
            }
            ArrayDeque<Vector> order = vectorPoolOrder.get(request.family());
            if (order != null) {
                order.remove(vector);
                if (order.isEmpty()) {
                    vectorPoolOrder.remove(request.family());
                }
            }
            if (pool.isEmpty()) {
                vectorPool.remove(request.family());
            }
            return vector;
        }

        public void trackVector(Vector vector, boolean reused)
        {
            inUseVectors.add(vector);
            stats.acquire(vector.retainedBytes(), reused);
        }

        public void releaseVector(Vector vector)
        {
            if (!inUseVectors.remove(vector)) {
                return;
            }

            stats.releaseBytes(vector.retainedBytes());
            Vector.PoolSlot slot = vector.poolSlot();
            if (slot == null) {
                return;
            }
            addVectorToPool(slot, vector);
        }

        public void discardVector(Vector vector)
        {
            if (!inUseVectors.remove(vector)) {
                return;
            }
            stats.releaseBytes(vector.retainedBytes());
        }

        public boolean transferVector(Vector vector)
        {
            if (!inUseVectors.remove(vector)) {
                return false;
            }
            stats.releaseBytes(vector.retainedBytes());
            return true;
        }

        public Mask borrowMask(int requiredCapacity)
        {
            Map.Entry<Integer, ArrayDeque<Mask>> entry = maskPool.ceilingEntry(requiredCapacity);
            if (entry == null) {
                return null;
            }

            Mask mask = entry.getValue().removeFirst();
            if (entry.getValue().isEmpty()) {
                maskPool.remove(entry.getKey());
            }
            return mask;
        }

        public void trackMask(Mask mask, boolean reused)
        {
            inUseMasks.add(mask);
            stats.acquire(maskBytes(mask), reused);
        }

        public boolean transferMask(Mask mask)
        {
            if (!inUseMasks.remove(mask)) {
                return false;
            }
            stats.releaseBytes(maskBytes(mask));
            return true;
        }

        public void releaseMask(Mask mask)
        {
            if (!inUseMasks.remove(mask)) {
                return;
            }
            stats.releaseBytes(maskBytes(mask));
            ArrayDeque<Mask> bucket = maskPool
                    .computeIfAbsent(mask.capacity(), _ -> new ArrayDeque<>());
            bucket.addLast(mask);
            while (bucket.size() > MAX_POOLED_MASKS_PER_BUCKET) {
                bucket.removeFirst();
            }
        }

        public void release()
        {
            for (Vector vector : inUseVectors) {
                Vector.PoolSlot slot = vector.poolSlot();
                if (slot != null) {
                    addVectorToPool(slot, vector);
                }
            }
            for (Mask mask : inUseMasks) {
                ArrayDeque<Mask> bucket = maskPool
                        .computeIfAbsent(mask.capacity(), _ -> new ArrayDeque<>());
                bucket.addLast(mask);
                while (bucket.size() > MAX_POOLED_MASKS_PER_BUCKET) {
                    bucket.removeFirst();
                }
            }
            inUseVectors.clear();
            inUseMasks.clear();
            stats.release();
        }

        private void addVectorToPool(Vector.PoolSlot slot, Vector vector)
        {
            vectorPool
                    .computeIfAbsent(slot.family(), _ -> new TreeMap<>())
                    .computeIfAbsent(slot.capacity(), _ -> new ArrayDeque<>())
                    .addLast(vector);
            ArrayDeque<Vector> order = vectorPoolOrder.computeIfAbsent(slot.family(), _ -> new ArrayDeque<>());
            order.addLast(vector);
            while (order.size() > slot.maxRetained()) {
                Vector discarded = order.removeFirst();
                removeVectorFromPool(requireNonNull(discarded.poolSlot(), "discarded vector has no pool slot"), discarded);
            }
        }

        private void removeVectorFromPool(Vector.PoolSlot slot, Vector vector)
        {
            TreeMap<Integer, ArrayDeque<Vector>> pool = vectorPool.get(slot.family());
            if (pool == null) {
                return;
            }

            ArrayDeque<Vector> bucket = pool.get(slot.capacity());
            if (bucket == null) {
                return;
            }

            bucket.remove(vector);
            if (bucket.isEmpty()) {
                pool.remove(slot.capacity());
            }
            if (pool.isEmpty()) {
                vectorPool.remove(slot.family());
            }
        }
    }

    // TODO: track amount of reallocated memory (i.e., how much effort is wasted due to potentially poor allocation strategies)
    private static class Stats
    {
        private long total;
        private long peak;
        private long current;

        public void acquire(long bytes, boolean reused)
        {
            if (!reused && bytes > 0) {
                total += bytes;
            }
            current += bytes;
            peak = Math.max(peak, current);
        }

        public void releaseBytes(long bytes)
        {
            current -= bytes;
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
