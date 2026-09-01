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
package org.weakref.nitro.operator.pattern;

import org.weakref.nitro.core.execution.ExecutionContext;
import org.weakref.nitro.data.PrimitiveArrayPool;

import java.util.Arrays;

import static java.lang.Math.max;
import static java.util.Objects.checkIndex;
import static java.util.Objects.requireNonNull;

/// Allocation-pooled non-deterministic row-pattern matcher.
///
/// A session owns all mutable match state and remains restartable at input-position checkpoints. Forked automaton
/// threads share immutable persistent label and exclusion histories, so a split does not copy a partition-length
/// array. Every primitive work array comes from the explicitly supplied pool and is returned when the session closes.
public final class PatternMatcher
{
    private final PatternProgram program;
    private final PrimitiveArrayPool arrayPool;

    public PatternMatcher(PatternProgram program, PrimitiveArrayPool arrayPool)
    {
        this.program = requireNonNull(program, "program is null");
        this.arrayPool = requireNonNull(arrayPool, "arrayPool is null");
    }

    public Session start(int inputLength, boolean matchingAtPartitionStart, PatternLabelEvaluator evaluator)
    {
        if (inputLength < 0) {
            throw new IllegalArgumentException("inputLength is negative");
        }
        return new Session(inputLength, matchingAtPartitionStart, requireNonNull(evaluator, "evaluator is null"));
    }

    public final class Session
            implements AutoCloseable
    {
        private final int inputLength;
        private final boolean matchingAtPartitionStart;
        private final PatternLabelEvaluator evaluator;
        private final ThreadStore threads = new ThreadStore();
        private final HistoryArena labels = new HistoryArena();
        private final HistoryArena exclusions = new HistoryArena();
        private final PooledIntList current = new PooledIntList();
        private final PooledIntList next = new PooledIntList();
        private final PooledIntList pendingKills = new PooledIntList();
        private final PooledIntList freeThreads = new PooledIntList();
        private final PooledIntList touchedInstructions = new PooledIntList();
        private final PooledIntList visitThreads = new PooledIntList();
        private final PooledIntList visitNext = new PooledIntList();
        private final PooledIntList taskThreads = new PooledIntList();
        private final PooledIntList taskPointers = new PooledIntList();
        private final BorrowedLabelHistory labelHistory = new BorrowedLabelHistory();
        private int[] instructionHeads = arrayPool.borrowInts(program.size());
        private int inputPosition;
        private int currentIndex;
        private int resultThread = -1;
        private boolean initialized;
        private boolean complete;
        private boolean closed;

        private Session(int inputLength, boolean matchingAtPartitionStart, PatternLabelEvaluator evaluator)
        {
            this.inputLength = inputLength;
            this.matchingAtPartitionStart = matchingAtPartitionStart;
            this.evaluator = evaluator;
            Arrays.fill(instructionHeads, -1);
        }

        /// Runs until a match decision is available or the execution context suspends at a restart-safe checkpoint.
        /// If a suspension unwinds the caller, invoking this method again resumes the same session.
        public boolean run(ExecutionContext context)
        {
            requireNonNull(context, "context is null");
            checkOpen();
            if (complete) {
                return resultThread >= 0;
            }
            if (!initialized) {
                initialized = true;
                resetInstructionVisits();
                advanceAndSchedule(current, threads.newThread(), 0, 0);
                releasePendingThreads();
                resetInstructionVisits();
            }

            while (!complete && inputPosition < inputLength) {
                while (currentIndex < current.size()) {
                    if ((currentIndex & 1023) == 0) {
                        context.checkpoint();
                    }
                    int thread = current.get(currentIndex++);
                    if (!threads.isLive(thread)) {
                        continue;
                    }
                    int pointer = threads.pointer(thread);
                    switch (program.instruction(pointer)) {
                        case MATCH_LABEL -> {
                            int label = program.firstOperand(pointer);
                            threads.setLabelHead(thread, labels.append(threads.labelHead(thread), label));
                            labelHistory.thread = thread;
                            if (evaluator.evaluate(label, inputPosition, labelHistory)) {
                                advanceAndSchedule(next, thread, pointer + 1, inputPosition + 1);
                            }
                            else {
                                scheduleKill(thread);
                            }
                        }
                        case DONE -> acceptCandidate(thread);
                        default -> throw new IllegalStateException("scheduled thread is not at a consuming instruction");
                    }
                    if (complete) {
                        break;
                    }
                }
                if (complete) {
                    break;
                }
                releasePendingThreads();
                current.swap(next);
                next.clear();
                currentIndex = 0;
                inputPosition++;
                resetInstructionVisits();
                if (current.isEmpty()) {
                    finishDecision();
                }
            }

            if (!complete) {
                for (int index = 0; index < current.size(); index++) {
                    int thread = current.get(index);
                    if (threads.isLive(thread) && program.instruction(threads.pointer(thread)) == PatternProgram.Instruction.DONE) {
                        currentIndex = index + 1;
                        acceptCandidate(thread);
                        break;
                    }
                }
                finishDecision();
            }
            return resultThread >= 0;
        }

        public boolean isComplete()
        {
            checkOpen();
            return complete;
        }

        public boolean matched()
        {
            checkComplete();
            return resultThread >= 0;
        }

        public int labelCount()
        {
            checkMatched();
            return labels.length(threads.labelHead(resultThread));
        }

        public int labelAt(int position)
        {
            checkMatched();
            return labels.valueAt(threads.labelHead(resultThread), position);
        }

        public void copyLabelsTo(int[] destination, int offset)
        {
            checkMatched();
            labels.copyTo(threads.labelHead(resultThread), destination, offset);
        }

        public int exclusionCount()
        {
            checkMatched();
            return exclusions.length(threads.exclusionHead(resultThread));
        }

        public int exclusionAt(int index)
        {
            checkMatched();
            return exclusions.valueAt(threads.exclusionHead(resultThread), index);
        }

        public void copyExclusionsTo(int[] destination, int offset)
        {
            checkMatched();
            exclusions.copyTo(threads.exclusionHead(resultThread), destination, offset);
        }

        private void advanceAndSchedule(PooledIntList destination, int initialThread, int initialPointer, int position)
        {
            taskThreads.clear();
            taskPointers.clear();
            pushTask(initialThread, initialPointer);
            while (!taskThreads.isEmpty()) {
                int thread = taskThreads.removeLast();
                int pointer = taskPointers.removeLast();
                if (!threads.isLive(thread)) {
                    continue;
                }
                if (hasEquivalentVisit(pointer, thread)) {
                    scheduleKill(thread);
                    continue;
                }
                recordVisit(pointer, thread);

                switch (program.instruction(pointer)) {
                    case MATCH_START -> {
                        if (position == 0 && matchingAtPartitionStart) {
                            pushTask(thread, pointer + 1);
                        }
                        else {
                            scheduleKill(thread);
                        }
                    }
                    case MATCH_END -> {
                        if (position == inputLength) {
                            pushTask(thread, pointer + 1);
                        }
                        else {
                            scheduleKill(thread);
                        }
                    }
                    case JUMP -> pushTask(thread, program.firstOperand(pointer));
                    case SPLIT -> {
                        int fork = threads.fork(thread);
                        pushTask(fork, program.secondOperand(pointer));
                        pushTask(thread, program.firstOperand(pointer));
                    }
                    case SAVE -> {
                        threads.setExclusionHead(thread, exclusions.append(threads.exclusionHead(thread), position));
                        pushTask(thread, pointer + 1);
                    }
                    case MATCH_LABEL, DONE -> {
                        threads.setPointer(thread, pointer);
                        destination.add(thread);
                    }
                }
            }
        }

        private boolean hasEquivalentVisit(int pointer, int thread)
        {
            for (int entry = instructionHeads[pointer]; entry >= 0; entry = visitNext.get(entry)) {
                int other = visitThreads.get(entry);
                if (threads.isLive(other) &&
                        labels.equal(threads.labelHead(other), threads.labelHead(thread)) &&
                        exclusions.equal(threads.exclusionHead(other), threads.exclusionHead(thread))) {
                    return true;
                }
            }
            return false;
        }

        private void recordVisit(int pointer, int thread)
        {
            if (instructionHeads[pointer] < 0) {
                touchedInstructions.add(pointer);
            }
            visitThreads.add(thread);
            visitNext.add(instructionHeads[pointer]);
            instructionHeads[pointer] = visitThreads.size() - 1;
        }

        private void resetInstructionVisits()
        {
            for (int index = 0; index < touchedInstructions.size(); index++) {
                instructionHeads[touchedInstructions.get(index)] = -1;
            }
            touchedInstructions.clear();
            visitThreads.clear();
            visitNext.clear();
        }

        private void pushTask(int thread, int pointer)
        {
            taskThreads.add(thread);
            taskPointers.add(pointer);
        }

        private void scheduleKill(int thread)
        {
            if (threads.scheduleKill(thread)) {
                pendingKills.add(thread);
            }
        }

        private void releasePendingThreads()
        {
            for (int index = 0; index < pendingKills.size(); index++) {
                releaseThread(pendingKills.get(index));
            }
            pendingKills.clear();
        }

        private void releaseThread(int thread)
        {
            if (!threads.isAllocated(thread)) {
                return;
            }
            labels.release(threads.labelHead(thread));
            exclusions.release(threads.exclusionHead(thread));
            threads.release(thread);
            freeThreads.add(thread);
        }

        private void acceptCandidate(int candidate)
        {
            if (resultThread >= 0 && resultThread != candidate) {
                releaseThread(resultThread);
            }
            resultThread = candidate;
            while (currentIndex < current.size()) {
                scheduleKill(current.get(currentIndex++));
            }
        }

        private void finishDecision()
        {
            complete = true;
            releasePendingThreads();
            releaseAllExcept(resultThread);
        }

        private void releaseAllExcept(int retainedThread)
        {
            pendingKills.clear();
            for (int thread = 0; thread < threads.allocatedCount(); thread++) {
                if (thread != retainedThread && threads.isAllocated(thread)) {
                    releaseThread(thread);
                }
            }
            current.clear();
            next.clear();
        }

        private void checkMatched()
        {
            checkComplete();
            if (resultThread < 0) {
                throw new IllegalStateException("pattern did not match");
            }
        }

        private void checkComplete()
        {
            checkOpen();
            if (!complete) {
                throw new IllegalStateException("match is not complete");
            }
        }

        private void checkOpen()
        {
            if (closed) {
                throw new IllegalStateException("match session is closed");
            }
        }

        @Override
        public void close()
        {
            if (closed) {
                return;
            }
            releaseAllExcept(-1);
            threads.close();
            labels.close();
            exclusions.close();
            current.close();
            next.close();
            pendingKills.close();
            freeThreads.close();
            touchedInstructions.close();
            visitThreads.close();
            visitNext.close();
            taskThreads.close();
            taskPointers.close();
            arrayPool.release(instructionHeads);
            instructionHeads = null;
            closed = true;
        }

        private final class BorrowedLabelHistory
                implements PatternLabelEvaluator.LabelHistory
        {
            private int thread;

            @Override
            public int size()
            {
                return labels.length(threads.labelHead(thread));
            }

            @Override
            public int labelAt(int position)
            {
                return labels.valueAt(threads.labelHead(thread), position);
            }
        }

        private final class ThreadStore
                implements AutoCloseable
        {
            private int[] pointers = arrayPool.borrowInts(16);
            private int[] labelHeads = arrayPool.borrowInts(16);
            private int[] exclusionHeads = arrayPool.borrowInts(16);
            private int[] states = arrayPool.borrowInts(16);
            private int allocatedCount;

            int newThread()
            {
                if (!freeThreads.isEmpty()) {
                    int thread = freeThreads.removeLast();
                    states[thread] = 1;
                    labelHeads[thread] = -1;
                    exclusionHeads[thread] = -1;
                    return thread;
                }
                ensureCapacity(allocatedCount + 1);
                int thread = allocatedCount++;
                states[thread] = 1;
                labelHeads[thread] = -1;
                exclusionHeads[thread] = -1;
                return thread;
            }

            int fork(int parent)
            {
                int child = newThread();
                labelHeads[child] = labelHeads[parent];
                exclusionHeads[child] = exclusionHeads[parent];
                labels.retain(labelHeads[child]);
                exclusions.retain(exclusionHeads[child]);
                return child;
            }

            boolean scheduleKill(int thread)
            {
                if (states[thread] != 1) {
                    return false;
                }
                states[thread] = 2;
                return true;
            }

            void release(int thread)
            {
                states[thread] = 0;
                labelHeads[thread] = -1;
                exclusionHeads[thread] = -1;
            }

            boolean isLive(int thread)
            {
                return states[thread] == 1;
            }

            boolean isAllocated(int thread)
            {
                return states[thread] != 0;
            }

            int allocatedCount()
            {
                return allocatedCount;
            }

            int pointer(int thread)
            {
                return pointers[thread];
            }

            void setPointer(int thread, int pointer)
            {
                pointers[thread] = pointer;
            }

            int labelHead(int thread)
            {
                return labelHeads[thread];
            }

            void setLabelHead(int thread, int head)
            {
                labelHeads[thread] = head;
            }

            int exclusionHead(int thread)
            {
                return exclusionHeads[thread];
            }

            void setExclusionHead(int thread, int head)
            {
                exclusionHeads[thread] = head;
            }

            private void ensureCapacity(int required)
            {
                if (required <= states.length) {
                    return;
                }
                int capacity = growthCapacity(states.length, required);
                pointers = grow(pointers, capacity, allocatedCount);
                labelHeads = grow(labelHeads, capacity, allocatedCount);
                exclusionHeads = grow(exclusionHeads, capacity, allocatedCount);
                states = grow(states, capacity, allocatedCount);
            }

            @Override
            public void close()
            {
                arrayPool.release(pointers);
                arrayPool.release(labelHeads);
                arrayPool.release(exclusionHeads);
                arrayPool.release(states);
                pointers = null;
                labelHeads = null;
                exclusionHeads = null;
                states = null;
            }
        }

        private final class HistoryArena
                implements AutoCloseable
        {
            private int[] values = arrayPool.borrowInts(16);
            private int[] previous = arrayPool.borrowInts(16);
            private int[] lengths = arrayPool.borrowInts(16);
            private int[] references = arrayPool.borrowInts(16);
            private final PooledIntList free = new PooledIntList();
            private int nodeCount;

            int append(int previousHead, int value)
            {
                int node;
                if (!free.isEmpty()) {
                    node = free.removeLast();
                }
                else {
                    ensureCapacity(nodeCount + 1);
                    node = nodeCount++;
                }
                values[node] = value;
                previous[node] = previousHead;
                lengths[node] = previousHead < 0 ? 1 : lengths[previousHead] + 1;
                references[node] = 1;
                return node;
            }

            void retain(int head)
            {
                if (head >= 0) {
                    references[head]++;
                }
            }

            void release(int head)
            {
                while (head >= 0 && --references[head] == 0) {
                    int released = head;
                    head = previous[released];
                    previous[released] = -1;
                    free.add(released);
                }
            }

            int length(int head)
            {
                return head < 0 ? 0 : lengths[head];
            }

            int valueAt(int head, int position)
            {
                int length = length(head);
                checkIndex(position, length);
                for (int remaining = length - position - 1; remaining > 0; remaining--) {
                    head = previous[head];
                }
                return values[head];
            }

            void copyTo(int head, int[] destination, int offset)
            {
                requireNonNull(destination, "destination is null");
                int length = length(head);
                if (offset < 0 || offset > destination.length - length) {
                    throw new IndexOutOfBoundsException("destination does not contain the requested range");
                }
                for (int position = offset + length - 1; position >= offset; position--) {
                    destination[position] = values[head];
                    head = previous[head];
                }
            }

            boolean equal(int first, int second)
            {
                if (first == second) {
                    return true;
                }
                if (length(first) != length(second)) {
                    return false;
                }
                while (first >= 0) {
                    if (values[first] != values[second]) {
                        return false;
                    }
                    first = previous[first];
                    second = previous[second];
                }
                return true;
            }

            private void ensureCapacity(int required)
            {
                if (required <= values.length) {
                    return;
                }
                int capacity = growthCapacity(values.length, required);
                values = grow(values, capacity, nodeCount);
                previous = grow(previous, capacity, nodeCount);
                lengths = grow(lengths, capacity, nodeCount);
                references = grow(references, capacity, nodeCount);
            }

            @Override
            public void close()
            {
                free.close();
                arrayPool.release(values);
                arrayPool.release(previous);
                arrayPool.release(lengths);
                arrayPool.release(references);
                values = null;
                previous = null;
                lengths = null;
                references = null;
            }
        }

        private final class PooledIntList
                implements AutoCloseable
        {
            private int[] values = arrayPool.borrowInts(16);
            private int size;

            void add(int value)
            {
                if (size == values.length) {
                    values = grow(values, growthCapacity(values.length, size + 1), size);
                }
                values[size++] = value;
            }

            int get(int index)
            {
                return values[checkIndex(index, size)];
            }

            int size()
            {
                return size;
            }

            boolean isEmpty()
            {
                return size == 0;
            }

            int removeLast()
            {
                if (size == 0) {
                    throw new IllegalStateException("list is empty");
                }
                return values[--size];
            }

            void clear()
            {
                size = 0;
            }

            void swap(PooledIntList other)
            {
                int[] oldValues = values;
                int oldSize = size;
                values = other.values;
                size = other.size;
                other.values = oldValues;
                other.size = oldSize;
            }

            @Override
            public void close()
            {
                arrayPool.release(values);
                values = null;
                size = 0;
            }
        }

        private int[] grow(int[] source, int capacity, int used)
        {
            int[] replacement = arrayPool.borrowInts(capacity);
            System.arraycopy(source, 0, replacement, 0, used);
            arrayPool.release(source);
            return replacement;
        }
    }

    private static int growthCapacity(int current, int required)
    {
        return max(required, max(16, current + (current >> 1)));
    }
}
