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

import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeVectorFactory;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Streams;

import static java.util.Objects.requireNonNull;

public final class SelectedPositionWindowFunction
        implements RunningWindowFunction
{
    private final TypeVectorFactory vectorFactory;
    private final WindowFrame frame;
    private final FramedPositionSelector selector;
    private final WindowFrame.Bounds frameBounds = new WindowFrame.Bounds();
    private final Selection selection = new Selection();
    private boolean outputValuesInitialized;

    public SelectedPositionWindowFunction(TypeBinding outputType, PositionSelector selector)
    {
        this(outputType, WindowFrame.fullPartition(), unframed(selector));
    }

    public SelectedPositionWindowFunction(TypeBinding outputType, WindowFrame frame, FramedPositionSelector selector)
    {
        requireNonNull(outputType, "outputType is null");
        this.vectorFactory = outputType.vectorFactory()
                .orElseThrow(() -> new IllegalArgumentException("outputType has no vector factory"));
        this.frame = requireNonNull(frame, "frame is null");
        this.selector = requireNonNull(selector, "selector is null");
    }

    private static FramedPositionSelector unframed(PositionSelector selector)
    {
        requireNonNull(selector, "selector is null");
        return (partition, _, outputPosition, selection) -> selector.select(partition, outputPosition, selection);
    }

    @Override
    public Streams emptyOutput(Allocator allocator, Allocator.Context allocationContext, int size)
    {
        outputValuesInitialized = false;
        return WindowValueCopySupport.emptyOutput(vectorFactory, allocator, allocationContext, size, true);
    }

    @Override
    public void reset() {}

    @Override
    public Streams append(
            Allocator allocator,
            Allocator.Context allocationContext,
            Streams output,
            Streams[] sourceColumns,
            int inputPosition,
            int outputPosition,
            int outputSize)
    {
        return output;
    }

    @Override
    public Streams finishPartition(
            Allocator allocator,
            Allocator.Context allocationContext,
            Streams output,
            WindowPositionIndex partition,
            int partitionStart,
            int outputSize)
    {
        for (int position = 0; position < partition.size(); position++) {
            frameBounds.clear();
            frame.resolve(partition, position, frameBounds);
            if (!frameBounds.present()) {
                continue;
            }
            selection.clear();
            selector.select(partition, frameBounds, position, selection);
            if (!selection.present()) {
                continue;
            }
            int sourcePosition = selection.position();
            if (sourcePosition >= partition.size()) {
                throw new IndexOutOfBoundsException(sourcePosition);
            }
            output = WindowValueCopySupport.copyRange(
                    allocator,
                    allocationContext,
                    partition.column(selection.column(), sourcePosition),
                    output,
                    outputValuesInitialized,
                    partition.sourcePosition(sourcePosition),
                    partitionStart + position,
                    partitionStart + position + 1,
                    outputSize);
            outputValuesInitialized = true;
        }
        return output;
    }

    @FunctionalInterface
    public interface PositionSelector
    {
        /** Leaves {@code selection} unset to produce null. */
        void select(WindowPositionIndex partition, int outputPosition, Selection selection);
    }

    @FunctionalInterface
    public interface FramedPositionSelector
    {
        /** Leaves {@code selection} unset to produce null. */
        void select(WindowPositionIndex partition, WindowFrame.Bounds frame, int outputPosition, Selection selection);
    }

    public static final class Selection
    {
        private int column;
        private int position;
        private boolean present;

        public void set(int column, int position)
        {
            if (column < 0) {
                throw new IllegalArgumentException("column is negative");
            }
            if (position < 0) {
                throw new IllegalArgumentException("position is negative");
            }
            this.column = column;
            this.position = position;
            present = true;
        }

        private void clear()
        {
            present = false;
        }

        private int column()
        {
            return column;
        }

        private int position()
        {
            return position;
        }

        private boolean present()
        {
            return present;
        }
    }
}
