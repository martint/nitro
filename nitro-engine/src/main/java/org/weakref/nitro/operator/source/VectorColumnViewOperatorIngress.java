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
package org.weakref.nitro.operator.source;

import org.weakref.nitro.core.batch.ColumnView;
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorColumnCapability;
import org.weakref.nitro.data.VectorColumnGeneration;
import org.weakref.nitro.operator.Output;

import java.util.Set;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/// Direct ingress for source columns that expose allocator-owned Nitro SPI vectors.
public final class VectorColumnViewOperatorIngress
        implements ColumnViewOperatorIngress
{
    private final TypeBinding type;
    private final Set<Stream> streams;

    public VectorColumnViewOperatorIngress(Field field)
    {
        requireNonNull(field, "field is null");
        this.type = field.type();
        if (type.supportedVectorTypes().isEmpty()) {
            throw new IllegalArgumentException("type does not declare any supported vector representations");
        }
        this.streams = field.nullable()
                ? Set.of(Stream.VALUES, Stream.NULLS)
                : Set.of(Stream.VALUES);
    }

    @Override
    public TypeBinding type()
    {
        return type;
    }

    @Override
    public Output output(Supplier<ColumnView> column)
    {
        requireNonNull(column, "column is null");
        ColumnView view = requireNonNull(column.get(), "column supplier returned null");
        Set<Stream> actualStreams = requireNonNull(view.streams(), "column returned null streams");
        if (!streams.containsAll(actualStreams)) {
            throw new IllegalArgumentException("source column exposes streams outside its field contract");
        }
        ColumnAccess access = new ColumnAccess(view);
        return new Output(
                actualStreams,
                access::borrow,
                access::borrow,
                access::tryBorrowMask,
                access::take,
                (_, _) -> {},
                access::copyPositions,
                access::copySinglePosition);
    }

    private final class ColumnAccess
    {
        private final ColumnView view;
        private VectorColumnGeneration generation;
        private boolean generationResolved;

        private ColumnAccess(ColumnView view)
        {
            this.view = view;
        }

        private Vector borrow(Stream stream)
        {
            VectorColumnGeneration generation = generation();
            Vector vector = generation == null ? view(stream).borrow(stream) : generation.borrow(stream);
            return validate(stream, vector, "column returned null vector");
        }

        private Vector borrow(Stream stream, Mask mask)
        {
            VectorColumnGeneration generation = generation();
            Vector vector = generation == null
                    ? view(stream).borrow(stream)
                    : mask == null ? generation.borrow(stream) : generation.borrow(stream, mask);
            return validate(stream, vector, "column returned null vector");
        }

        private Mask tryBorrowMask(
                Stream stream,
                Mask mask,
                boolean selectTrue,
                Allocator allocator,
                Allocator.Context allocationContext)
        {
            VectorColumnGeneration generation = generation();
            return generation == null ? null : generation.tryBorrowMask(stream, mask, selectTrue, allocator, allocationContext);
        }

        private Vector take(Stream stream, Vector ignored)
        {
            VectorColumnGeneration generation = generation();
            Vector vector = generation == null ? view(stream).take(stream) : generation.take(stream);
            return validate(stream, vector, "column returned null transferred vector");
        }

        private Streams copyPositions(
                Set<Stream> streams,
                Streams existing,
                int[] positions,
                int sourceStart,
                int sourceCount,
                int outputStart,
                int size,
                boolean assumeClear)
        {
            VectorColumnGeneration generation = generation();
            return generation == null
                    ? null
                    : generation.copyPositions(existing, positions, sourceStart, sourceCount, outputStart, size, assumeClear);
        }

        private Streams copySinglePosition(Streams existing, int sourcePosition, int outputPosition, int size)
        {
            VectorColumnGeneration generation = generation();
            return generation == null
                    ? null
                    : generation.copySinglePosition(existing, sourcePosition, outputPosition, size);
        }

        private VectorColumnGeneration generation()
        {
            if (!generationResolved) {
                generation = view().capability(VectorColumnCapability.VECTOR_GENERATION).orElse(null);
                generationResolved = true;
            }
            return generation;
        }

        private ColumnView view()
        {
            return view;
        }

        private ColumnView view(Stream stream)
        {
            ColumnView view = view();
            if (!requireNonNull(view.streams(), "column returned null streams").contains(stream)) {
                throw new IllegalArgumentException("source column does not expose declared stream: " + stream);
            }
            return view;
        }

        private Vector validate(Stream stream, Vector vector, String nullMessage)
        {
            vector = requireNonNull(vector, nullMessage);
            if (stream == Stream.VALUES && !type.supportsVector(vector)) {
                throw new IllegalArgumentException("Source returned %s for logical type %s; supported boundary classes are %s"
                        .formatted(vector.getClass().getName(), type.identity(), type.supportedVectorTypes()));
            }
            return vector;
        }
    }
}
