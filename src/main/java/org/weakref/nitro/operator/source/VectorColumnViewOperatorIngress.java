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

import org.weakref.nitro.core.batch.ColumnStream;
import org.weakref.nitro.core.batch.ColumnView;
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Output;
import org.weakref.nitro.operator.evaluator.ir.Stream;

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
        return new Output(
                streams,
                stream -> borrow(column, stream),
                (stream, _) -> take(column, stream),
                (_, _) -> {});
    }

    private Vector borrow(Supplier<ColumnView> column, Stream stream)
    {
        ColumnView view = view(column, stream);
        Vector vector = requireNonNull(
                view.borrow(columnStream(stream)),
                "column returned null vector");
        validate(stream, vector);
        return vector;
    }

    private Vector take(Supplier<ColumnView> column, Stream stream)
    {
        ColumnView view = view(column, stream);
        Vector vector = requireNonNull(
                view.take(columnStream(stream)),
                "column returned null transferred vector");
        validate(stream, vector);
        return vector;
    }

    private void validate(Stream stream, Vector vector)
    {
        if (stream == Stream.VALUES && !type.supportsVector(vector)) {
            throw new IllegalArgumentException("source returned a vector representation not supported by its type");
        }
    }

    private static ColumnView view(Supplier<ColumnView> column, Stream stream)
    {
        ColumnView view = requireNonNull(column.get(), "column supplier returned null");
        if (!requireNonNull(view.streams(), "column returned null streams").contains(columnStream(stream))) {
            throw new IllegalArgumentException("source column does not expose declared stream: " + stream);
        }
        return view;
    }

    private static ColumnStream columnStream(Stream stream)
    {
        return switch (stream) {
            case VALUES -> ColumnStream.VALUES;
            case NULLS -> ColumnStream.NULLS;
            case ERRORS -> ColumnStream.ERRORS;
        };
    }
}
