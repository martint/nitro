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
package org.weakref.nitro.parquet;

import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;

import static java.util.Objects.requireNonNull;

/** Applies connector-owned logical conversion to a generic physical nested-value accumulator. */
final class LogicalNestedValueAccumulator
        implements NestedValueAccumulator
{
    private final NestedValueAccumulator delegate;
    private final TypeBinding outputType;
    private final ParquetPrimitiveValueBinding.Bound binding;

    LogicalNestedValueAccumulator(
            NestedValueAccumulator delegate,
            ParquetSchema.Primitive leaf,
            TypeBinding outputType,
            ParquetPrimitiveValueBinding binding)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.outputType = requireNonNull(outputType, "outputType is null");
        this.binding = binding == null
                ? null
                : requireNonNull(binding.bind(leaf.descriptor(), outputType), "logical value binding returned null");
    }

    @Override
    public void reset(Allocator allocator)
    {
        delegate.reset(allocator);
    }

    @Override
    public void reset(Allocator allocator, Allocator.Context context, int exactSize)
    {
        delegate.reset(allocator, context, exactSize);
    }

    @Override
    public void append(PhysicalValueDecoder decoder, int ordinal, int dictionaryId)
    {
        delegate.append(decoder, ordinal, dictionaryId);
    }

    @Override
    public void appendEvents(PhysicalValueDecoder decoder, int[] valueOrdinals, int[] dictionaryIds, int eventOffset, int eventCount)
    {
        delegate.appendEvents(decoder, valueOrdinals, dictionaryIds, eventOffset, eventCount);
    }

    @Override
    public void appendDictionaryRun(PhysicalValueDecoder decoder, int[] dictionaryIds, int ordinal, int count)
    {
        delegate.appendDictionaryRun(decoder, dictionaryIds, ordinal, count);
    }

    @Override
    public void appendPlainRun(PhysicalValueDecoder decoder, int ordinal, int count)
    {
        delegate.appendPlainRun(decoder, ordinal, count);
    }

    @Override
    public void appendNull()
    {
        delegate.appendNull();
    }

    @Override
    public int size()
    {
        return delegate.size();
    }

    @Override
    public Streams materialize(Allocator allocator, Allocator.Context context)
    {
        Streams streams = delegate.materialize(allocator, context);
        if (binding != null) {
            streams = streams.with(Stream.VALUES, binding.convert(allocator, context, streams.values()));
        }
        if (!outputType.supportsVector(streams.values())) {
            throw new UnsupportedParquetFeatureException(
                    "Native nested Parquet representation does not match output type " + outputType.identity());
        }
        return streams;
    }

    @Override
    public void close()
    {
        delegate.close();
    }
}
