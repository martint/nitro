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

import org.weakref.nitro.data.BatchBufferOwner;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorColumnGeneration;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Execution facade over a source-neutral lazy vector-column generation.
 */
public final class Output
        extends VectorColumnGeneration
{
    public interface PositionAccessor
    {
        boolean isNull(int position);

        int compareNonNull(int position, Vector otherValues, int otherPosition);
    }

    private PositionAccessor positionAccessor;

    public static Output of(Streams streams)
    {
        return new Output(streams.streams(), streams::get);
    }

    public Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver)
    {
        super(exposedStreams, resolver);
    }

    public Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver, BiFunction<Stream, Vector, Vector> takeResolver)
    {
        super(exposedStreams, resolver, takeResolver);
    }

    public Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver, BiFunction<Stream, Vector, Vector> takeResolver, BiConsumer<Stream, Vector> releaseResolver)
    {
        super(exposedStreams, resolver, takeResolver, releaseResolver);
    }

    public Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver, BatchBufferOwner bufferOwner)
    {
        super(exposedStreams, resolver, bufferOwner);
    }

    public Output(
            Set<Stream> exposedStreams,
            Function<Stream, Vector> resolver,
            MaskedResolver maskedResolver,
            MaskResolver maskResolver,
            PositionsResolver positionsResolver,
            SinglePositionResolver singlePositionResolver,
            BatchBufferOwner bufferOwner)
    {
        super(exposedStreams, resolver, maskedResolver, maskResolver, positionsResolver, singlePositionResolver, bufferOwner);
    }

    public Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver, BiFunction<Stream, Vector, Vector> takeResolver, BiConsumer<Stream, Vector> releaseResolver, SinglePositionResolver singlePositionResolver)
    {
        super(exposedStreams, resolver, takeResolver, releaseResolver, singlePositionResolver);
    }

    public Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver, BiFunction<Stream, Vector, Vector> takeResolver, BiConsumer<Stream, Vector> releaseResolver, PositionsResolver positionsResolver, SinglePositionResolver singlePositionResolver)
    {
        super(exposedStreams, resolver, takeResolver, releaseResolver, positionsResolver, singlePositionResolver);
    }

    public Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver, MaskedResolver maskedResolver, BiFunction<Stream, Vector, Vector> takeResolver, BiConsumer<Stream, Vector> releaseResolver, PositionsResolver positionsResolver, SinglePositionResolver singlePositionResolver)
    {
        super(exposedStreams, resolver, maskedResolver, takeResolver, releaseResolver, positionsResolver, singlePositionResolver);
    }

    public Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver, MaskedResolver maskedResolver, MaskResolver maskResolver, BiFunction<Stream, Vector, Vector> takeResolver, BiConsumer<Stream, Vector> releaseResolver, PositionsResolver positionsResolver, SinglePositionResolver singlePositionResolver)
    {
        super(exposedStreams, resolver, maskedResolver, maskResolver, takeResolver, releaseResolver, positionsResolver, singlePositionResolver);
    }

    private Output(
            Set<Stream> exposedStreams,
            Function<Stream, Vector> resolver,
            MaskedResolver maskedResolver,
            MaskResolver maskResolver,
            BiFunction<Stream, Vector, Vector> takeResolver,
            BiConsumer<Stream, Vector> releaseResolver,
            PositionsResolver positionsResolver,
            SinglePositionResolver singlePositionResolver,
            int knownAllFalseFlags)
    {
        super(
                exposedStreams,
                resolver,
                maskedResolver,
                maskResolver,
                takeResolver,
                releaseResolver,
                positionsResolver,
                singlePositionResolver,
                knownAllFalseFlags);
    }

    @Override
    protected VectorColumnGeneration newGeneration(
            Set<Stream> exposedStreams,
            Function<Stream, Vector> resolver,
            MaskedResolver maskedResolver,
            MaskResolver maskResolver,
            BiFunction<Stream, Vector, Vector> takeResolver,
            BiConsumer<Stream, Vector> releaseResolver,
            PositionsResolver positionsResolver,
            SinglePositionResolver singlePositionResolver,
            int knownAllFalseFlags)
    {
        Output output = new Output(
                exposedStreams,
                resolver,
                maskedResolver,
                maskResolver,
                takeResolver,
                releaseResolver,
                positionsResolver,
                singlePositionResolver,
                knownAllFalseFlags);
        output.positionAccessor = positionAccessor;
        return output;
    }

    public Output withPositionAccessor(PositionAccessor positionAccessor)
    {
        this.positionAccessor = requireNonNull(positionAccessor, "positionAccessor is null");
        return this;
    }

    public PositionAccessor positionAccessor()
    {
        return positionAccessor;
    }

    @Override
    public Output withKnownAllFalse(Set<Stream> knownAllFalseStreams)
    {
        return (Output) super.withKnownAllFalse(knownAllFalseStreams);
    }

    @Override
    public Output select(Set<Stream> selectedStreams)
    {
        return (Output) super.select(selectedStreams);
    }

    @Override
    public Output forward(BiFunction<Stream, Vector, Vector> takeResolver, BiConsumer<Stream, Vector> releaseResolver)
    {
        return (Output) super.forward(takeResolver, releaseResolver);
    }

    @Override
    public Output forwardSinglePositionOnly(BiFunction<Stream, Vector, Vector> takeResolver, BiConsumer<Stream, Vector> releaseResolver)
    {
        return (Output) super.forwardSinglePositionOnly(takeResolver, releaseResolver);
    }

    @Override
    public Output withConstraintSensitiveResolution()
    {
        super.withConstraintSensitiveResolution();
        return this;
    }
}
