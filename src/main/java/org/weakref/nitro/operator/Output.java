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

import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Lazily-resolved set of streams for one logical output column.
 * <p>
 * Streams can be borrowed multiple times, or taken exactly once. Borrowed-but-not-taken streams
 * are released on {@link #close()}.
 */
public final class Output
        implements AutoCloseable
{
    @FunctionalInterface
    public interface SinglePositionResolver
    {
        Streams copySinglePosition(Streams existing, int sourcePosition, int outputPosition, int size);
    }

    @FunctionalInterface
    public interface PositionsResolver
    {
        Streams copyPositions(Set<Stream> streams, Streams existing, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size, boolean assumeClearOutputRange);
    }

    private static final int VALUES_FLAG = 1;
    private static final int NULLS_FLAG = 1 << 1;
    private static final int ERRORS_FLAG = 1 << 2;

    @SuppressWarnings("unchecked")
    private static final Set<Stream>[] STREAM_SETS = new Set[8];

    static {
        for (int flags = 0; flags < STREAM_SETS.length; flags++) {
            STREAM_SETS[flags] = Streams.streamSet(flags);
        }
    }

    private final int exposedFlags;
    private final Function<Stream, Vector> resolver;
    private final BiFunction<Stream, Vector, Vector> takeResolver;
    private final BiConsumer<Stream, Vector> releaseResolver;
    private final PositionsResolver positionsResolver;
    private final SinglePositionResolver singlePositionResolver;
    private final int knownAllFalseFlags;
    private final int debugDepth;
    private final Vector[] resolvedStreams = new Vector[Stream.values().length];
    private int resolvedFlags;
    private int takenFlags;
    private boolean closed;

    public static Output of(Streams streams)
    {
        return new Output(streams.streams(), streams::get);
    }

    public Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver)
    {
        this(exposedStreams, resolver, (_, vector) -> vector, (_, _) -> {}, null, null);
        OutputDebug.recordBaseOutput(false, false, false);
    }

    public Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver, BiFunction<Stream, Vector, Vector> takeResolver)
    {
        this(exposedStreams, resolver, takeResolver, (_, _) -> {}, null, null);
        OutputDebug.recordBaseOutput(false, false, false);
    }

    public Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver, BiFunction<Stream, Vector, Vector> takeResolver, BiConsumer<Stream, Vector> releaseResolver)
    {
        this(exposedStreams, resolver, takeResolver, releaseResolver, null, null);
        OutputDebug.recordBaseOutput(false, false, false);
    }

    public Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver, BiFunction<Stream, Vector, Vector> takeResolver, BiConsumer<Stream, Vector> releaseResolver, SinglePositionResolver singlePositionResolver)
    {
        this(exposedStreams, resolver, takeResolver, releaseResolver, null, singlePositionResolver);
        OutputDebug.recordBaseOutput(false, false, singlePositionResolver != null);
    }

    public Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver, BiFunction<Stream, Vector, Vector> takeResolver, BiConsumer<Stream, Vector> releaseResolver, PositionsResolver positionsResolver, SinglePositionResolver singlePositionResolver)
    {
        this(exposedStreams, resolver, takeResolver, releaseResolver, positionsResolver, singlePositionResolver, 0, 0);
        OutputDebug.recordBaseOutput(false, positionsResolver != null, singlePositionResolver != null);
    }

    private Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver, BiFunction<Stream, Vector, Vector> takeResolver, BiConsumer<Stream, Vector> releaseResolver, PositionsResolver positionsResolver, SinglePositionResolver singlePositionResolver, int debugDepth, int knownAllFalseFlags)
    {
        requireNonNull(exposedStreams, "exposedStreams is null");
        this.exposedFlags = streamFlags(exposedStreams);
        this.resolver = requireNonNull(resolver, "resolver is null");
        this.takeResolver = requireNonNull(takeResolver, "takeResolver is null");
        this.releaseResolver = requireNonNull(releaseResolver, "releaseResolver is null");
        this.positionsResolver = positionsResolver;
        this.singlePositionResolver = singlePositionResolver;
        this.knownAllFalseFlags = knownAllFalseFlags & this.exposedFlags;
        this.debugDepth = debugDepth;
    }

    public Output withKnownAllFalse(Set<Stream> knownAllFalseStreams)
    {
        requireNonNull(knownAllFalseStreams, "knownAllFalseStreams is null");
        int additionalFlags = streamFlags(knownAllFalseStreams) & exposedFlags;
        if (additionalFlags == 0 || (knownAllFalseFlags | additionalFlags) == knownAllFalseFlags) {
            return this;
        }
        return new Output(
                streams(),
                resolver,
                takeResolver,
                releaseResolver,
                positionsResolver,
                singlePositionResolver,
                debugDepth,
                knownAllFalseFlags | additionalFlags);
    }

    public Vector borrow(Stream stream)
    {
        checkOpen();
        requireNonNull(stream, "stream is null");
        int flag = streamFlag(stream);
        int index = streamIndex(stream);
        if ((takenFlags & flag) != 0) {
            throw new IllegalStateException("Stream already taken: " + stream);
        }
        if ((exposedFlags & flag) == 0) {
            throw new IllegalArgumentException("Output does not expose stream: " + stream);
        }
        Vector resolved = resolvedStreams[index];
        if (resolved != null) {
            OutputDebug.recordBorrow(debugDepth, index, true);
            return resolved;
        }
        resolved = requireNonNull(resolver.apply(stream), "resolver returned null");
        resolvedStreams[index] = resolved;
        resolvedFlags |= flag;
        OutputDebug.recordBorrow(debugDepth, index, false);
        return resolved;
    }

    public Vector borrowOrNull(Stream stream)
    {
        checkOpen();
        requireNonNull(stream, "stream is null");
        if ((exposedFlags & streamFlag(stream)) == 0) {
            return null;
        }
        return borrow(stream);
    }

    public boolean has(Stream stream)
    {
        requireNonNull(stream, "stream is null");
        return (exposedFlags & streamFlag(stream)) != 0;
    }

    public boolean hasValues()
    {
        return (exposedFlags & VALUES_FLAG) != 0;
    }

    public boolean hasNulls()
    {
        return (exposedFlags & NULLS_FLAG) != 0;
    }

    public boolean hasErrors()
    {
        return (exposedFlags & ERRORS_FLAG) != 0;
    }

    public boolean isKnownAllFalse(Stream stream)
    {
        requireNonNull(stream, "stream is null");
        return (knownAllFalseFlags & streamFlag(stream)) != 0;
    }

    public Set<Stream> knownAllFalseStreams()
    {
        return STREAM_SETS[knownAllFalseFlags];
    }

    public boolean isValuesOnly()
    {
        return exposedFlags == VALUES_FLAG;
    }

    public Vector take(Stream stream)
    {
        checkOpen();
        OutputDebug.recordTake(debugDepth, streamIndex(stream));
        Vector vector = requireNonNull(takeResolver.apply(stream, borrow(stream)), "takeResolver returned null");
        takenFlags |= streamFlag(stream);
        return vector;
    }

    public Set<Stream> streams()
    {
        return STREAM_SETS[exposedFlags];
    }

    int flags()
    {
        return exposedFlags;
    }

    public Streams copySinglePosition(Streams existing, int sourcePosition, int outputPosition, int size)
    {
        checkOpen();
        if (singlePositionResolver != null) {
            OutputDebug.recordCopySinglePosition(debugDepth, true);
            return singlePositionResolver.copySinglePosition(existing, sourcePosition, outputPosition, size);
        }
        if (positionsResolver == null) {
            OutputDebug.recordCopySinglePosition(debugDepth, false);
            return null;
        }
        OutputDebug.recordCopySinglePosition(debugDepth, true);
        return positionsResolver.copyPositions(streams(), existing, new int[] {sourcePosition}, 0, 1, outputPosition, size, false);
    }

    public Streams copyPositions(Streams existing, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size, boolean assumeClearOutputRange)
    {
        checkOpen();
        if (sourceCount == 1 && singlePositionResolver != null) {
            OutputDebug.recordCopyPositions(debugDepth, true);
            return singlePositionResolver.copySinglePosition(existing, sourcePositions[sourceStart], outputStart, size);
        }
        boolean hit = positionsResolver != null;
        OutputDebug.recordCopyPositions(debugDepth, hit);
        return hit ? positionsResolver.copyPositions(streams(), existing, sourcePositions, sourceStart, sourceCount, outputStart, size, assumeClearOutputRange) : null;
    }

    public Output select(Set<Stream> selectedStreams)
    {
        requireNonNull(selectedStreams, "selectedStreams is null");
        int selectedFlags = streamFlags(selectedStreams);
        if ((selectedFlags & ~exposedFlags) != 0) {
            throw new IllegalArgumentException("Selected streams are not exposed by output");
        }
        if (selectedFlags == exposedFlags) {
            OutputDebug.recordSelectIdentityHit();
            return this;
        }
        OutputDebug.recordSelectedOutput(false, positionsResolver != null, false, debugDepth + 1);
        return new Output(selectedStreams, this::borrow, (stream, vector) -> take(stream), (_, _) -> {}, positionsResolver, null, debugDepth + 1, knownAllFalseFlags & selectedFlags);
    }

    public Output forward(BiFunction<Stream, Vector, Vector> takeResolver, BiConsumer<Stream, Vector> releaseResolver)
    {
        requireNonNull(takeResolver, "takeResolver is null");
        requireNonNull(releaseResolver, "releaseResolver is null");
        OutputDebug.recordForwardedOutput(false, positionsResolver != null, singlePositionResolver != null, debugDepth + 1);
        return new Output(streams(), this::borrow, takeResolver, releaseResolver, positionsResolver, singlePositionResolver, debugDepth + 1, knownAllFalseFlags);
    }

    public Output forwardSinglePositionOnly(BiFunction<Stream, Vector, Vector> takeResolver, BiConsumer<Stream, Vector> releaseResolver)
    {
        requireNonNull(takeResolver, "takeResolver is null");
        requireNonNull(releaseResolver, "releaseResolver is null");
        OutputDebug.recordForwardedOutput(false, false, singlePositionResolver != null, debugDepth + 1);
        return new Output(streams(), this::borrow, takeResolver, releaseResolver, null, singlePositionResolver, debugDepth + 1, knownAllFalseFlags);
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        OutputDebug.recordClose(debugDepth);
        closed = true;
        int flags = resolvedFlags & ~takenFlags;
        while (flags != 0) {
            int flag = Integer.lowestOneBit(flags);
            Stream stream = stream(flag);
            OutputDebug.recordRelease(debugDepth, streamIndex(stream));
            releaseResolver.accept(stream, resolvedStreams[streamIndex(stream)]);
            flags &= ~flag;
        }
        for (int index = 0; index < resolvedStreams.length; index++) {
            resolvedStreams[index] = null;
        }
        resolvedFlags = 0;
        takenFlags = 0;
    }

    private static int streamFlags(Set<Stream> streams)
    {
        int flags = 0;
        for (Stream stream : streams) {
            flags |= streamFlag(stream);
        }
        return flags;
    }

    private static int streamFlag(Stream stream)
    {
        return switch (stream) {
            case VALUES -> VALUES_FLAG;
            case NULLS -> NULLS_FLAG;
            case ERRORS -> ERRORS_FLAG;
        };
    }

    private static int streamIndex(Stream stream)
    {
        return switch (stream) {
            case VALUES -> 0;
            case NULLS -> 1;
            case ERRORS -> 2;
        };
    }

    private static Stream stream(int flag)
    {
        return switch (flag) {
            case VALUES_FLAG -> Stream.VALUES;
            case NULLS_FLAG -> Stream.NULLS;
            case ERRORS_FLAG -> Stream.ERRORS;
            default -> throw new IllegalArgumentException("Unknown stream flag: " + flag);
        };
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("Output already closed");
        }
    }
}
