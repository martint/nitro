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
    private final SinglePositionResolver singlePositionResolver;
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
        this(exposedStreams, resolver, (_, vector) -> vector, (_, _) -> {}, null);
    }

    public Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver, BiFunction<Stream, Vector, Vector> takeResolver)
    {
        this(exposedStreams, resolver, takeResolver, (_, _) -> {}, null);
    }

    public Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver, BiFunction<Stream, Vector, Vector> takeResolver, BiConsumer<Stream, Vector> releaseResolver)
    {
        this(exposedStreams, resolver, takeResolver, releaseResolver, null);
    }

    public Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver, BiFunction<Stream, Vector, Vector> takeResolver, BiConsumer<Stream, Vector> releaseResolver, SinglePositionResolver singlePositionResolver)
    {
        requireNonNull(exposedStreams, "exposedStreams is null");
        this.exposedFlags = streamFlags(exposedStreams);
        this.resolver = requireNonNull(resolver, "resolver is null");
        this.takeResolver = requireNonNull(takeResolver, "takeResolver is null");
        this.releaseResolver = requireNonNull(releaseResolver, "releaseResolver is null");
        this.singlePositionResolver = singlePositionResolver;
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
            return resolved;
        }
        resolved = requireNonNull(resolver.apply(stream), "resolver returned null");
        resolvedStreams[index] = resolved;
        resolvedFlags |= flag;
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

    public boolean isValuesOnly()
    {
        return exposedFlags == VALUES_FLAG;
    }

    public Vector take(Stream stream)
    {
        checkOpen();
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
        return singlePositionResolver == null ? null : singlePositionResolver.copySinglePosition(existing, sourcePosition, outputPosition, size);
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        int flags = resolvedFlags & ~takenFlags;
        while (flags != 0) {
            int flag = Integer.lowestOneBit(flags);
            Stream stream = stream(flag);
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
