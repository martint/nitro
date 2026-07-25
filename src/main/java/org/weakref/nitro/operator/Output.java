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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;

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

    @FunctionalInterface
    public interface MaskedResolver
    {
        Vector resolve(Stream stream, Mask mask);
    }

    @FunctionalInterface
    public interface MaskResolver
    {
        Mask resolve(Stream stream, Mask mask, boolean selectTrue, Allocator allocator, Allocator.Context allocationContext);
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
    private final MaskedResolver maskedResolver;
    private final MaskResolver maskResolver;
    private final BiFunction<Stream, Vector, Vector> takeResolver;
    private final BiConsumer<Stream, Vector> releaseResolver;
    private final PositionsResolver positionsResolver;
    private final SinglePositionResolver singlePositionResolver;
    private final int knownAllFalseFlags;
    private Vector resolvedValues;
    private Vector resolvedNulls;
    private Vector resolvedErrors;
    private int resolvedFlags;
    private int takenFlags;
    private boolean constraintSensitiveResolution;
    private boolean closed;

    public static Output of(Streams streams)
    {
        return new Output(streams.streams(), streams::get);
    }

    public Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver)
    {
        this(exposedStreams, resolver, (_, vector) -> vector, (_, _) -> {}, null, null);
    }

    public Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver, BiFunction<Stream, Vector, Vector> takeResolver)
    {
        this(exposedStreams, resolver, takeResolver, (_, _) -> {}, null, null);
    }

    public Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver, BiFunction<Stream, Vector, Vector> takeResolver, BiConsumer<Stream, Vector> releaseResolver)
    {
        this(exposedStreams, resolver, takeResolver, releaseResolver, null, null);
    }

    /** Creates an output whose resolved buffers participate in the supplied reusable batch scope. */
    public Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver, BatchBufferOwner bufferOwner)
    {
        this(exposedStreams, resolver, null, null, requireNonNull(bufferOwner, "bufferOwner is null"), bufferOwner, null, null, 0);
    }

    /** Full lazy-resolution form for scoped outputs that support masked resolution and direct copy operations. */
    public Output(
            Set<Stream> exposedStreams,
            Function<Stream, Vector> resolver,
            MaskedResolver maskedResolver,
            MaskResolver maskResolver,
            PositionsResolver positionsResolver,
            SinglePositionResolver singlePositionResolver,
            BatchBufferOwner bufferOwner)
    {
        this(exposedStreams, resolver, maskedResolver, maskResolver, requireNonNull(bufferOwner, "bufferOwner is null"), bufferOwner, positionsResolver, singlePositionResolver, 0);
    }

    public Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver, BiFunction<Stream, Vector, Vector> takeResolver, BiConsumer<Stream, Vector> releaseResolver, SinglePositionResolver singlePositionResolver)
    {
        this(exposedStreams, resolver, takeResolver, releaseResolver, null, singlePositionResolver);
    }

    public Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver, BiFunction<Stream, Vector, Vector> takeResolver, BiConsumer<Stream, Vector> releaseResolver, PositionsResolver positionsResolver, SinglePositionResolver singlePositionResolver)
    {
        this(exposedStreams, resolver, null, null, takeResolver, releaseResolver, positionsResolver, singlePositionResolver, 0);
    }

    public Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver, MaskedResolver maskedResolver, BiFunction<Stream, Vector, Vector> takeResolver, BiConsumer<Stream, Vector> releaseResolver, PositionsResolver positionsResolver, SinglePositionResolver singlePositionResolver)
    {
        this(exposedStreams, resolver, maskedResolver, null, takeResolver, releaseResolver, positionsResolver, singlePositionResolver, 0);
    }

    public Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver, MaskedResolver maskedResolver, MaskResolver maskResolver, BiFunction<Stream, Vector, Vector> takeResolver, BiConsumer<Stream, Vector> releaseResolver, PositionsResolver positionsResolver, SinglePositionResolver singlePositionResolver)
    {
        this(exposedStreams, resolver, maskedResolver, maskResolver, takeResolver, releaseResolver, positionsResolver, singlePositionResolver, 0);
    }

    private Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver, MaskedResolver maskedResolver, MaskResolver maskResolver, BiFunction<Stream, Vector, Vector> takeResolver, BiConsumer<Stream, Vector> releaseResolver, PositionsResolver positionsResolver, SinglePositionResolver singlePositionResolver, int knownAllFalseFlags)
    {
        requireNonNull(exposedStreams, "exposedStreams is null");
        this.exposedFlags = streamFlags(exposedStreams);
        this.resolver = requireNonNull(resolver, "resolver is null");
        this.maskedResolver = maskedResolver;
        this.maskResolver = maskResolver;
        this.takeResolver = requireNonNull(takeResolver, "takeResolver is null");
        this.releaseResolver = requireNonNull(releaseResolver, "releaseResolver is null");
        this.positionsResolver = positionsResolver;
        this.singlePositionResolver = singlePositionResolver;
        this.knownAllFalseFlags = knownAllFalseFlags & this.exposedFlags;
    }

    public Output withKnownAllFalse(Set<Stream> knownAllFalseStreams)
    {
        requireNonNull(knownAllFalseStreams, "knownAllFalseStreams is null");
        int additionalFlags = streamFlags(knownAllFalseStreams) & exposedFlags;
        if (additionalFlags == 0 || (knownAllFalseFlags | additionalFlags) == knownAllFalseFlags) {
            return this;
        }
        Output output = new Output(
                streams(),
                resolver,
                maskedResolver,
                maskResolver,
                takeResolver,
                releaseResolver,
                positionsResolver,
                singlePositionResolver,
                knownAllFalseFlags | additionalFlags);
        return output;
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
        if (maskedResolver != null) {
            return resolveMasked(stream, null, flag, index);
        }
        Vector resolved = resolvedStream(index);
        if (resolved != null) {
            return resolved;
        }
        resolved = requireNonNull(resolver.apply(stream), "resolver returned null");
        resolvedStream(index, resolved);
        resolvedFlags |= flag;
        return resolved;
    }

    public Vector borrow(Stream stream, Mask mask)
    {
        checkOpen();
        requireNonNull(stream, "stream is null");
        requireNonNull(mask, "mask is null");
        int flag = streamFlag(stream);
        int index = streamIndex(stream);
        if ((takenFlags & flag) != 0) {
            throw new IllegalStateException("Stream already taken: " + stream);
        }
        if ((exposedFlags & flag) == 0) {
            throw new IllegalArgumentException("Output does not expose stream: " + stream);
        }
        if (maskedResolver == null) {
            return borrow(stream);
        }
        return resolveMasked(stream, mask, flag, index);
    }

    private Vector resolveMasked(Stream stream, Mask mask, int flag, int index)
    {
        Vector previous = resolvedStream(index);
        Vector resolved = requireNonNull(maskedResolver.resolve(stream, mask), "maskedResolver returned null");
        if (previous != null && previous != resolved) {
            release(stream, previous);
        }
        resolvedStream(index, resolved);
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

    public Vector borrowOrNull(Stream stream, Mask mask)
    {
        checkOpen();
        requireNonNull(stream, "stream is null");
        requireNonNull(mask, "mask is null");
        if ((exposedFlags & streamFlag(stream)) == 0) {
            return null;
        }
        return borrow(stream, mask);
    }

    public Mask tryBorrowMask(Stream stream, Mask mask, boolean selectTrue, Allocator allocator, Allocator.Context allocationContext)
    {
        checkOpen();
        requireNonNull(stream, "stream is null");
        requireNonNull(mask, "mask is null");
        requireNonNull(allocator, "allocator is null");
        requireNonNull(allocationContext, "allocationContext is null");
        if ((exposedFlags & streamFlag(stream)) == 0 || maskResolver == null) {
            return null;
        }
        if ((takenFlags & streamFlag(stream)) != 0) {
            throw new IllegalStateException("Stream already taken: " + stream);
        }
        return maskResolver.resolve(stream, mask, selectTrue, allocator, allocationContext);
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
            return singlePositionResolver.copySinglePosition(existing, sourcePosition, outputPosition, size);
        }
        if (positionsResolver == null) {
            return null;
        }
        return positionsResolver.copyPositions(streams(), existing, new int[] {sourcePosition}, 0, 1, outputPosition, size, false);
    }

    public Streams copyPositions(Streams existing, int[] sourcePositions, int sourceStart, int sourceCount, int outputStart, int size, boolean assumeClearOutputRange)
    {
        checkOpen();
        if (sourceCount == 1 && singlePositionResolver != null) {
            return singlePositionResolver.copySinglePosition(existing, sourcePositions[sourceStart], outputStart, size);
        }
        boolean hit = positionsResolver != null;
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
            return this;
        }
        return new Output(selectedStreams, this::borrow, this::borrowMaybeMasked, this::tryBorrowMask, (stream, vector) -> take(stream), (_, _) -> {}, positionsResolver, null, knownAllFalseFlags & selectedFlags);
    }

    public Output forward(BiFunction<Stream, Vector, Vector> takeResolver, BiConsumer<Stream, Vector> releaseResolver)
    {
        requireNonNull(takeResolver, "takeResolver is null");
        requireNonNull(releaseResolver, "releaseResolver is null");
        return new Output(streams(), this::borrow, this::borrowMaybeMasked, this::tryBorrowMask, takeResolver, releaseResolver, positionsResolver, singlePositionResolver, knownAllFalseFlags);
    }

    public Output forwardSinglePositionOnly(BiFunction<Stream, Vector, Vector> takeResolver, BiConsumer<Stream, Vector> releaseResolver)
    {
        requireNonNull(takeResolver, "takeResolver is null");
        requireNonNull(releaseResolver, "releaseResolver is null");
        return new Output(streams(), this::borrow, this::borrowMaybeMasked, this::tryBorrowMask, takeResolver, releaseResolver, null, singlePositionResolver, knownAllFalseFlags);
    }

    /** Marks cached streams as mask-sensitive so {@link Batch#constrain(Mask)} releases them before re-borrow. */
    public Output withConstraintSensitiveResolution()
    {
        constraintSensitiveResolution = true;
        return this;
    }

    boolean hasConstraintSensitiveTakenStreams()
    {
        return constraintSensitiveResolution && takenFlags != 0;
    }

    /**
     * Drops mask-sensitive cached streams before a constrained re-borrow. A taken stream belongs to the caller and
     * cannot be invalidated safely; {@link Batch} checks that invariant for every output before invalidating any of
     * them. Untaken streams remain owned here and are released through their normal resolver contract.
     */
    void invalidateResolvedForConstraint()
    {
        if (!constraintSensitiveResolution) {
            return;
        }
        checkOpen();
        if (takenFlags != 0) {
            throw new IllegalStateException("Cannot constrain batch after an output stream was taken");
        }
        int flags = resolvedFlags;
        while (flags != 0) {
            int flag = Integer.lowestOneBit(flags);
            Stream stream = stream(flag);
            release(stream, resolvedStream(streamIndex(stream)));
            flags &= ~flag;
        }
        resolvedValues = null;
        resolvedNulls = null;
        resolvedErrors = null;
        resolvedFlags = 0;
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
            release(stream, resolvedStream(streamIndex(stream)));
            flags &= ~flag;
        }
        resolvedValues = null;
        resolvedNulls = null;
        resolvedErrors = null;
        resolvedFlags = 0;
        takenFlags = 0;
    }

    private static int streamFlags(Set<Stream> streams)
    {
        int flags = 0;
        if (streams.contains(Stream.VALUES)) {
            flags |= VALUES_FLAG;
        }
        if (streams.contains(Stream.NULLS)) {
            flags |= NULLS_FLAG;
        }
        if (streams.contains(Stream.ERRORS)) {
            flags |= ERRORS_FLAG;
        }
        return flags;
    }

    private Vector borrowMaybeMasked(Stream stream, Mask mask)
    {
        return mask == null ? borrow(stream) : borrow(stream, mask);
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

    private Vector resolvedStream(int index)
    {
        return switch (index) {
            case 0 -> resolvedValues;
            case 1 -> resolvedNulls;
            case 2 -> resolvedErrors;
            default -> throw new IllegalArgumentException("Unknown stream index: " + index);
        };
    }

    private void release(Stream stream, Vector vector)
    {
        try {
            releaseResolver.accept(stream, vector);
        }
        finally {
            vector.releaseTransferredBuffers();
        }
    }

    private void resolvedStream(int index, Vector vector)
    {
        switch (index) {
            case 0 -> resolvedValues = vector;
            case 1 -> resolvedNulls = vector;
            case 2 -> resolvedErrors = vector;
            default -> throw new IllegalArgumentException("Unknown stream index: " + index);
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("Output already closed");
        }
    }
}
