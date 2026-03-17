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

import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public final class Output
{
    private final EnumSet<Stream> exposedStreams;
    private final Set<Stream> exposedStreamsView;
    private final Function<Stream, Vector> resolver;
    private final BiFunction<Stream, Vector, Vector> takeResolver;
    private final EnumMap<Stream, Vector> resolvedStreams = new EnumMap<>(Stream.class);
    private final EnumSet<Stream> takenStreams = EnumSet.noneOf(Stream.class);

    public static Output of(Streams streams)
    {
        var streamsByKind = streams.asMap();
        EnumSet<Stream> exposedStreams = streamsByKind.isEmpty() ? EnumSet.noneOf(Stream.class) : EnumSet.copyOf(streamsByKind.keySet());
        return new Output(exposedStreams, streams::get);
    }

    public Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver)
    {
        this(exposedStreams, resolver, (_, vector) -> vector);
    }

    public Output(Set<Stream> exposedStreams, Function<Stream, Vector> resolver, BiFunction<Stream, Vector, Vector> takeResolver)
    {
        requireNonNull(exposedStreams, "exposedStreams is null");
        this.exposedStreams = exposedStreams.isEmpty() ? EnumSet.noneOf(Stream.class) : EnumSet.copyOf(exposedStreams);
        this.exposedStreamsView = Collections.unmodifiableSet(this.exposedStreams);
        this.resolver = requireNonNull(resolver, "resolver is null");
        this.takeResolver = requireNonNull(takeResolver, "takeResolver is null");
    }

    public Vector borrow(Stream stream)
    {
        requireNonNull(stream, "stream is null");
        if (takenStreams.contains(stream)) {
            throw new IllegalStateException("Stream already taken: " + stream);
        }
        if (!exposedStreams.contains(stream)) {
            throw new IllegalArgumentException("Output does not expose stream: " + stream);
        }
        return resolvedStreams.computeIfAbsent(stream, key -> requireNonNull(resolver.apply(key), "resolver returned null"));
    }

    public Vector borrowOrNull(Stream stream)
    {
        requireNonNull(stream, "stream is null");
        if (!exposedStreams.contains(stream)) {
            return null;
        }
        return borrow(stream);
    }

    public Vector take(Stream stream)
    {
        Vector vector = requireNonNull(takeResolver.apply(stream, borrow(stream)), "takeResolver returned null");
        takenStreams.add(stream);
        return vector;
    }

    public Set<Stream> streams()
    {
        return exposedStreamsView;
    }
}
