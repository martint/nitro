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
package org.weakref.nitro.operator.source.compatibility;

import org.weakref.nitro.core.batch.ColumnCapability;
import org.weakref.nitro.core.batch.ColumnTraits;
import org.weakref.nitro.core.batch.ColumnView;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Output;

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/// Read-only facade over a lazy native output.
final class NativeColumnView
        implements ColumnView
{
    private final TypeBinding type;
    private final int positionCount;
    private final ColumnTraits traits;
    private final Output output;

    NativeColumnView(TypeBinding type, int positionCount, ColumnTraits traits, Output output)
    {
        this.type = requireNonNull(type, "type is null");
        this.positionCount = positionCount;
        this.traits = requireNonNull(traits, "traits is null");
        this.output = requireNonNull(output, "output is null");
    }

    @Override
    public TypeBinding type()
    {
        return type;
    }

    @Override
    public int positionCount()
    {
        return positionCount;
    }

    @Override
    public ColumnTraits traits()
    {
        return traits;
    }

    @Override
    public Set<Stream> streams()
    {
        EnumSet<Stream> streams = EnumSet.noneOf(Stream.class);
        if (output.hasValues()) {
            streams.add(Stream.VALUES);
        }
        if (output.hasNulls()) {
            streams.add(Stream.NULLS);
        }
        if (output.hasErrors()) {
            streams.add(Stream.ERRORS);
        }
        return Set.copyOf(streams);
    }

    @Override
    public Vector borrow(Stream stream)
    {
        return output.borrow(requireNonNull(stream, "stream is null"));
    }

    @Override
    public Vector take(Stream stream)
    {
        return output.take(requireNonNull(stream, "stream is null"));
    }

    @Override
    public <T> Optional<T> capability(ColumnCapability<T> capability)
    {
        if (capability == NativeColumnCapability.NATIVE_COLUMN) {
            return Optional.of(capability.valueType().cast((NativeColumnAccess) () -> output));
        }
        return Optional.empty();
    }
}
