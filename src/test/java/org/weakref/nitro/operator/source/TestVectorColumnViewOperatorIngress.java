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

import org.junit.jupiter.api.Test;
import org.weakref.nitro.core.batch.ColumnCapability;
import org.weakref.nitro.core.batch.ColumnEncoding;
import org.weakref.nitro.core.batch.ColumnTraits;
import org.weakref.nitro.core.batch.ColumnView;
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorColumnCapability;
import org.weakref.nitro.data.VectorColumnGeneration;
import org.weakref.nitro.operator.Output;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class TestVectorColumnViewOperatorIngress
{
    private static final TypeBinding TYPE = new TestingTypeBinding();

    @Test
    void testPreservesSourceStreamsAndTransfersVectorOwnership()
    {
        I64Vector values = new I64Vector(new long[] {11, 22});
        BooleanVector nulls = new BooleanVector(new boolean[] {false, true});
        AtomicInteger columnRequests = new AtomicInteger();
        AtomicBoolean valuesTaken = new AtomicBoolean();
        ColumnView column = new TestingColumnView(values, nulls, valuesTaken);
        Output output = new VectorColumnViewOperatorIngress(new Field(TYPE, true))
                .output(() -> {
                    columnRequests.incrementAndGet();
                    return column;
                });

        assertThat(columnRequests).hasValue(1);
        assertThat(output.streams()).containsExactlyInAnyOrder(Stream.VALUES, Stream.NULLS);
        assertThat(output.borrow(Stream.VALUES)).isSameAs(values);
        assertThat(columnRequests).hasValue(1);
        assertThat(output.take(Stream.VALUES)).isSameAs(values);
        assertThat(valuesTaken).isTrue();
        assertThat(output.borrow(Stream.NULLS)).isSameAs(nulls);
        output.close();
    }

    @Test
    void testRejectsVectorOutsideTypeContract()
    {
        ColumnView column = new TestingColumnView(
                new F64Vector(new double[] {1}),
                null,
                new AtomicBoolean());
        Output output = new VectorColumnViewOperatorIngress(new Field(TYPE, false))
                .output(() -> column);

        assertThatIllegalArgumentException()
                .isThrownBy(() -> output.borrow(Stream.VALUES))
                .withMessageContaining(F64Vector.class.getName())
                .withMessageContaining(TYPE.identity().toString())
                .withMessageContaining(I64Vector.class.getName());
        output.close();
    }

    @Test
    void testPreservesVectorGenerationMaskedResolution()
    {
        I64Vector values = new I64Vector(new long[] {11, 22});
        AtomicInteger unconstrainedResolutions = new AtomicInteger();
        AtomicInteger maskedResolutions = new AtomicInteger();
        VectorColumnGeneration generation = new VectorColumnGeneration(
                Set.of(Stream.VALUES),
                _ -> values,
                (_, mask) -> {
                    if (mask == null) {
                        unconstrainedResolutions.incrementAndGet();
                    }
                    else {
                        maskedResolutions.incrementAndGet();
                    }
                    return values;
                },
                (_, vector) -> vector,
                (_, _) -> {},
                null,
                null);
        ColumnView column = new TestingColumnView(values, null, new AtomicBoolean(), generation);
        AtomicInteger columnRequests = new AtomicInteger();
        Output output = new VectorColumnViewOperatorIngress(new Field(TYPE, true))
                .output(() -> {
                    columnRequests.incrementAndGet();
                    return column;
                });

        assertThat(columnRequests).hasValue(1);
        assertThat(output.streams()).containsExactly(Stream.VALUES);
        assertThat(output.borrow(Stream.VALUES)).isSameAs(values);
        assertThat(unconstrainedResolutions).hasValue(1);
        assertThat(output.borrow(Stream.VALUES, Mask.all(2))).isSameAs(values);
        assertThat(columnRequests).hasValue(1);
        assertThat(maskedResolutions).hasValue(1);
        output.close();
    }

    private static final class TestingColumnView
            implements ColumnView
    {
        private Vector values;
        private Vector nulls;
        private final AtomicBoolean valuesTaken;
        private final VectorColumnGeneration generation;

        private TestingColumnView(Vector values, Vector nulls, AtomicBoolean valuesTaken)
        {
            this(values, nulls, valuesTaken, null);
        }

        private TestingColumnView(Vector values, Vector nulls, AtomicBoolean valuesTaken, VectorColumnGeneration generation)
        {
            this.values = values;
            this.nulls = nulls;
            this.valuesTaken = valuesTaken;
            this.generation = generation;
        }

        @Override
        public TypeBinding type()
        {
            return TYPE;
        }

        @Override
        public int positionCount()
        {
            return values.length();
        }

        @Override
        public ColumnTraits traits()
        {
            return new ColumnTraits(ColumnEncoding.FLAT, nulls != null, true, true);
        }

        @Override
        public Set<Stream> streams()
        {
            return nulls == null
                    ? Set.of(Stream.VALUES)
                    : Set.of(Stream.VALUES, Stream.NULLS);
        }

        @Override
        public Vector borrow(Stream stream)
        {
            return switch (stream) {
                case VALUES -> values;
                case NULLS -> nulls;
                case ERRORS -> throw new IllegalArgumentException("no errors");
            };
        }

        @Override
        public Vector take(Stream stream)
        {
            Vector vector = borrow(stream);
            switch (stream) {
                case VALUES -> {
                    valuesTaken.set(true);
                    values = null;
                }
                case NULLS -> nulls = null;
                case ERRORS -> throw new IllegalArgumentException("no errors");
            }
            return vector;
        }

        @Override
        public <T> Optional<T> capability(ColumnCapability<T> capability)
        {
            if (capability == VectorColumnCapability.VECTOR_GENERATION && generation != null) {
                return Optional.of(capability.valueType().cast(generation));
            }
            return Optional.empty();
        }
    }

    private record TestingTypeBinding()
            implements TypeBinding
    {
        @Override
        public TypeIdentity identity()
        {
            return new TypeIdentity("testing:long");
        }

        @Override
        public Class<?> carrierType()
        {
            return long.class;
        }

        @Override
        public TypeOperators operators()
        {
            return TypeOperators.UNSPECIFIED;
        }

        @Override
        public Set<Class<? extends Vector>> supportedVectorTypes()
        {
            return Set.of(I64Vector.class);
        }
    }
}
