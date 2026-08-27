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

import org.junit.jupiter.api.Test;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.core.type.TypeVectorFactory;
import org.weakref.nitro.data.AllocationResources;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.ErrorValue;
import org.weakref.nitro.data.ErrorVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.StructVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAllocator;

import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.operator.PartitionEndpointWindowFunction.Endpoint.FIRST;
import static org.weakref.nitro.operator.PartitionEndpointWindowFunction.Endpoint.LAST;

class TestPartitionEndpointWindowFunction
{
    @Test
    void broadcastsPrimitiveEndpointsAndPreservesOptionalStreamsAcrossPartitions()
    {
        try (AllocationResources resources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            Allocator.Context context = new Allocator.Context("test");
            PartitionEndpointWindowFunction first = new PartitionEndpointWindowFunction(i64Type(), 0, FIRST);
            Streams output = first.emptyOutput(allocator, context, 5);

            ErrorVector errors = new ErrorVector(2);
            ErrorValue failure = new ErrorValue("test", 1, "BAD_VALUE", "USER_ERROR", "bad value");
            errors.setError(0, failure);
            Streams firstPage = Streams.of(
                    new I64Vector(new long[] {11, 12}),
                    new BooleanVector(new boolean[] {false, true}),
                    errors);
            output = first.append(allocator, context, output, new Streams[] {firstPage}, 0, 0, 5);
            output = first.append(allocator, context, output, new Streams[] {firstPage}, 1, 1, 5);
            output = first.finishPartition(allocator, context, output, 0, 2, 5);

            first.reset();
            Streams secondPage = Streams.ofValues(new I64Vector(new long[] {21, 22, 23}));
            output = first.append(allocator, context, output, new Streams[] {secondPage}, 0, 2, 5);
            output = first.append(allocator, context, output, new Streams[] {secondPage}, 1, 3, 5);
            output = first.append(allocator, context, output, new Streams[] {secondPage}, 2, 4, 5);
            output = first.finishPartition(allocator, context, output, 2, 5, 5);

            assertThat(((I64Vector) output.values()).values()).containsExactly(11, 11, 21, 21, 21);
            assertThat(((BooleanVector) output.get(Stream.NULLS)).values()).containsOnly(false);
            ErrorVector outputErrors = (ErrorVector) output.get(Stream.ERRORS);
            assertThat(outputErrors.values()).containsExactly(true, true, false, false, false);
            assertThat(outputErrors.error(0)).isSameAs(failure);
            assertThat(outputErrors.error(1)).isSameAs(failure);
            assertThat(outputErrors.error(2)).isNull();
            allocator.release(context);
        }
    }

    @Test
    void broadcastsLastNestedValueWithoutReadingLogicalTypeIdentity()
    {
        try (AllocationResources resources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            Allocator.Context context = new Allocator.Context("test");
            PartitionEndpointWindowFunction last = new PartitionEndpointWindowFunction(structType(), 0, LAST);
            Streams output = last.emptyOutput(allocator, context, 3);

            StructVector structs = new StructVector(3);
            structs.setField("value", Streams.ofValues(new I64Vector(new long[] {7, 8, 9})));
            Streams source = Streams.ofValues(structs);
            for (int position = 0; position < 3; position++) {
                output = last.append(allocator, context, output, new Streams[] {source}, position, position, 3);
            }
            output = last.finishPartition(allocator, context, output, 0, 3, 3);

            StructVector values = (StructVector) output.values();
            assertThat(((I64Vector) values.field("value").values()).values()).containsExactly(9, 9, 9);
            allocator.release(context);
        }
    }

    private static TypeBinding i64Type()
    {
        return type(
                "testing:i64",
                Set.of(I64Vector.class),
                (allocator, length) -> allocator.allocate(I64Vector.class, length, I64Vector::new));
    }

    private static TypeBinding structType()
    {
        return type(
                "testing:struct",
                Set.of(StructVector.class),
                (allocator, length) -> {
                    StructVector values = allocator.allocate(StructVector.class, length, StructVector::new);
                    values.setField("value", Streams.ofValues(allocator.allocate(I64Vector.class, length, I64Vector::new)));
                    return values;
                });
    }

    private static TypeBinding type(String identity, Set<Class<? extends Vector>> vectorTypes, NullValues nullValues)
    {
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity(identity);
            }

            @Override
            public Class<?> carrierType()
            {
                return Object.class;
            }

            @Override
            public TypeOperators operators()
            {
                return TypeOperators.UNSPECIFIED;
            }

            @Override
            public Optional<TypeVectorFactory> vectorFactory()
            {
                return Optional.of(new TypeVectorFactory()
                {
                    @Override
                    public Vector constant(VectorAllocator allocator, Object value, int length)
                    {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Vector nullValues(VectorAllocator allocator, int length)
                    {
                        return nullValues.create(allocator, length);
                    }
                });
            }

            @Override
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return vectorTypes;
            }
        };
    }

    @FunctionalInterface
    private interface NullValues
    {
        Vector create(VectorAllocator allocator, int length);
    }
}
