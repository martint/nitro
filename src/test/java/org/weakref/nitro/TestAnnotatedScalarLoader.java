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
package org.weakref.nitro;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.core.function.mask.DirectMaskInputProvider;
import org.weakref.nitro.core.function.mask.MaskCodeProvider;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.function.scalar.AnnotatedScalarLoader;
import org.weakref.nitro.function.scalar.MaskEvaluablePrimitiveFunction;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;
import org.weakref.nitro.function.scalar.PrimitiveFunction;
import org.weakref.nitro.function.scalar.ScalarDescriptor;
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.function.scalar.builtin.BoundLikeUtf8;
import org.weakref.nitro.function.scalar.builtin.EqualF64;
import org.weakref.nitro.function.scalar.builtin.EqualF64Optimization;
import org.weakref.nitro.function.scalar.builtin.EqualI64;
import org.weakref.nitro.function.scalar.builtin.EqualI64Optimization;
import org.weakref.nitro.function.scalar.builtin.HandwrittenBigintAdd;
import org.weakref.nitro.function.scalar.builtin.IsNullDirectMaskOptimization;
import org.weakref.nitro.function.scalar.builtin.IsNullI64;
import org.weakref.nitro.function.scalar.builtin.LikeUtf8;
import org.weakref.nitro.function.scalar.builtin.LikeUtf8Policy;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class TestAnnotatedScalarLoader
{
    @Test
    void testLoadsAnnotatedScalarFunction()
    {
        ScalarDescriptor descriptor = new AnnotatedScalarLoader().load(TestHandwrittenBigintAdd.class);

        assertThat(descriptor.name()).isEqualTo("add");
        assertThat(descriptor.deterministic()).isTrue();
        assertThat(descriptor.implementation()).isInstanceOf(TestHandwrittenBigintAdd.class);
    }

    @Test
    void testPrimitiveRegistryRegistersVectorizedFunction()
    {
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();

        ScalarDescriptor descriptor = new AnnotatedScalarLoader().load(HandwrittenBigintAdd.class);
        primitiveRegistry.register(descriptor);

        assertThat(descriptor.implementation()).isInstanceOf(HandwrittenBigintAdd.class);
        assertThat(primitiveRegistry.get("handwritten_bigint_add")).isInstanceOf(HandwrittenBigintAdd.class);
    }

    @Test
    void testRegistersExplicitlyConstructedScalarFunction()
    {
        LikeUtf8 function = new LikeUtf8(new LikeUtf8Policy(false));

        ScalarDescriptor descriptor = new AnnotatedScalarLoader().load(function);

        assertThat(descriptor.name()).isEqualTo("like_utf8");
        assertThat(descriptor.implementation()).isSameAs(function);
    }

    @Test
    void testRegistersBoundLikeFunction()
    {
        BoundLikeUtf8 function = new BoundLikeUtf8("%special%requests%", new LikeUtf8Policy(false));

        ScalarDescriptor descriptor = new AnnotatedScalarLoader().load(function);

        assertThat(descriptor.name()).isEqualTo("bound_like_utf8");
        assertThat(descriptor.implementation()).isSameAs(function);
    }

    @Test
    void testRegistersDirectMaskInputAsCapabilityMetadata()
    {
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();

        ScalarDescriptor descriptor = new AnnotatedScalarLoader().load(IsNullI64.class);
        primitiveRegistry.register(descriptor);

        assertThat(descriptor.implementation()).isNotInstanceOf(MaskEvaluablePrimitiveFunction.class);
        DirectMaskInputProvider provider = descriptor.capabilities().stream()
                .filter(DirectMaskInputProvider.class::isInstance)
                .map(DirectMaskInputProvider.class::cast)
                .findFirst()
                .orElseThrow();
        assertThat(provider).isInstanceOf(IsNullDirectMaskOptimization.class);
        assertThat(provider.argumentCount()).isEqualTo(1);
        assertThat(provider.argumentIndex()).isZero();
        assertThat(provider.inputComponent()).isEqualTo(DirectMaskInputProvider.InputComponent.NULLS);
    }

    @Test
    void testRegistersProviderAuthoredMaskCodeAsCapabilityMetadata()
    {
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();

        ScalarDescriptor descriptor = new AnnotatedScalarLoader().load(EqualF64.class);
        primitiveRegistry.register(descriptor);

        assertThat(descriptor.implementation()).isNotInstanceOf(MaskEvaluablePrimitiveFunction.class);
        MaskCodeProvider provider = descriptor.capabilities().stream()
                .filter(MaskCodeProvider.class::isInstance)
                .map(MaskCodeProvider.class::cast)
                .findFirst()
                .orElseThrow();
        assertThat(provider).isInstanceOf(EqualF64Optimization.class);
    }

    @Test
    void testLongMaskExecutionComesFromCapabilityMetadata()
    {
        ScalarDescriptor descriptor = new AnnotatedScalarLoader().load(EqualI64.class);

        assertThat(descriptor.implementation()).isNotInstanceOf(MaskEvaluablePrimitiveFunction.class);
        assertThat(descriptor.capabilities()).anyMatch(EqualI64Optimization.class::isInstance);
    }

    @ScalarFunction(name = "add")
    public static final class TestHandwrittenBigintAdd
            implements PrimitiveFunction
    {
        @Override
        public Streams apply(java.util.List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
        {
            return new HandwrittenBigintAdd().apply(inputs, mask, requestedStreams, output, context);
        }
    }
}
