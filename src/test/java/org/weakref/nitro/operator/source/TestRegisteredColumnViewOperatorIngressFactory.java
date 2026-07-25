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
import org.weakref.nitro.core.batch.ColumnView;
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.operator.Output;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class TestRegisteredColumnViewOperatorIngressFactory
{
    private static final TypeBinding FIRST = new TestingTypeBinding(new TypeIdentity("provider:first"));
    private static final TypeBinding SECOND = new TestingTypeBinding(new TypeIdentity("provider:second"));
    private static final TypeBinding MISSING = new TestingTypeBinding(new TypeIdentity("provider:missing"));

    @Test
    void testRoutesOpaqueTypeIdentityToRegisteredProvider()
    {
        ColumnViewOperatorIngress first = new TestingIngress(FIRST);
        ColumnViewOperatorIngress second = new TestingIngress(SECOND);
        RegisteredColumnViewOperatorIngressFactory registry = new RegisteredColumnViewOperatorIngressFactory(Map.of(
                FIRST.identity(), _ -> first,
                SECOND.identity(), _ -> second));

        assertThat(registry.bind(new Field(FIRST, false))).isSameAs(first);
        assertThat(registry.bind(new Field(SECOND, true))).isSameAs(second);
    }

    @Test
    void testRejectsMissingAndMismatchedProviders()
    {
        RegisteredColumnViewOperatorIngressFactory missing = new RegisteredColumnViewOperatorIngressFactory(Map.of());
        assertThatIllegalArgumentException()
                .isThrownBy(() -> missing.bind(new Field(MISSING, false)))
                .withMessageContaining("No column ingress provider registered");

        RegisteredColumnViewOperatorIngressFactory mismatched = new RegisteredColumnViewOperatorIngressFactory(Map.of(
                FIRST.identity(), _ -> new TestingIngress(SECOND)));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> mismatched.bind(new Field(FIRST, false)))
                .withMessageContaining("different type");
    }

    private record TestingTypeBinding(TypeIdentity identity)
            implements TypeBinding
    {
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
    }

    private record TestingIngress(TypeBinding type)
            implements ColumnViewOperatorIngress
    {
        @Override
        public Output output(Supplier<ColumnView> column)
        {
            return new Output(Set.of(Stream.VALUES), _ -> {
                throw new AssertionError("not resolved");
            });
        }
    }
}
