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
package org.weakref.nitro.core.type;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TestSchema
{
    private static final TypeBinding BIGINT = new TestingTypeBinding(new TypeIdentity("test:bigint"));
    private static final TypeBinding VARCHAR = new TestingTypeBinding(new TypeIdentity("test:varchar"));

    @Test
    void testLayoutCompatibilityIgnoresFieldNames()
    {
        Schema named = new Schema(List.of(new Field("value", BIGINT, false)));
        Schema unnamed = new Schema(List.of(new Field(BIGINT, false)));
        Schema renamed = new Schema(List.of(new Field("renamed", BIGINT, false)));

        assertThat(named.isLayoutCompatibleWith(unnamed)).isTrue();
        assertThat(unnamed.isLayoutCompatibleWith(named)).isTrue();
        assertThat(named.isLayoutCompatibleWith(renamed)).isTrue();
    }

    @Test
    void testLayoutCompatibilityRetainsPhysicalContract()
    {
        Schema schema = new Schema(List.of(new Field(BIGINT, false)));

        assertThat(schema.isLayoutCompatibleWith(new Schema(List.of()))).isFalse();
        assertThat(schema.isLayoutCompatibleWith(new Schema(List.of(new Field(VARCHAR, false))))).isFalse();
        assertThat(schema.isLayoutCompatibleWith(new Schema(List.of(new Field(BIGINT, true))))).isFalse();
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
}
