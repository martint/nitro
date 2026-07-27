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
package org.weakref.nitro.architecture;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.core.batch.Selection;
import org.weakref.nitro.core.source.BatchSource;
import org.weakref.nitro.core.source.RuntimeFilter;
import org.weakref.nitro.core.source.SourceColumnHandle;
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAllocator;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.DynamicFilter;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.source.BatchSourceOperator;
import org.weakref.nitro.operator.source.ColumnViewSourceOperatorIngress;
import org.weakref.nitro.operator.source.RuntimeFilterSourceIngress;
import org.weakref.nitro.operator.source.SelectionOperatorIngress;
import org.weakref.nitro.operator.source.VectorColumnViewOperatorIngress;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class TestConnectorClassLoaderBoundary
{
    private static final String ISOLATED_SOURCE =
            "org.weakref.nitro.architecture.isolated.IsolatedBatchSource";
    private static final TypeBinding BIGINT = new TestingTypeBinding(
            new TypeIdentity("testing:bigint"),
            long.class,
            Set.of(I64Vector.class));

    @Test
    void testIsolatedSourceCrossesCoreAndVectorSpi()
            throws ReflectiveOperationException
    {
        Schema schema = new Schema(List.of(new Field("value", BIGINT, false)));
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            VectorAllocator connectorAllocator =
                    allocator.vectorAllocator(new Allocator.Context("isolated-connector"));
            BatchSource source = loadSource(schema, connectorAllocator);
            assertThat(source.getClass().getClassLoader()).isNotSameAs(getClass().getClassLoader());

            SelectionOperatorIngress selectionIngress = new SelectionOperatorIngress()
            {
                @Override
                public Mask toMask(Selection selection)
                {
                    if (selection.isDense()) {
                        return Mask.all(selection.positionCount());
                    }
                    int[] positions = new int[selection.count()];
                    for (int index = 0; index < positions.length; index++) {
                        positions[index] = selection.position(index);
                    }
                    return Mask.sparse(positions, selection.positionCount());
                }

                @Override
                public Selection toSelection(Mask mask)
                {
                    throw new UnsupportedOperationException();
                }
            };
            RuntimeFilterSourceIngress runtimeFilterIngress = new RuntimeFilterSourceIngress()
            {
                @Override
                public boolean supports(BatchSource source, SourceColumnHandle column)
                {
                    return false;
                }

                @Override
                public RuntimeFilter runtimeFilter(SourceColumnHandle column, DynamicFilter filter)
                {
                    throw new UnsupportedOperationException();
                }
            };

            try (Operator operator = new BatchSourceOperator(
                    source,
                    new ColumnViewSourceOperatorIngress(
                            schema,
                            selectionIngress,
                            runtimeFilterIngress,
                            VectorColumnViewOperatorIngress::new))) {
                assertThat(operator.exactOutputRows()).isEqualTo(2);
                try (Batch batch = operator.next()) {
                    I64Vector values = (I64Vector) batch.output(0).borrow(Stream.VALUES);
                    assertThat(values.values()).containsExactly(41, 42);
                }
                assertThat(operator.hasNext()).isFalse();
            }

            assertThat(source.getClass().getMethod("closed").invoke(source)).isEqualTo(true);
        }
    }

    private static BatchSource loadSource(Schema schema, VectorAllocator allocator)
            throws ReflectiveOperationException
    {
        ClassLoader connectorLoader = new ClassLoader(TestConnectorClassLoaderBoundary.class.getClassLoader())
        {
            @Override
            protected Class<?> loadClass(String name, boolean resolve)
                    throws ClassNotFoundException
            {
                synchronized (getClassLoadingLock(name)) {
                    if (!name.equals(ISOLATED_SOURCE) && !name.startsWith(ISOLATED_SOURCE + "$")) {
                        return super.loadClass(name, resolve);
                    }
                    Class<?> loaded = findLoadedClass(name);
                    if (loaded == null) {
                        byte[] bytes = classBytes(name);
                        loaded = defineClass(name, bytes, 0, bytes.length);
                    }
                    if (resolve) {
                        resolveClass(loaded);
                    }
                    return loaded;
                }
            }
        };
        return (BatchSource) connectorLoader.loadClass(ISOLATED_SOURCE)
                .getConstructor(Schema.class, VectorAllocator.class)
                .newInstance(schema, allocator);
    }

    private static byte[] classBytes(String name)
            throws ClassNotFoundException
    {
        String resource = name.replace('.', '/') + ".class";
        try (var input = TestConnectorClassLoaderBoundary.class.getClassLoader().getResourceAsStream(resource)) {
            if (input == null) {
                throw new ClassNotFoundException("Missing class bytes: " + resource);
            }
            return input.readAllBytes();
        }
        catch (IOException exception) {
            throw new ClassNotFoundException("Unable to read class bytes: " + resource, exception);
        }
    }

    private record TestingTypeBinding(
            TypeIdentity identity,
            Class<?> carrierType,
            Set<Class<? extends Vector>> supportedVectorTypes)
            implements TypeBinding
    {
        @Override
        public TypeOperators operators()
        {
            return TypeOperators.UNSPECIFIED;
        }
    }
}
