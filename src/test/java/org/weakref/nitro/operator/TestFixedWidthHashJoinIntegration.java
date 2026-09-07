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
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.FixedWidthKeyLayout;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.StructVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class TestFixedWidthHashJoinIntegration
{
    @Test
    void testProviderLayoutDrivesHashJoinBuildAndProbe()
    {
        TypeBinding pairType = pairType();
        Schema probeSchema = new Schema(List.of(new Field(pairType, false)));
        Schema buildSchema = new Schema(List.of(
                new Field(pairType, false),
                Schema.unspecified(1).field(0)));
        StructVector buildKeys = pairs(
                new long[] {1, 1, 2},
                new long[] {10, 10, 20});
        TableOperator build = new TableOperator(
                buildSchema,
                List.of(TableOperator.Page.values(
                        buildKeys.length(),
                        new Vector[] {buildKeys, new I64Vector(new long[] {100, 101, 200})},
                        Mask.all(buildKeys.length()))));

        try (EngineResources resources = EngineResources.createDefault();
                org.weakref.nitro.data.Allocator allocator = new org.weakref.nitro.data.Allocator(resources);
                HashJoinSession session = new HashJoinSession(
                        resources.operatorResources(),
                        allocator,
                        probeSchema,
                        new int[] {0},
                        build,
                        new int[] {0},
                        false)) {
            allocator.beginExecution();
            StructVector probeKeys = pairs(
                    new long[] {1, 2, 3},
                    new long[] {10, 20, 30});
            session.addInput(new Batch(
                    Mask.all(probeKeys.length()),
                    Output.of(Streams.ofValues(DictionaryVector.wrap(
                            new int[] {0, 1, 2},
                            probeKeys)))));

            List<Long> payloads = new ArrayList<>();
            while (session.hasOutput()) {
                try (Batch output = session.getOutput()) {
                    VectorAccess.LongValues values = VectorAccess.longValues(output.output(2).borrow(Stream.VALUES));
                    for (int position : output.borrowMask()) {
                        payloads.add(values.value(position));
                    }
                }
            }
            assertThat(payloads).containsExactly(100L, 101L, 200L);
        }
    }

    private static StructVector pairs(long[] high, long[] low)
    {
        StructVector pairs = new StructVector(high.length);
        pairs.setField("high", Streams.ofValues(new I64Vector(high)));
        pairs.setField("low", Streams.ofValues(new I64Vector(low)));
        return pairs;
    }

    private static TypeBinding pairType()
    {
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:fixed-width-hash-join-pair");
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
            public Optional<FixedWidthKeyLayout> fixedWidthKeyLayout()
            {
                return Optional.of(new FixedWidthKeyLayout(List.of(
                        FixedWidthKeyLayout.Lane.i64(List.of("high")),
                        FixedWidthKeyLayout.Lane.i64(List.of("low")))));
            }

            @Override
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return Set.of(StructVector.class, DictionaryVector.class);
            }
        };
    }
}
