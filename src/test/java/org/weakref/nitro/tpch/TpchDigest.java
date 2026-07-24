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
package org.weakref.nitro.tpch;

import org.weakref.nitro.OperatorAssertions;
import org.weakref.nitro.TestPrimitiveFunctions;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;

import java.lang.reflect.Method;
import java.util.List;

/** Prints a row-count + digest of TPC-H queries so the same query can be diffed across config (e.g. group ceiling). */
public final class TpchDigest
{
    private TpchDigest() {}

    public static void main(String[] args)
            throws Exception
    {
        TpchParquetTables tables = TpchParquetTables.requiredActual();
        PrimitiveRegistry registry = TestPrimitiveFunctions.primitiveRegistry();
        for (String name : args) {
            Method method = TpchParquetSupport.class.getDeclaredMethod(
                    name, Allocator.class, PrimitiveRegistry.class, TpchParquetTables.class);
            method.setAccessible(true);
            List<?> rows;
            try (Operator operator = (Operator) method.invoke(null, new Allocator(EngineResources.createDefault()), registry, tables)) {
                rows = OperatorAssertions.OperatorAssert.toRows(operator);
            }
            long digest = 1125899906842597L;
            for (Object row : rows) {
                digest = digest * 1000003L + String.valueOf(row).hashCode();
            }
            System.out.println(name + " rows=" + rows.size() + " digest=" + digest);
        }
    }
}
