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
package org.weakref.nitro.clickbench;

import org.weakref.nitro.OperatorAssertions;
import org.weakref.nitro.TestPrimitiveFunctions;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.List;

/** Prints a row-count + digest of ClickBench queries so the same query can be diffed across config. */
public final class CbDigest
{
    private CbDigest() {}

    public static void main(String[] args)
            throws Exception
    {
        Path dir = Path.of(System.getProperty("nitro.clickbench.hits.path", "/root/data/clickbench"));
        PrimitiveRegistry registry = TestPrimitiveFunctions.primitiveRegistry();
        for (String name : args) {
            Operator operator;
            try {
                Method m = ClickBenchHitsSupport.class.getDeclaredMethod(name, Allocator.class, PrimitiveRegistry.class, Path.class);
                m.setAccessible(true);
                operator = (Operator) m.invoke(null, new Allocator(EngineResources.createDefault()), registry, dir);
            }
            catch (NoSuchMethodException e) {
                Method m = ClickBenchHitsSupport.class.getDeclaredMethod(name, Allocator.class, Path.class);
                m.setAccessible(true);
                operator = (Operator) m.invoke(null, new Allocator(EngineResources.createDefault()), dir);
            }
            List<?> rows;
            try (Operator op = operator) {
                rows = OperatorAssertions.OperatorAssert.toRows(op);
            }
            long digest = 1125899906842597L;
            for (Object row : rows) {
                digest = digest * 1000003L + String.valueOf(row).hashCode();
            }
            System.out.println(name + " rows=" + rows.size() + " digest=" + digest);
        }
    }
}
