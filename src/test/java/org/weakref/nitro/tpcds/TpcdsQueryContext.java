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
package org.weakref.nitro.tpcds;

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.operator.HashJoinOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;

import static java.util.Objects.requireNonNull;

/**
 * Explicit ownership boundary for constructing one TPC-DS operator tree.
 *
 * <p>This is benchmark/harness composition state, not an engine or connector SPI. It prevents optional diagnostics
 * from becoming ambient execution state while the TPC-DS query-construction graph is migrated.
 */
record TpcdsQueryContext(
        Allocator allocator,
        PrimitiveRegistry primitiveRegistry,
        TpcdsParquetTables tables,
        OperatorCpuProfile operatorCpuProfile)
{
    TpcdsQueryContext
    {
        requireNonNull(allocator, "allocator is null");
        requireNonNull(primitiveRegistry, "primitiveRegistry is null");
        requireNonNull(tables, "tables is null");
    }

    static TpcdsQueryContext unprofiled(
            Allocator allocator,
            PrimitiveRegistry primitiveRegistry,
            TpcdsParquetTables tables)
    {
        return new TpcdsQueryContext(allocator, primitiveRegistry, tables, null);
    }

    Operator profiled(String name, Operator operator)
    {
        if (operator instanceof HashJoinOperator hashJoinOperator) {
            hashJoinOperator.withProfileName(name);
        }
        if (operatorCpuProfile == null) {
            return operator;
        }
        return operatorCpuProfile.wrap(name, operator);
    }
}
