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

import org.weakref.nitro.TestPrimitiveFunctions;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.FilterOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Literal;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.ReferenceMask;
import org.weakref.nitro.operator.evaluator.ir.Variable;
import org.weakref.nitro.parquet.NitroParquetScanOperator;
import org.weakref.nitro.parquet.NitroParquetScanResources;

import java.util.List;

/**
 * Dumps a checksum of the surviving {@code t_time_sk} values of {@code time_dim} filtered by {@code t_hour=20},
 * pulled as a masked payload, so the late-materialization masked path can be compared on/off.
 */
public final class DebugLazyScan
{
    private DebugLazyScan() {}

    public static void main(String[] args)
    {
        EngineResources engineResources = EngineResources.createDefault();
        Allocator allocator = new Allocator(engineResources);
        PrimitiveRegistry registry = TestPrimitiveFunctions.primitiveRegistry();
        TpcdsParquetTables tables = TpcdsParquetTables.requiredActual("sf10");
        // Large multi-page constrained FACT scan: filter on ss_quantity, masked-read nullable ss_customer_sk.
        Operator scan = new NitroParquetScanOperator(NitroParquetScanResources.createDefault(), allocator, tables.tableFiles("store_sales"), List.of("ss_quantity", "ss_customer_sk"));

        Variable literal = new Variable(0);
        Variable greater = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(50L), AllMask.ALL),
                new Assignment(greater, new Call("lt", List.of(
                        new Reference(literal, Stream.VALUES),
                        new Reference(new Input(0), Stream.VALUES))), AllMask.ALL)), List.of());
        Operator filter = new FilterOperator(
                scan,
                plan,
                registry,
                new ReferenceMask(new Reference(greater, Stream.VALUES)),
                allocator,
                engineResources.operatorResources().filter());
        // GROUP BY masked nullable ss_customer_sk (index 1), SUM(ss_quantity index 0).
        Operator grouped = new org.weakref.nitro.operator.GroupedAggregationOperator(
                allocator,
                List.of(Integer.valueOf(1)),
                List.of(new org.weakref.nitro.operator.aggregation.Sum(0)),
                filter);

        long groups = 0;
        long keySum = 0;
        long aggSum = 0;
        while (grouped.hasNext()) {
            Batch batch = grouped.next();
            org.weakref.nitro.data.Mask mask = batch.borrowMask();
            long[] keys = ((org.weakref.nitro.data.I64Vector) batch.output(0).borrow(Stream.VALUES)).values();
            boolean[] keyNulls = ((org.weakref.nitro.data.BooleanVector) batch.output(0).borrow(Stream.NULLS)).values();
            long[] agg = ((org.weakref.nitro.data.I64Vector) batch.output(1).borrow(Stream.VALUES)).values();
            int c = mask.selectedCount();
            for (int i = 0; i < c; i++) {
                int pos = mask.position(i);
                groups++;
                if (!keyNulls[pos]) {
                    keySum += keys[pos];
                }
                aggSum += agg[pos];
            }
            batch.close();
        }
        grouped.close();
        System.out.println("groups=" + groups + " keySum=" + keySum + " aggSum=" + aggSum);
    }
}
