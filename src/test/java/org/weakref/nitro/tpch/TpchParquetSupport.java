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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.operator.AggregationOperator;
import org.weakref.nitro.operator.FilterOperator;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.MultiStageOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.SortOperator;
import org.weakref.nitro.operator.TrinoParquetScanOperator;
import org.weakref.nitro.operator.aggregation.AvgF64;
import org.weakref.nitro.operator.aggregation.CountAll;
import org.weakref.nitro.operator.aggregation.SumF64;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Literal;
import org.weakref.nitro.operator.evaluator.ir.MaskExpression;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.ReferenceMask;
import org.weakref.nitro.operator.evaluator.ir.Stream;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * The TPC-H queries as Nitro operator trees, mirroring Trino's optimized logical plans
 * (target/tpch-explain, regenerate via {@link ExplainTpchQueries}). Single-threaded shape: distributed
 * exchanges and partial/final aggregation pairs collapse to one operator.
 */
final class TpchParquetSupport
{
    private TpchParquetSupport() {}

    /**
     * Q1: lineitem scanned once; ScanFilterProject(l_shipdate <= 1998-09-02, disc price and charge expressions)
     * feeding the (l_returnflag, l_linestatus) aggregation, sorted by the group keys.
     */
    public static Operator query01(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        Operator lineitem = scannedTable(allocator, tables, "lineitem",
                "l_returnflag", "l_linestatus", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_shipdate");
        Operator filtered = filter(allocator, primitiveRegistry, lineitem,
                lessThan(6, LocalDate.of(1998, 9, 2).toEpochDay() + 1));

        // [flag, status, qty, extprice, disc, discPrice, charge]
        Variable one = new Variable(0);
        Variable oneMinusDiscount = new Variable(1);
        Variable discPrice = new Variable(2);
        Variable onePlusTax = new Variable(3);
        Variable charge = new Variable(4);
        Operator projected = new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(one, new Literal(1.0), AllMask.ALL),
                                new Assignment(oneMinusDiscount, new Call("subtract_f64", List.of(
                                        new Reference(one, Stream.VALUES),
                                        new Reference(new Input(4), Stream.VALUES))), AllMask.ALL),
                                new Assignment(discPrice, new Call("multiply_f64", List.of(
                                        new Reference(new Input(3), Stream.VALUES),
                                        new Reference(oneMinusDiscount, Stream.VALUES))), AllMask.ALL),
                                new Assignment(onePlusTax, new Call("add_f64", List.of(
                                        new Reference(new Input(5), Stream.VALUES),
                                        new Reference(one, Stream.VALUES))), AllMask.ALL),
                                new Assignment(charge, new Call("multiply_f64", List.of(
                                        new Reference(discPrice, Stream.VALUES),
                                        new Reference(onePlusTax, Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES),
                                new Reference(new Input(4), Stream.VALUES),
                                new Reference(discPrice, Stream.VALUES),
                                new Reference(charge, Stream.VALUES))),
                primitiveRegistry,
                filtered);

        // [flag, status, sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count]
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(
                        new SumF64(2),
                        new SumF64(3),
                        new SumF64(5),
                        new SumF64(6),
                        new AvgF64(2),
                        new AvgF64(3),
                        new AvgF64(4),
                        new CountAll()),
                projected);
        return new SortOperator(allocator, new int[] {0, 1}, new boolean[] {false, false}, aggregated);
    }

    /**
     * Q6: one ScanFilter over lineitem (shipdate year 1994, discount within [0.05, 0.07], quantity < 24),
     * projecting extendedprice * discount into a global sum.
     */
    public static Operator query06(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        Operator lineitem = scannedTable(allocator, tables, "lineitem",
                "l_shipdate", "l_discount", "l_quantity", "l_extendedprice");
        Operator filtered = filter(allocator, primitiveRegistry, lineitem,
                and(
                        greaterThan(0, LocalDate.of(1994, 1, 1).toEpochDay() - 1),
                        lessThan(0, LocalDate.of(1995, 1, 1).toEpochDay()),
                        // SQL's 0.06 +/- 0.01 is DECIMAL arithmetic: the bounds are exactly 0.05 and 0.07.
                        compareF64("gte_f64", 1, 0.05),
                        compareF64("lte_f64", 1, 0.07),
                        compareF64("lt_f64", 2, 24.0)));

        Variable product = new Variable(0);
        Operator projected = new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(new Assignment(product, new Call("multiply_f64", List.of(
                                new Reference(new Input(3), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))), AllMask.ALL)),
                        List.of(new Reference(product, Stream.VALUES))),
                primitiveRegistry,
                filtered);
        return new AggregationOperator(allocator, List.of(new SumF64(0)), projected);
    }

    // ---- scan / filter plumbing ----

    private static Operator scannedTable(Allocator allocator, TpchParquetTables tables, String tableName, String... columns)
    {
        List<Path> files = tables.tableFiles(tableName);
        return new MultiStageOperator(columns.length, files, (Function<Path, Operator>) path -> new TrinoParquetScanOperator(allocator, path, List.of(columns)));
    }

    private static Operator filter(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, FilterSpec filterSpec)
    {
        return new FilterOperator(source, filterSpec.plan(), primitiveRegistry, filterSpec.predicate(), allocator);
    }

    record FilterSpec(EvaluationPlan plan, MaskExpression predicate) {}

    private static FilterSpec greaterThan(int inputIndex, long constant)
    {
        // The engine has only lt over I64: constant < input.
        Variable literal = new Variable(0);
        Variable result = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(constant), AllMask.ALL),
                new Assignment(result, new Call("lt", List.of(
                        new Reference(literal, Stream.VALUES),
                        new Reference(new Input(inputIndex), Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(result, Stream.VALUES)));
    }

    private static FilterSpec lessThan(int inputIndex, long constant)
    {
        return comparison("lt", inputIndex, new Literal(constant));
    }

    private static FilterSpec compareF64(String function, int inputIndex, double constant)
    {
        return comparison(function, inputIndex, new Literal(constant));
    }

    private static FilterSpec comparison(String function, int inputIndex, Literal constant)
    {
        Variable literal = new Variable(0);
        Variable result = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(literal, constant, AllMask.ALL),
                new Assignment(result, new Call(function, List.of(
                        new Reference(new Input(inputIndex), Stream.VALUES),
                        new Reference(literal, Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(result, Stream.VALUES)));
    }

    private static FilterSpec and(FilterSpec first, FilterSpec... rest)
    {
        FilterSpec result = first;
        for (FilterSpec spec : rest) {
            result = combineBoolean("and", result, spec);
        }
        return result;
    }

    private static FilterSpec combineBoolean(String functionName, FilterSpec left, FilterSpec right)
    {
        int rightOffset = maxVariableId(left.plan().assignments()) + 1;
        List<Assignment> assignments = new ArrayList<>(left.plan().assignments());
        assignments.addAll(remap(right.plan().assignments(), rightOffset));

        Variable result = new Variable(maxVariableId(assignments) + 1);
        Reference leftReference = new Reference(left.plan().assignments().getLast().output(), Stream.VALUES);
        Reference rightReference = new Reference(new Variable(right.plan().assignments().getLast().output().id() + rightOffset), Stream.VALUES);
        assignments.add(new Assignment(result, new Call(functionName, List.of(leftReference, rightReference)), AllMask.ALL));
        return new FilterSpec(new EvaluationPlan(assignments, List.of()), new ReferenceMask(new Reference(result, Stream.VALUES)));
    }

    private static List<Assignment> remap(List<Assignment> assignments, int variableOffset)
    {
        return assignments.stream()
                .map(assignment -> new Assignment(
                        new Variable(assignment.output().id() + variableOffset),
                        remap(assignment.operation(), variableOffset),
                        assignment.mask()))
                .toList();
    }

    private static org.weakref.nitro.operator.evaluator.ir.Operation remap(org.weakref.nitro.operator.evaluator.ir.Operation operation, int variableOffset)
    {
        return switch (operation) {
            case Literal literal -> literal;
            case Call(String functionName, List<Reference> arguments) -> new Call(functionName, arguments.stream()
                    .map(argument -> remap(argument, variableOffset))
                    .toList());
            default -> throw new IllegalArgumentException("Unsupported operation in filter spec: " + operation);
        };
    }

    private static Reference remap(Reference reference, int variableOffset)
    {
        return switch (reference.producer()) {
            case Input ignored -> reference;
            case Variable variable -> new Reference(new Variable(variable.id() + variableOffset), reference.stream());
            default -> reference;
        };
    }

    private static int maxVariableId(List<Assignment> assignments)
    {
        return assignments.stream()
                .mapToInt(assignment -> assignment.output().id())
                .max()
                .orElse(-1);
    }
}
