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
package org.weakref.nitro.operator.aggregation;

import java.util.ArrayList;
import java.util.List;

/**
 * Static planner that rewrites an accumulator list to fuse cooperating accumulator pairs that can
 * share a single input scan per batch.
 *
 * <p>Operators that drive a list of {@link Accumulator}s should call {@link #fuse(List)} once at
 * construction time and use the returned list as their canonical accumulator sequence. The returned
 * list has the same length as the input and preserves the original positions; each entry is either
 * the original accumulator or a cooperating replacement that honors the same {@link Accumulator}
 * contract.
 *
 * <p>The operator stays generic: it iterates the list and calls {@link Accumulator#accumulate} per
 * entry without any knowledge of which entries were fused. The operation-specific recognition
 * (e.g. pairing {@link Min} with {@link Max} on the same input column) lives here, in the
 * aggregation package where operation-specific code belongs.
 */
public final class AccumulatorFusion
{
    private static final boolean FUSED_SUM_AVG_F64 =
            Boolean.parseBoolean(System.getProperty("nitro.aggregate.fusedSumAvgF64", "true"));

    private AccumulatorFusion() {}

    /**
     * Returns a list of the same length as {@code accumulators} where recognized pairs have been
     * replaced by cooperating accumulators that share input scans. Unmatched entries are returned
     * unchanged.
     */
    public static List<Accumulator> fuse(List<Accumulator> accumulators)
    {
        if (accumulators.size() < 2) {
            return accumulators;
        }

        List<Accumulator> result = new ArrayList<>(accumulators);
        boolean[] paired = new boolean[accumulators.size()];
        for (int firstIndex = 0; firstIndex < accumulators.size(); firstIndex++) {
            if (paired[firstIndex] || !(accumulators.get(firstIndex) instanceof ConditionalSum first)) {
                continue;
            }
            List<Integer> compatible = new ArrayList<>();
            compatible.add(firstIndex);
            for (int candidate = firstIndex + 1; candidate < accumulators.size(); candidate++) {
                if (!paired[candidate] && accumulators.get(candidate) instanceof ConditionalSum conditional &&
                        conditional.discriminatorColumn() == first.discriminatorColumn() &&
                        conditional.valueColumn() == first.valueColumn() &&
                        compatible.stream().noneMatch(index -> sameLiteral(((ConditionalSum) accumulators.get(index)).literal(), conditional.literal()))) {
                    compatible.add(candidate);
                }
            }
            if (compatible.size() > 1) {
                FusedConditionalSums.fuse(result, compatible);
                for (int index : compatible) {
                    paired[index] = true;
                }
            }
        }
        for (int firstIndex = 0; firstIndex < accumulators.size(); firstIndex++) {
            if (paired[firstIndex]) {
                continue;
            }
            if (FUSED_SUM_AVG_F64) {
                for (int secondIndex = firstIndex + 1; secondIndex < accumulators.size(); secondIndex++) {
                    if (!paired[secondIndex] && tryFuseSumAvgF64(result, firstIndex, secondIndex)) {
                        paired[firstIndex] = true;
                        paired[secondIndex] = true;
                        break;
                    }
                }
            }
            if (paired[firstIndex]) {
                continue;
            }
            for (int secondIndex = firstIndex + 1; secondIndex < accumulators.size(); secondIndex++) {
                if (paired[secondIndex]) {
                    continue;
                }
                if (tryFuseMinMax(result, firstIndex, secondIndex)) {
                    paired[firstIndex] = true;
                    paired[secondIndex] = true;
                    break;
                }
            }
        }
        return result;
    }

    private static boolean tryFuseSumAvgF64(List<Accumulator> accumulators, int firstIndex, int secondIndex)
    {
        Accumulator first = accumulators.get(firstIndex);
        Accumulator second = accumulators.get(secondIndex);
        if (first instanceof SumF64 sum && second instanceof AvgF64 avg && sum.inputColumn() == avg.inputColumn()) {
            FusedSumAvgF64.SharedState handle = new FusedSumAvgF64.SharedState();
            accumulators.set(firstIndex, FusedSumAvgF64.scanner(sum, sum.inputColumn(), handle, FusedSumAvgF64.Kind.SUM));
            accumulators.set(secondIndex, FusedSumAvgF64.follower(avg, handle, FusedSumAvgF64.Kind.AVG));
            return true;
        }
        if (first instanceof AvgF64 avg && second instanceof SumF64 sum && sum.inputColumn() == avg.inputColumn()) {
            FusedSumAvgF64.SharedState handle = new FusedSumAvgF64.SharedState();
            accumulators.set(firstIndex, FusedSumAvgF64.scanner(avg, avg.inputColumn(), handle, FusedSumAvgF64.Kind.AVG));
            accumulators.set(secondIndex, FusedSumAvgF64.follower(sum, handle, FusedSumAvgF64.Kind.SUM));
            return true;
        }
        return false;
    }

    private static boolean sameLiteral(ConditionalSum.Literal left, ConditionalSum.Literal right)
    {
        return switch (left) {
            case ConditionalSum.LongLiteral value when right instanceof ConditionalSum.LongLiteral other -> value.value() == other.value();
            case ConditionalSum.BinaryLiteral value when right instanceof ConditionalSum.BinaryLiteral other -> java.util.Arrays.equals(value.value(), other.value());
            default -> false;
        };
    }

    private static boolean tryFuseMinMax(List<Accumulator> accumulators, int firstIndex, int secondIndex)
    {
        Accumulator first = accumulators.get(firstIndex);
        Accumulator second = accumulators.get(secondIndex);

        // Match Min + Max on the same input column in either order. The accumulator whose result
        // the operator will read from {first, second} keeps its output slot; its accumulate call is
        // the one that performs the shared scan.
        if (first instanceof Min min && second instanceof Max max && min.inputColumn() == max.inputColumn()) {
            FusedMinMaxI64.SharedState handle = new FusedMinMaxI64.SharedState();
            accumulators.set(firstIndex, FusedMinMaxI64.scanner(min.inputColumn(), handle, FusedMinMaxI64.Kind.MIN));
            accumulators.set(secondIndex, FusedMinMaxI64.follower(max.inputColumn(), handle));
            return true;
        }
        if (first instanceof Max max && second instanceof Min min && min.inputColumn() == max.inputColumn()) {
            FusedMinMaxI64.SharedState handle = new FusedMinMaxI64.SharedState();
            accumulators.set(firstIndex, FusedMinMaxI64.scanner(max.inputColumn(), handle, FusedMinMaxI64.Kind.MAX));
            accumulators.set(secondIndex, FusedMinMaxI64.follower(min.inputColumn(), handle));
            return true;
        }
        return false;
    }
}
