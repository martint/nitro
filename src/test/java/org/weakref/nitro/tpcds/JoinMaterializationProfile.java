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

import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.HashJoinOperator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

public final class JoinMaterializationProfile
        implements HashJoinOperator.MaterializationProfile
{
    private final Map<String, Metric> metrics = new LinkedHashMap<>();

    @Override
    public void record(String operatorName, int outputIndex, Streams streams, int rowCount, long nanos)
    {
        String valuesType = streamType(streams, Stream.VALUES);
        String nullsType = streamType(streams, Stream.NULLS);
        String errorsType = streamType(streams, Stream.ERRORS);
        String key = operatorName + "#" + outputIndex + "|" + valuesType + "|" + nullsType + "|" + errorsType;
        Metric metric = metrics.computeIfAbsent(key, ignored -> new Metric(operatorName, outputIndex, valuesType, nullsType, errorsType));
        metric.calls++;
        metric.rows += rowCount;
        metric.nanos += nanos;
        long nullTrueCount = countTrue(streams.getOrNull(Stream.NULLS), rowCount);
        long errorTrueCount = countTrue(streams.getOrNull(Stream.ERRORS), rowCount);
        metric.nullTrueCount += nullTrueCount;
        metric.errorTrueCount += errorTrueCount;
        if (streams.hasNulls() && nullTrueCount == 0) {
            metric.nullFalseOnlyCalls++;
            metric.nullFalseOnlyRows += rowCount;
        }
        if (streams.hasErrors() && errorTrueCount == 0) {
            metric.errorFalseOnlyCalls++;
            metric.errorFalseOnlyRows += rowCount;
        }
    }

    public String formatReport()
    {
        List<Metric> sorted = new ArrayList<>(metrics.values());
        sorted.sort(Comparator.comparingLong(Metric::nanos).reversed());
        long totalNanos = sorted.stream()
                .mapToLong(Metric::nanos)
                .sum();

        StringBuilder builder = new StringBuilder();
        builder.append("HashJoin materialization profile\n");
        builder.append(format("%-42s %6s %10s %8s %8s %12s %12s %12s %12s %12s %s%n", "Join output", "slot", "cpu_ms", "share", "calls", "rows", "null_true", "error_true", "null_false", "error_false", "streams"));
        for (Metric metric : sorted) {
            builder.append(format(
                    "%-42s %6d %10.3f %7.2f%% %8d %12d %12d %12d %12s %12s values=%s nulls=%s errors=%s%n",
                    metric.operatorName(),
                    metric.outputIndex(),
                    metric.nanos() / 1_000_000.0,
                    totalNanos == 0 ? 0.0 : (100.0 * metric.nanos() / totalNanos),
                    metric.calls(),
                    metric.rows(),
                    metric.nullTrueCount(),
                    metric.errorTrueCount(),
                    format("%d/%d", metric.nullFalseOnlyCalls(), metric.nullFalseOnlyRows()),
                    format("%d/%d", metric.errorFalseOnlyCalls(), metric.errorFalseOnlyRows()),
                    metric.valuesType(),
                    metric.nullsType(),
                    metric.errorsType()));
        }
        builder.append(format("Total attributed cpu_ms: %.3f%n", totalNanos / 1_000_000.0));
        return builder.toString();
    }

    private static String streamType(Streams streams, Stream stream)
    {
        return streams.has(stream) ? describeVector(streams.get(stream)) : "-";
    }

    private static String describeVector(Vector vector)
    {
        return switch (vector) {
            case DictionaryVector dictionary -> "DictionaryVector<" + describeVector(dictionary.values()) + ">";
            case RleVector rle -> "RleVector<" + describeVector(rle.values()) + ">";
            default -> vector.getClass().getSimpleName();
        };
    }

    private static long countTrue(Vector vector, int rowCount)
    {
        if (vector == null || rowCount == 0) {
            return 0;
        }
        return switch (vector) {
            case BooleanVector values -> countTrue(values.values(), rowCount);
            case DictionaryVector values -> {
                int[] ids = values.ids();
                Vector dictionaryValues = values.values();
                long count = 0;
                for (int position = 0; position < rowCount; position++) {
                    count += countTrueAt(dictionaryValues, ids[position]) ? 1 : 0;
                }
                yield count;
            }
            case RleVector values -> {
                Vector runValues = values.values();
                long count = 0;
                for (int position = 0; position < rowCount; position++) {
                    count += countTrueAt(runValues, values.runIndex(position)) ? 1 : 0;
                }
                yield count;
            }
            default -> 0;
        };
    }

    private static boolean countTrueAt(Vector vector, int position)
    {
        return switch (vector) {
            case BooleanVector values -> values.values().length == 0 || position >= values.values().length ? false : values.values()[position];
            case DictionaryVector values -> values.length() == 0 || position >= values.length() ? false : countTrueAt(values.values(), values.ids()[position]);
            case RleVector values -> position >= values.length() ? false : countTrueAt(values.values(), values.runIndex(position));
            default -> false;
        };
    }

    private static long countTrue(boolean[] values, int rowCount)
    {
        long count = 0;
        int countLimit = Math.min(values.length, rowCount);
        for (int position = 0; position < countLimit; position++) {
            if (values[position]) {
                count++;
            }
        }
        return count;
    }

    private static final class Metric
    {
        private final String operatorName;
        private final int outputIndex;
        private final String valuesType;
        private final String nullsType;
        private final String errorsType;
        private long calls;
        private long rows;
        private long nanos;
        private long nullTrueCount;
        private long errorTrueCount;
        private long nullFalseOnlyCalls;
        private long nullFalseOnlyRows;
        private long errorFalseOnlyCalls;
        private long errorFalseOnlyRows;

        private Metric(String operatorName, int outputIndex, String valuesType, String nullsType, String errorsType)
        {
            this.operatorName = operatorName;
            this.outputIndex = outputIndex;
            this.valuesType = valuesType;
            this.nullsType = nullsType;
            this.errorsType = errorsType;
        }

        public String operatorName()
        {
            return operatorName;
        }

        public int outputIndex()
        {
            return outputIndex;
        }

        public String valuesType()
        {
            return valuesType;
        }

        public String nullsType()
        {
            return nullsType;
        }

        public String errorsType()
        {
            return errorsType;
        }

        public long calls()
        {
            return calls;
        }

        public long rows()
        {
            return rows;
        }

        public long nanos()
        {
            return nanos;
        }

        public long nullTrueCount()
        {
            return nullTrueCount;
        }

        public long errorTrueCount()
        {
            return errorTrueCount;
        }

        public long nullFalseOnlyCalls()
        {
            return nullFalseOnlyCalls;
        }

        public long nullFalseOnlyRows()
        {
            return nullFalseOnlyRows;
        }

        public long errorFalseOnlyCalls()
        {
            return errorFalseOnlyCalls;
        }

        public long errorFalseOnlyRows()
        {
            return errorFalseOnlyRows;
        }
    }
}
