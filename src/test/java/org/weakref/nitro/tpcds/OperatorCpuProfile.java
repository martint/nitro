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

import org.weakref.nitro.data.Mask;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Output;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static java.lang.String.format;

public final class OperatorCpuProfile
{
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();
    private static final boolean CPU_TIME_SUPPORTED = THREAD_MX_BEAN.isCurrentThreadCpuTimeSupported();
    private static final com.sun.management.ThreadMXBean ALLOCATION_MX_BEAN =
            ManagementFactory.getPlatformMXBean(com.sun.management.ThreadMXBean.class);
    private static final boolean ALLOCATION_SUPPORTED =
            ALLOCATION_MX_BEAN != null && ALLOCATION_MX_BEAN.isThreadAllocatedMemorySupported();

    static {
        if (CPU_TIME_SUPPORTED && !THREAD_MX_BEAN.isThreadCpuTimeEnabled()) {
            THREAD_MX_BEAN.setThreadCpuTimeEnabled(true);
        }
        if (ALLOCATION_SUPPORTED && !ALLOCATION_MX_BEAN.isThreadAllocatedMemoryEnabled()) {
            ALLOCATION_MX_BEAN.setThreadAllocatedMemoryEnabled(true);
        }
    }

    private final Map<String, Metric> metrics = new LinkedHashMap<>();
    private final int highWaterBatches;

    public OperatorCpuProfile()
    {
        this(Integer.getInteger("nitro.operatorCpuProfile.highWaterBatches", 0));
    }

    OperatorCpuProfile(int highWaterBatches)
    {
        if (highWaterBatches < 0) {
            throw new IllegalArgumentException("highWaterBatches is negative");
        }
        this.highWaterBatches = highWaterBatches;
    }

    public Operator wrap(String name, Operator delegate)
    {
        Metric metric = metrics.computeIfAbsent(name, ignored -> new Metric(name, delegate.getClass().getSimpleName(), highWaterBatches));
        return new Operator()
        {
            @Override
            public int outputCount()
            {
                return delegate.outputCount();
            }

            @Override
            public boolean hasNext()
            {
                metric.markSteadyBoundary();
                return time(metric.hasNextCount, metric.hasNextNanos, metric.hasNextAllocatedBytes, delegate::hasNext);
            }

            @Override
            public Batch next()
            {
                Batch batch = time(metric.nextCount, metric.nextNanos, metric.nextAllocatedBytes, delegate::next);
                Mask mask = time(metric.borrowMaskCount, metric.borrowMaskNanos, metric.borrowMaskAllocatedBytes, batch::borrowMask);
                metric.rowsProduced += mask.count();
                metric.batchCount++;

                Output[] outputs = new Output[delegate.outputCount()];
                for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
                    outputs[outputIndex] = wrapOutput(metric, batch.output(outputIndex));
                }

                return new Batch(
                        mask,
                        constrainedMask -> time(metric.constrainCount, metric.constrainNanos, metric.constrainAllocatedBytes, () -> {
                            batch.constrain(constrainedMask);
                            return null;
                        }),
                        takenMask -> time(metric.takeMaskCount, metric.takeMaskNanos, metric.takeMaskAllocatedBytes, batch::takeMask),
                        _ -> {},
                        () -> time(metric.batchCloseCount, metric.batchCloseNanos, metric.batchCloseAllocatedBytes, () -> {
                            batch.close();
                            return null;
                        }),
                        outputs);
            }

            @Override
            public void constrain(Mask mask)
            {
                time(metric.constrainCount, metric.constrainNanos, metric.constrainAllocatedBytes, () -> {
                    delegate.constrain(mask);
                    return null;
                });
            }

            @Override
            public void pushDynamicFilter(org.weakref.nitro.operator.DynamicFilter filter)
            {
                delegate.pushDynamicFilter(filter);
            }

            @Override
            public boolean supportsRetainedBatches()
            {
                return delegate.supportsRetainedBatches();
            }

            @Override
            public boolean supportsStableBatchBorrow()
            {
                return delegate.supportsStableBatchBorrow();
            }

            @Override
            public long exactOutputRows()
            {
                return delegate.exactOutputRows();
            }

            @Override
            public boolean supportsConstrainedReborrow()
            {
                return delegate.supportsConstrainedReborrow();
            }

            @Override
            public void close()
            {
                time(metric.closeCount, metric.closeNanos, metric.closeAllocatedBytes, () -> {
                    delegate.close();
                    return null;
                });
            }
        };
    }

    public String formatReport()
    {
        List<Metric> sorted = new ArrayList<>(metrics.values());
        sorted.sort(Comparator.comparingLong(Metric::totalNanos).reversed());
        long totalNanos = sorted.stream()
                .mapToLong(Metric::totalNanos)
                .sum();

        StringBuilder builder = new StringBuilder();
        builder.append("Operator CPU profile (thread CPU time)\n");
        builder.append(format("%-34s %-24s %10s %8s %12s %14s %14s %8s %12s%n", "Operator", "Type", "cpu_ms", "share", "alloc_mb", "alloc_kb/batch", "steady_kb/batch", "batches", "rows"));
        for (Metric metric : sorted) {
            double cpuMillis = metric.totalNanos() / 1_000_000.0;
            double share = totalNanos == 0 ? 0 : (100.0 * metric.totalNanos() / totalNanos);
            builder.append(format(
                    "%-34s %-24s %10.3f %7.2f%% %12.3f %14.3f %14.3f %8d %12d%n",
                    metric.name,
                    metric.operatorType,
                    cpuMillis,
                    share,
                    metric.totalAllocatedBytes() / (1024.0 * 1024.0),
                    metric.batchCount == 0 ? 0 : metric.totalAllocatedBytes() / 1024.0 / metric.batchCount,
                    metric.steadyBatchCount() == 0 ? 0 : metric.steadyAllocatedBytes() / 1024.0 / metric.steadyBatchCount(),
                    metric.batchCount,
                    metric.rowsProduced));
            builder.append(format(
                    "  next=%7.3f  borrow=%7.3f  take=%7.3f  constrain=%7.3f  close=%7.3f" +
                            "  alloc[next=%8.3f borrow=%8.3f take=%8.3f constrain=%8.3f close=%8.3f] MB%n",
                    metric.nextNanos[0] / 1_000_000.0,
                    metric.outputBorrowNanos[0] / 1_000_000.0,
                    metric.outputTakeNanos[0] / 1_000_000.0,
                    metric.constrainNanos[0] / 1_000_000.0,
                    (metric.batchCloseNanos[0] + metric.closeNanos[0]) / 1_000_000.0,
                    metric.nextAllocatedBytes[0] / (1024.0 * 1024.0),
                    metric.outputBorrowAllocatedBytes[0] / (1024.0 * 1024.0),
                    metric.outputTakeAllocatedBytes[0] / (1024.0 * 1024.0),
                    metric.constrainAllocatedBytes[0] / (1024.0 * 1024.0),
                    (metric.batchCloseAllocatedBytes[0] + metric.closeAllocatedBytes[0]) / (1024.0 * 1024.0)));
        }
        builder.append(format("Total attributed cpu_ms: %.3f%n", totalNanos / 1_000_000.0));
        return builder.toString();
    }

    private Output wrapOutput(Metric metric, Output delegate)
    {
        Set<Stream> streams = delegate.streams();
        return new Output(
                streams,
                stream -> time(metric.outputBorrowCount, metric.outputBorrowNanos, metric.outputBorrowAllocatedBytes, () -> delegate.borrow(stream)),
                (stream, vector) -> time(metric.outputTakeCount, metric.outputTakeNanos, metric.outputTakeAllocatedBytes, () -> delegate.take(stream)),
                (_, _) -> {},
                (existing, sourcePosition, outputPosition, size) -> time(metric.copySinglePositionCount, metric.copySinglePositionNanos, metric.copySinglePositionAllocatedBytes, () -> delegate.copySinglePosition(existing, sourcePosition, outputPosition, size)));
    }

    private static long now()
    {
        return CPU_TIME_SUPPORTED ? THREAD_MX_BEAN.getCurrentThreadCpuTime() : System.nanoTime();
    }

    private static long allocatedBytes()
    {
        return ALLOCATION_SUPPORTED ? ALLOCATION_MX_BEAN.getCurrentThreadAllocatedBytes() : 0;
    }

    private static <T> T time(long[] count, long[] nanos, long[] allocated, Supplier<T> supplier)
    {
        long start = now();
        long allocatedStart = allocatedBytes();
        try {
            return supplier.get();
        }
        finally {
            allocated[0] += allocatedBytes() - allocatedStart;
            nanos[0] += now() - start;
            count[0]++;
        }
    }

    private static final class Metric
    {
        private final String name;
        private final String operatorType;
        private final int highWaterBatches;
        private final long[] hasNextCount = new long[1];
        private final long[] hasNextNanos = new long[1];
        private final long[] hasNextAllocatedBytes = new long[1];
        private final long[] nextCount = new long[1];
        private final long[] nextNanos = new long[1];
        private final long[] nextAllocatedBytes = new long[1];
        private final long[] constrainCount = new long[1];
        private final long[] constrainNanos = new long[1];
        private final long[] constrainAllocatedBytes = new long[1];
        private final long[] closeCount = new long[1];
        private final long[] closeNanos = new long[1];
        private final long[] closeAllocatedBytes = new long[1];
        private final long[] borrowMaskCount = new long[1];
        private final long[] borrowMaskNanos = new long[1];
        private final long[] borrowMaskAllocatedBytes = new long[1];
        private final long[] takeMaskCount = new long[1];
        private final long[] takeMaskNanos = new long[1];
        private final long[] takeMaskAllocatedBytes = new long[1];
        private final long[] batchCloseCount = new long[1];
        private final long[] batchCloseNanos = new long[1];
        private final long[] batchCloseAllocatedBytes = new long[1];
        private final long[] outputBorrowCount = new long[1];
        private final long[] outputBorrowNanos = new long[1];
        private final long[] outputBorrowAllocatedBytes = new long[1];
        private final long[] outputTakeCount = new long[1];
        private final long[] outputTakeNanos = new long[1];
        private final long[] outputTakeAllocatedBytes = new long[1];
        private final long[] copySinglePositionCount = new long[1];
        private final long[] copySinglePositionNanos = new long[1];
        private final long[] copySinglePositionAllocatedBytes = new long[1];
        private long batchCount;
        private long rowsProduced;
        private boolean steadyBoundaryMarked;
        private long steadyBoundaryAllocatedBytes;
        private long steadyBoundaryBatchCount;

        private Metric(String name, String operatorType, int highWaterBatches)
        {
            this.name = name;
            this.operatorType = operatorType;
            this.highWaterBatches = highWaterBatches;
        }

        private void markSteadyBoundary()
        {
            if (!steadyBoundaryMarked && batchCount >= highWaterBatches) {
                steadyBoundaryAllocatedBytes = totalAllocatedBytes();
                steadyBoundaryBatchCount = batchCount;
                steadyBoundaryMarked = true;
            }
        }

        private long steadyAllocatedBytes()
        {
            return steadyBoundaryMarked ? totalAllocatedBytes() - steadyBoundaryAllocatedBytes : 0;
        }

        private long steadyBatchCount()
        {
            return steadyBoundaryMarked ? batchCount - steadyBoundaryBatchCount : 0;
        }

        public long totalNanos()
        {
            return hasNextNanos[0] +
                    nextNanos[0] +
                    constrainNanos[0] +
                    closeNanos[0] +
                    borrowMaskNanos[0] +
                    takeMaskNanos[0] +
                    batchCloseNanos[0] +
                    outputBorrowNanos[0] +
                    outputTakeNanos[0] +
                    copySinglePositionNanos[0];
        }

        public long totalAllocatedBytes()
        {
            return hasNextAllocatedBytes[0] +
                    nextAllocatedBytes[0] +
                    constrainAllocatedBytes[0] +
                    closeAllocatedBytes[0] +
                    borrowMaskAllocatedBytes[0] +
                    takeMaskAllocatedBytes[0] +
                    batchCloseAllocatedBytes[0] +
                    outputBorrowAllocatedBytes[0] +
                    outputTakeAllocatedBytes[0] +
                    copySinglePositionAllocatedBytes[0];
        }
    }
}
