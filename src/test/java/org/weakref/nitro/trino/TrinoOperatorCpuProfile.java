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
package org.weakref.nitro.trino;

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.spi.Page;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.lang.String.format;

public final class TrinoOperatorCpuProfile
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

    public Operator wrap(String name, Operator delegate)
    {
        Metric metric = metrics.computeIfAbsent(name, ignored -> new Metric(name, delegate.getClass().getSimpleName()));
        return new Operator()
        {
            @Override
            public OperatorContext getOperatorContext()
            {
                return delegate.getOperatorContext();
            }

            @Override
            public ListenableFuture<Void> isBlocked()
            {
                return time(metric.isBlockedCount, metric.isBlockedNanos, metric.isBlockedAllocatedBytes, delegate::isBlocked);
            }

            @Override
            public boolean needsInput()
            {
                return time(metric.needsInputCount, metric.needsInputNanos, metric.needsInputAllocatedBytes, delegate::needsInput);
            }

            @Override
            public void addInput(Page page)
            {
                time(metric.addInputCount, metric.addInputNanos, metric.addInputAllocatedBytes, () -> {
                    delegate.addInput(page);
                    return null;
                });
                metric.inputPages++;
                metric.inputRows += page.getPositionCount();
            }

            @Override
            public Page getOutput()
            {
                Page page = time(metric.getOutputCount, metric.getOutputNanos, metric.getOutputAllocatedBytes, delegate::getOutput);
                if (page != null) {
                    metric.outputPages++;
                    metric.outputRows += page.getPositionCount();
                }
                return page;
            }

            @Override
            public ListenableFuture<Void> startMemoryRevoke()
            {
                return time(metric.startMemoryRevokeCount, metric.startMemoryRevokeNanos, metric.startMemoryRevokeAllocatedBytes, delegate::startMemoryRevoke);
            }

            @Override
            public void finishMemoryRevoke()
            {
                time(metric.finishMemoryRevokeCount, metric.finishMemoryRevokeNanos, metric.finishMemoryRevokeAllocatedBytes, () -> {
                    delegate.finishMemoryRevoke();
                    return null;
                });
            }

            @Override
            public void finish()
            {
                time(metric.finishCount, metric.finishNanos, metric.finishAllocatedBytes, () -> {
                    delegate.finish();
                    return null;
                });
            }

            @Override
            public boolean isFinished()
            {
                return time(metric.isFinishedCount, metric.isFinishedNanos, metric.isFinishedAllocatedBytes, delegate::isFinished);
            }

            @Override
            public void close()
                    throws Exception
            {
                long start = now();
                long allocatedStart = allocatedBytes();
                try {
                    delegate.close();
                }
                finally {
                    metric.closeNanos[0] += now() - start;
                    metric.closeAllocatedBytes[0] += allocatedBytes() - allocatedStart;
                    metric.closeCount[0]++;
                }
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
        builder.append("Trino operator CPU profile (thread CPU time)\n");
        builder.append(format("%-34s %-28s %10s %8s %12s %8s %12s %8s %12s%n", "Operator", "Type", "cpu_ms", "share", "alloc_mb", "in_pg", "in_rows", "out_pg", "out_rows"));
        for (Metric metric : sorted) {
            double cpuMillis = metric.totalNanos() / 1_000_000.0;
            double share = totalNanos == 0 ? 0 : (100.0 * metric.totalNanos() / totalNanos);
            builder.append(format(
                    "%-34s %-28s %10.3f %7.2f%% %12.3f %8d %12d %8d %12d%n",
                    metric.name,
                    metric.operatorType,
                    cpuMillis,
                    share,
                    metric.totalAllocatedBytes() / (1024.0 * 1024.0),
                    metric.inputPages,
                    metric.inputRows,
                    metric.outputPages,
                    metric.outputRows));
            builder.append(format(
                    "  output=%7.3f  input=%7.3f  needs=%7.3f  blocked=%7.3f  finish=%7.3f  close=%7.3f%n",
                    metric.getOutputNanos[0] / 1_000_000.0,
                    metric.addInputNanos[0] / 1_000_000.0,
                    metric.needsInputNanos[0] / 1_000_000.0,
                    metric.isBlockedNanos[0] / 1_000_000.0,
                    (metric.finishNanos[0] + metric.isFinishedNanos[0]) / 1_000_000.0,
                    metric.closeNanos[0] / 1_000_000.0));
        }
        builder.append(format("Total attributed cpu_ms: %.3f%n", totalNanos / 1_000_000.0));
        return builder.toString();
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
        private final long[] isBlockedCount = new long[1];
        private final long[] isBlockedNanos = new long[1];
        private final long[] isBlockedAllocatedBytes = new long[1];
        private final long[] needsInputCount = new long[1];
        private final long[] needsInputNanos = new long[1];
        private final long[] needsInputAllocatedBytes = new long[1];
        private final long[] addInputCount = new long[1];
        private final long[] addInputNanos = new long[1];
        private final long[] addInputAllocatedBytes = new long[1];
        private final long[] getOutputCount = new long[1];
        private final long[] getOutputNanos = new long[1];
        private final long[] getOutputAllocatedBytes = new long[1];
        private final long[] startMemoryRevokeCount = new long[1];
        private final long[] startMemoryRevokeNanos = new long[1];
        private final long[] startMemoryRevokeAllocatedBytes = new long[1];
        private final long[] finishMemoryRevokeCount = new long[1];
        private final long[] finishMemoryRevokeNanos = new long[1];
        private final long[] finishMemoryRevokeAllocatedBytes = new long[1];
        private final long[] finishCount = new long[1];
        private final long[] finishNanos = new long[1];
        private final long[] finishAllocatedBytes = new long[1];
        private final long[] isFinishedCount = new long[1];
        private final long[] isFinishedNanos = new long[1];
        private final long[] isFinishedAllocatedBytes = new long[1];
        private final long[] closeCount = new long[1];
        private final long[] closeNanos = new long[1];
        private final long[] closeAllocatedBytes = new long[1];
        private long inputPages;
        private long inputRows;
        private long outputPages;
        private long outputRows;

        private Metric(String name, String operatorType)
        {
            this.name = name;
            this.operatorType = operatorType;
        }

        public long totalNanos()
        {
            return isBlockedNanos[0] +
                    needsInputNanos[0] +
                    addInputNanos[0] +
                    getOutputNanos[0] +
                    startMemoryRevokeNanos[0] +
                    finishMemoryRevokeNanos[0] +
                    finishNanos[0] +
                    isFinishedNanos[0] +
                    closeNanos[0];
        }

        public long totalAllocatedBytes()
        {
            return isBlockedAllocatedBytes[0] +
                    needsInputAllocatedBytes[0] +
                    addInputAllocatedBytes[0] +
                    getOutputAllocatedBytes[0] +
                    startMemoryRevokeAllocatedBytes[0] +
                    finishMemoryRevokeAllocatedBytes[0] +
                    finishAllocatedBytes[0] +
                    isFinishedAllocatedBytes[0] +
                    closeAllocatedBytes[0];
        }
    }
}
