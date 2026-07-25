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
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.Operator;

import java.lang.management.ManagementFactory;
import java.util.function.Supplier;

import static java.lang.String.format;

/**
 * Measures allocation at the query lifecycle boundaries without wrapping operators or allocating per batch.
 * Construction, initial high-water batches, later steady-state batches, terminal polling, and close are reported
 * separately. The consumption shape is identical to {@link BenchmarkQueries}: borrow the mask and every VALUES
 * stream, then close the batch.
 */
public final class SteadyStateAllocationProfile
{
    private static final com.sun.management.ThreadMXBean ALLOCATION_MX_BEAN =
            ManagementFactory.getPlatformMXBean(com.sun.management.ThreadMXBean.class);
    private static final boolean SUPPORTED =
            ALLOCATION_MX_BEAN != null && ALLOCATION_MX_BEAN.isThreadAllocatedMemorySupported();

    static {
        if (SUPPORTED && !ALLOCATION_MX_BEAN.isThreadAllocatedMemoryEnabled()) {
            ALLOCATION_MX_BEAN.setThreadAllocatedMemoryEnabled(true);
        }
    }

    private SteadyStateAllocationProfile() {}

    public static Report measure(Supplier<Operator> factory, int highWaterBatches)
    {
        if (highWaterBatches < 0) {
            throw new IllegalArgumentException("highWaterBatches is negative");
        }

        long constructionStart = allocatedBytes();
        Operator operator = factory.get();
        long constructionBytes = allocatedBytes() - constructionStart;

        int outputCount = operator.outputCount();
        long highWaterPullBytes = 0;
        long highWaterCloseBytes = 0;
        long steadyPullBytes = 0;
        long steadyCloseBytes = 0;
        long terminalPollBytes = 0;
        long closeBytes;
        long highWaterRows = 0;
        long steadyRows = 0;
        long sink = 0;
        int batches = 0;
        boolean closed = false;
        try {
            while (true) {
                long pullStart = allocatedBytes();
                if (!operator.hasNext()) {
                    terminalPollBytes += allocatedBytes() - pullStart;
                    break;
                }

                Batch batch = operator.next();
                Mask mask = batch.borrowMask();
                int rows = mask.count();
                sink += rows;
                for (int column = 0; column < outputCount; column++) {
                    Vector values = batch.output(column).borrow(Stream.VALUES);
                    sink += values.length();
                }
                long pullBytes = allocatedBytes() - pullStart;

                long closeStart = allocatedBytes();
                batch.close();
                long batchCloseBytes = allocatedBytes() - closeStart;

                if (batches < highWaterBatches) {
                    highWaterPullBytes += pullBytes;
                    highWaterCloseBytes += batchCloseBytes;
                    highWaterRows += rows;
                }
                else {
                    steadyPullBytes += pullBytes;
                    steadyCloseBytes += batchCloseBytes;
                    steadyRows += rows;
                }
                batches++;
            }
        }
        finally {
            long closeStart = allocatedBytes();
            operator.close();
            closeBytes = allocatedBytes() - closeStart;
            closed = true;
        }

        int highWaterBatchCount = Math.min(batches, highWaterBatches);
        int steadyBatchCount = batches - highWaterBatchCount;
        return new Report(
                constructionBytes,
                highWaterPullBytes,
                highWaterCloseBytes,
                steadyPullBytes,
                steadyCloseBytes,
                terminalPollBytes,
                closeBytes,
                highWaterBatchCount,
                steadyBatchCount,
                highWaterRows,
                steadyRows,
                sink,
                closed);
    }

    private static long allocatedBytes()
    {
        return SUPPORTED ? ALLOCATION_MX_BEAN.getCurrentThreadAllocatedBytes() : 0;
    }

    public record Report(
            long constructionBytes,
            long highWaterPullBytes,
            long highWaterCloseBytes,
            long steadyPullBytes,
            long steadyCloseBytes,
            long terminalPollBytes,
            long closeBytes,
            int highWaterBatches,
            int steadyBatches,
            long highWaterRows,
            long steadyRows,
            long sink,
            boolean closed)
    {
        long highWaterBytes()
        {
            return highWaterPullBytes + highWaterCloseBytes;
        }

        long steadyBytes()
        {
            return steadyPullBytes + steadyCloseBytes;
        }

        long executionBytes()
        {
            return highWaterBytes() + steadyBytes() + terminalPollBytes;
        }

        public String formatReport()
        {
            return format(
                    "construction=%d B, high-water=%d B (%d batches, %.1f B/batch, %.3f B/row), " +
                            "steady=%d B (%d batches, %.1f B/batch, %.3f B/row), terminal-poll=%d B, " +
                            "close=%d B, execution=%d B, sink=%d",
                    constructionBytes,
                    highWaterBytes(),
                    highWaterBatches,
                    perBatch(highWaterBytes(), highWaterBatches),
                    perRow(highWaterBytes(), highWaterRows),
                    steadyBytes(),
                    steadyBatches,
                    perBatch(steadyBytes(), steadyBatches),
                    perRow(steadyBytes(), steadyRows),
                    terminalPollBytes,
                    closeBytes,
                    executionBytes(),
                    sink);
        }

        private static double perBatch(long bytes, int batches)
        {
            return batches == 0 ? 0 : (double) bytes / batches;
        }

        private static double perRow(long bytes, long rows)
        {
            return rows == 0 ? 0 : (double) bytes / rows;
        }
    }
}
