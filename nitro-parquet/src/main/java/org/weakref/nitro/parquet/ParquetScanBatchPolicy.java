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
package org.weakref.nitro.parquet;

/**
 * Immutable output-batch sizing for a native Parquet scan.
 *
 * @param adaptiveMaximumVectorCells maximum rows multiplied by materializable value/null vectors after adaptive
 * growth; the initial row count remains the lower bound
 */
public record ParquetScanBatchPolicy(
        int initialRows,
        int maxRows,
        long adaptiveObservationRows,
        AdaptiveGrowth adaptiveGrowth,
        double adaptiveSelectedFractionThreshold,
        long adaptiveMaximumVectorCells)
{
    public enum AdaptiveGrowth
    {
        SPARSE,
        DENSE;

        boolean admits(double selectedFraction, double threshold)
        {
            return switch (this) {
                case SPARSE -> selectedFraction <= threshold;
                case DENSE -> selectedFraction >= threshold;
            };
        }
    }

    public ParquetScanBatchPolicy
    {
        if (initialRows <= 0) {
            throw new IllegalArgumentException("initialRows must be positive");
        }
        if (maxRows < initialRows) {
            throw new IllegalArgumentException("maxRows is less than initialRows");
        }
        if (adaptiveObservationRows < 0) {
            throw new IllegalArgumentException("adaptiveObservationRows is negative");
        }
        if (adaptiveGrowth == null) {
            throw new IllegalArgumentException("adaptiveGrowth is null");
        }
        if (adaptiveSelectedFractionThreshold < 0 || adaptiveSelectedFractionThreshold > 1) {
            throw new IllegalArgumentException("adaptiveSelectedFractionThreshold is outside [0, 1]");
        }
        if (adaptiveMaximumVectorCells <= 0) {
            throw new IllegalArgumentException("adaptiveMaximumVectorCells must be positive");
        }
    }

    public ParquetScanBatchPolicy(int maxRows)
    {
        this(maxRows, maxRows, 0, AdaptiveGrowth.SPARSE, 0, Long.MAX_VALUE);
    }

    public ParquetScanBatchPolicy(
            int initialRows,
            int maxRows,
            long adaptiveObservationRows,
            double adaptiveMaximumSelectedFraction)
    {
        this(initialRows, maxRows, adaptiveObservationRows, AdaptiveGrowth.SPARSE, adaptiveMaximumSelectedFraction, Long.MAX_VALUE);
    }

    public ParquetScanBatchPolicy(
            int initialRows,
            int maxRows,
            long adaptiveObservationRows,
            double adaptiveMaximumSelectedFraction,
            long adaptiveMaximumVectorCells)
    {
        this(initialRows, maxRows, adaptiveObservationRows, AdaptiveGrowth.SPARSE, adaptiveMaximumSelectedFraction, adaptiveMaximumVectorCells);
    }

    public static ParquetScanBatchPolicy defaults()
    {
        return new ParquetScanBatchPolicy(10_000);
    }

    /**
     * Starts with the cache-qualified native batch size and grows only after a downstream pipeline has
     * demonstrated that the host boundary would otherwise receive very sparse batches. Growth is additionally
     * bounded by projected vector width so a wide sparse scan cannot trade fewer boundary batches for an aggregate
     * multi-gigabyte live vector set.
     */
    public static ParquetScanBatchPolicy adaptiveHostBoundaryDefaults()
    {
        return new ParquetScanBatchPolicy(10_000, 40_000, 20_000, AdaptiveGrowth.SPARSE, 0.25, 1_000_000);
    }

    /**
     * Grows dense batches that remain inside a Nitro pipeline while retaining smaller encoded-domain-friendly
     * batches as soon as downstream evaluation narrows the selection.
     */
    public static ParquetScanBatchPolicy adaptiveInternalPipelineDefaults()
    {
        return new ParquetScanBatchPolicy(10_000, 40_000, 20_000, AdaptiveGrowth.DENSE, 1.0, 1_000_000);
    }

    public static ParquetScanBatchPolicy fromSystemProperties()
    {
        return new ParquetScanBatchPolicy(
                Integer.getInteger("nitro.parquet.scan.maxBatchRows", 10_000));
    }

    boolean adaptive()
    {
        return initialRows < maxRows && adaptiveObservationRows > 0;
    }

    int adaptiveRows(int vectorColumns)
    {
        if (vectorColumns <= 0) {
            return maxRows;
        }
        long cellBoundedRows = adaptiveMaximumVectorCells / vectorColumns;
        return (int) Math.max(initialRows, Math.min(maxRows, cellBoundedRows));
    }
}
