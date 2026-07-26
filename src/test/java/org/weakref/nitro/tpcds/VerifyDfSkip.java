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

import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.parquet.ColumnReader;
import org.weakref.nitro.parquet.ParquetFile;
import org.weakref.nitro.parquet.ParquetMaterializationPolicy;
import org.weakref.nitro.parquet.ParquetPageNavigationPolicy;
import org.weakref.nitro.parquet.ParquetReaderDiagnostics;
import org.weakref.nitro.parquet.RleReaderPolicy;

import java.nio.file.Path;
import java.util.List;

/**
 * Mimics {@link org.weakref.nitro.operator.source.compatibility.parquet.NitroParquetScanOperator}'s
 * dynamic-filter flow exactly: a lead
 * filter column is full-decoded over a large window to derive a CLUSTERED survivor set (like a real date
 * filter over date-sorted inventory), then a payload column is read both by skip-decode (at the survivors) and
 * by full decode, and the two must agree. This reproduces the multi-reader + real-clustered-survivor scenario
 * that {@link VerifySkip}'s synthetic single-reader patterns do not cover.
 */
public final class VerifyDfSkip
{
    private VerifyDfSkip() {}

    public static void main(String[] args)
    {
        EngineResources engineResources = EngineResources.createDefault();
        PrimitiveArrayPool arrayPool = engineResources.primitiveArrays();
        String table = "inventory";
        String filterColumn = "inv_date_sk";
        String payloadColumn = args.length > 0 ? args[0] : "inv_item_sk";
        int window = Integer.getInteger("window", 1 << 20);

        TpcdsParquetTables tables = TpcdsParquetTables.requiredActual("sf10");
        List<Path> files = tables.tableFiles(table);
        long totalRows = 0;
        for (Path p : files) {
            try (ParquetFile f = ParquetFile.open(p)) {
                totalRows += f.numRows();
            }
        }

        ParquetFile.Column filterMeta;
        try (ParquetFile f = ParquetFile.open(files.get(0))) {
            filterMeta = f.column(filterColumn);
        }
        boolean filterIsInt = filterMeta.type() == org.apache.parquet.format.Type.INT32 && !filterMeta.decimal();

        // First pass: global min/max of the filter column to pick a ~20% clustered range.
        ColumnReader probe = reader(files, filterColumn, arrayPool);
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        int[] buf = new int[window];
        long[] bufL = new long[window];
        long seen = 0;
        while (seen < totalRows) {
            int count = (int) Math.min(window, totalRows - seen);
            if (filterIsInt) {
                probe.readInts(buf, null, count);
            }
            else {
                probe.readLongs(bufL, null, count);
            }
            for (int i = 0; i < count; i++) {
                long v = filterIsInt ? buf[i] : bufL[i];
                min = Math.min(min, v);
                max = Math.max(max, v);
            }
            seen += count;
        }
        probe.close();
        long lo = min;
        long hi = min + (max - min) / 5;   // first 20% of the date domain (clustered when date-sorted)
        System.out.println("filter " + filterColumn + " range [" + lo + "," + hi + "] of [" + min + "," + max + "]");

        ColumnReader date = reader(files, filterColumn, arrayPool);
        ColumnReader skip = reader(files, payloadColumn, arrayPool);
        ColumnReader full = reader(files, payloadColumn, arrayPool);
        ParquetFile.Column meta;
        try (ParquetFile f = ParquetFile.open(files.get(0))) {
            meta = f.column(payloadColumn);
        }
        boolean isInt = meta.type() == org.apache.parquet.format.Type.INT32 && !meta.decimal();

        int[] dateBuf = new int[window];
        long[] dateBufL = new long[window];
        int[] survivors = new int[window];
        int[] skipI = new int[window];
        int[] fullI = new int[window];
        long[] skipL = new long[window];
        long[] fullL = new long[window];
        boolean[] skipN = new boolean[window];
        boolean[] fullN = new boolean[window];

        long row = 0;
        long checked = 0;
        while (row < totalRows) {
            int count = (int) Math.min(window, totalRows - row);
            if (filterIsInt) {
                date.readInts(dateBuf, null, count);
            }
            else {
                date.readLongs(dateBufL, null, count);
            }
            int sc = 0;
            for (int i = 0; i < count; i++) {
                long v = filterIsInt ? dateBuf[i] : dateBufL[i];
                if (v >= lo && v <= hi) {
                    survivors[sc++] = i;
                }
            }
            if (isInt) {
                skip.readSelectedInts(survivors, sc, count, skipI, skipN);
                full.readInts(fullI, fullN, count);
                for (int k = 0; k < sc; k++) {
                    int pos = survivors[k];
                    if (skipI[k] != fullI[pos] || skipN[k] != fullN[pos]) {
                        throw new AssertionError(payloadColumn + " mismatch at window row " + (row + pos)
                                + " (survivor " + k + "/" + sc + "): skip=" + skipI[k] + "/" + skipN[k]
                                + " full=" + fullI[pos] + "/" + fullN[pos]);
                    }
                }
            }
            else {
                skip.readSelectedLongs(survivors, sc, count, skipL, skipN);
                full.readLongs(fullL, fullN, count);
                for (int k = 0; k < sc; k++) {
                    int pos = survivors[k];
                    if (skipL[k] != fullL[pos] || skipN[k] != fullN[pos]) {
                        throw new AssertionError(payloadColumn + " mismatch at window row " + (row + pos)
                                + ": skip=" + skipL[k] + " full=" + fullL[pos]);
                    }
                }
            }
            checked += sc;
            row += count;
        }
        date.close();
        skip.close();
        full.close();
        System.out.println("OK df-skip==full for " + payloadColumn + " (" + checked + " survivors checked, window=" + window + ")");
    }

    private static ColumnReader reader(List<Path> files, String column, PrimitiveArrayPool arrayPool)
    {
        ColumnReader reader = null;
        for (Path p : files) {
            ParquetFile file = ParquetFile.open(p);
            ParquetFile.Column col = file.column(column);
            if (reader == null) {
                reader = new ColumnReader(
                        col.type(),
                        col.optional(),
                        col.typeLength(),
                        col.decimal(),
                        null,
                        arrayPool,
                        RleReaderPolicy.defaults(),
                        ParquetPageNavigationPolicy.defaults(),
                        ParquetReaderDiagnostics.disabled(),
                        ParquetMaterializationPolicy.defaults());
            }
            for (var rowGroup : file.rowGroups()) {
                reader.addChunk(file.data(), file.columnChunk(rowGroup, col).meta_data, rowGroup.num_rows);
            }
        }
        return reader;
    }
}
