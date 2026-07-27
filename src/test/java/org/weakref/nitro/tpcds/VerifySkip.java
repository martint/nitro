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

import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.parquet.ColumnReader;
import org.weakref.nitro.parquet.ParquetFile;
import org.weakref.nitro.parquet.ParquetReaderPolicy;

import java.nio.file.Path;
import java.util.List;

/**
 * Proves {@link ColumnReader#readSelectedLongs}/{@code readSelectedInts} (skip-decode) returns exactly the
 * values a full decode produces at the selected positions. Two independent readers over the same column run in
 * lockstep batch by batch: one full-decodes the batch, the other skip-decodes a varied survivor subset; their
 * values and null flags must agree. Exits non-zero on the first mismatch.
 */
public final class VerifySkip
{
    private static int batchSize = 8192;
    private static int pattern;
    private static int batchIndex;

    private VerifySkip() {}

    public static void main(String[] args)
    {
        EngineResources engineResources = EngineResources.createDefault();
        PrimitiveArrayPool arrayPool = engineResources.primitiveArrays();
        String table = args[0];
        TpcdsParquetTables tables = TpcdsParquetTables.requiredActual("sf10");
        List<Path> files = tables.tableFiles(table);
        // Sweep batch sizes (forcing page splits) x survivor patterns (dense / sparse / clustered).
        int[] batches = {8192, 10000, 1000, 1234, 777, 1 << 20, 1 << 19, 300000};
        for (int b : batches) {
            for (int pat = 0; pat < 10; pat++) {
                batchSize = b;
                pattern = pat;
                for (int c = 1; c < args.length; c++) {
                    verify(files, args[c], arrayPool);
                }
            }
        }
    }

    private static boolean selected(int i)
    {
        return switch (pattern) {
            case 0 -> ((i * 0x9E3779B1) >>> 28 & 7) == 0;        // ~1/8 dense, varied gaps
            case 1 -> (i * 0x85EBCA6B & 0x1FF) == 0;             // ~1/512 sparse
            case 2 -> (i & 0x3FF) < 5 || (i & 0x3FF) > 0x3FA;    // clustered runs near 1024-boundaries
            case 3 -> (i & 1) == 0;                              // 50%, runs of 1
            case 4 -> (i % 100) < 80;                            // 80%, long runs of 80
            case 5 -> true;                                       // every position (longest runs)
            case 6 -> i < batchSize / 10;                        // front-clustered: survivors only in first 10%
            case 7 -> i > batchSize - batchSize / 10;            // back-clustered: survivors only in last 10%
            // batch-VARYING (mimics a clustered filter: whole windows empty, then full, then partial) -- this is
            // what a real date-clustered DF produces and is NOT covered by the uniform-per-batch patterns above.
            case 8 -> (batchIndex & 1) == 0;                     // alternate: whole batch in, whole batch out
            default -> (batchIndex % 3 == 0) ? i < batchSize / 4 // every 3rd batch: front quarter; else empty
                    : (batchIndex % 3 == 1) && i > batchSize * 3 / 4;
        };
    }

    private static void verify(List<Path> files, String column, PrimitiveArrayPool arrayPool)
    {
        int capacity = batchSize;
        ColumnReader full = reader(files, column, arrayPool);
        ColumnReader skip = reader(files, column, arrayPool);
        long totalRows = 0;
        ParquetFile.Column meta;
        try (ParquetFile f = ParquetFile.open(files.get(0))) {
            meta = f.column(column);
        }
        for (Path p : files) {
            try (ParquetFile f = ParquetFile.open(p)) {
                totalRows += f.numRows();
            }
        }
        boolean isInt = meta.type() == org.apache.parquet.format.Type.INT32 && !meta.decimal();
        long[] fullL = new long[capacity];
        int[] fullI = new int[capacity];
        boolean[] fullN = new boolean[capacity];
        long[] selL = new long[capacity];
        int[] selI = new int[capacity];
        boolean[] selN = new boolean[capacity];
        int[] survivors = new int[capacity];

        long row = 0;
        long checked = 0;
        batchIndex = 0;
        while (row < totalRows) {
            int batch = (int) Math.min(capacity, totalRows - row);
            int count = 0;
            batchIndex++;
            for (int i = 0; i < batch; i++) {
                if (selected(i)) {
                    survivors[count++] = i;
                }
            }
            if (isInt) {
                full.readInts(fullI, fullN, batch);
                skip.readSelectedInts(survivors, count, batch, selI, selN);
                for (int k = 0; k < count; k++) {
                    int pos = survivors[k];
                    if (selI[k] != fullI[pos] || selN[k] != fullN[pos]) {
                        throw new AssertionError(column + " int mismatch row " + (row + pos) + ": skip=" + selI[k] + "/" + selN[k] + " full=" + fullI[pos] + "/" + fullN[pos]);
                    }
                }
            }
            else {
                full.readLongs(fullL, fullN, batch);
                skip.readSelectedLongs(survivors, count, batch, selL, selN);
                for (int k = 0; k < count; k++) {
                    int pos = survivors[k];
                    if (selL[k] != fullL[pos] || selN[k] != fullN[pos]) {
                        throw new AssertionError(column + " long mismatch row " + (row + pos) + ": skip=" + selL[k] + "/" + selN[k] + " full=" + fullL[pos] + "/" + fullN[pos]);
                    }
                }
            }
            checked += count;
            row += batch;
        }
        full.close();
        skip.close();
        System.out.println("OK skip==full for " + column + " (" + totalRows + " rows, " + checked + " survivors checked)");
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
                        ParquetReaderPolicy.defaults());
            }
            for (var rowGroup : file.rowGroups()) {
                reader.addChunk(file.data(), file.columnChunk(rowGroup, col).meta_data, rowGroup.num_rows);
            }
        }
        return reader;
    }
}
