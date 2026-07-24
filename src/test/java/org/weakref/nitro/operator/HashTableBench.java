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
package org.weakref.nitro.operator;

import io.airlift.slice.Slices;
import io.trino.operator.FlatHashStrategyCompiler;
import io.trino.operator.GroupByHash;
import io.trino.operator.UpdateMemory;
import io.trino.operator.Work;
import io.trino.spi.Page;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Utf8Traits;
import org.weakref.nitro.data.Vector;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.SplittableRandom;

/**
 * Head-to-head, in-isolation benchmark of the two grouping hash tables (no scan/decode/aggregate) over the
 * SAME realistic key data: Nitro's {@link FlatGroupingTable} vs Trino's {@link GroupByHash}. Drives both in
 * batches, persisting one table across the whole input (as real grouping does). Answers: is the hash table
 * itself the q13/q15/q67 gap?
 */
public final class HashTableBench
{
    private HashTableBench() {}

    private static final int BATCH = 8192;

    public static void main(String[] args)
    {
        PrimitiveArrayPool arrayPool = EngineResources.createDefault().primitiveArrays();
        int rows = args.length > 0 ? Integer.parseInt(args[0]) : 10_000_000;
        int distinct = args.length > 1 ? Integer.parseInt(args[1]) : 2_000_000;
        // Build a pool of distinct realistic strings (SearchPhrase-like: 8-28 UTF-8 bytes), then sample.
        SplittableRandom random = new SplittableRandom(42);
        byte[][] pool = new byte[distinct][];
        for (int i = 0; i < distinct; i++) {
            int len = 8 + random.nextInt(20);
            StringBuilder s = new StringBuilder(len);
            for (int c = 0; c < len; c++) {
                s.append((char) ('a' + random.nextInt(26)));
            }
            pool[i] = s.toString().getBytes(StandardCharsets.UTF_8);
        }
        byte[][] keys = new byte[rows][];
        for (int i = 0; i < rows; i++) {
            keys[i] = pool[random.nextInt(distinct)];
        }
        System.out.printf("rows=%d distinct=%d batch=%d%n", rows, distinct, BATCH);

        // Pre-build the per-batch columnar inputs ONCE so the timed loop is pure grouping (no block/vector build).
        Vector[][] nitroBatches = buildNitroBatches(keys);
        Page[] trinoPages = buildTrinoPages(keys);

        for (int iter = 0; iter < 6; iter++) {
            long t0 = System.nanoTime();
            int ng = runNitro(nitroBatches, distinct, arrayPool);
            long t1 = System.nanoTime();
            int bg = runNitroBulk(nitroBatches, distinct, arrayPool);
            long t2 = System.nanoTime();
            int tg = runTrino(trinoPages, distinct);
            long t3 = System.nanoTime();
            System.out.printf("iter%d  NITRO %5d ms (g=%d)   NITRO-BULK %5d ms (g=%d)   TRINO %5d ms (g=%d)%n",
                    iter, (t1 - t0) / 1_000_000, ng, (t2 - t1) / 1_000_000, bg, (t3 - t2) / 1_000_000, tg);
        }
    }

    private static int runNitroBulk(Vector[][] batches, int expectedDistinct, PrimitiveArrayPool arrayPool)
    {
        FlatGroupingTable table = null;
        long nextGroupId = 0;
        Vector[] nulls = new Vector[] {null};
        for (Vector[] values : batches) {
            int count = values[0].length();
            if (table == null) {
                FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, false, arrayPool);
                table = new FlatGroupingTable(layout, Math.max(16, expectedDistinct));
            }
            Mask mask = Mask.all(count);
            table.beginBatch(values, nulls);
            table.prepareBatchHashes(values, nulls, mask);
            for (int position : mask) {
                long group = table.assignGroup(values, position, nextGroupId);
                if (group == nextGroupId) {
                    nextGroupId++;
                }
            }
            table.endBatch();
        }
        return (int) nextGroupId;
    }

    private static Vector[][] buildNitroBatches(byte[][] keys)
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("input");
        int rows = keys.length;
        int batches = (rows + BATCH - 1) / BATCH;
        Vector[][] result = new Vector[batches][];
        int b = 0;
        for (int start = 0; start < rows; start += BATCH, b++) {
            int count = Math.min(BATCH, rows - start);
            int bytes = 0;
            for (int i = 0; i < count; i++) {
                bytes += keys[start + i].length;
            }
            BinaryVector vector = BinaryVector.allocate(allocator, context, count, bytes);
            vector.addTrait(Utf8Traits.UTF8_STRING);
            vector.addTrait(Utf8Traits.ASCII_ONLY);
            for (int i = 0; i < count; i++) {
                vector.setBytes(i, keys[start + i]);
            }
            result[b] = new Vector[] {vector};
        }
        return result;
    }

    private static Page[] buildTrinoPages(byte[][] keys)
    {
        int rows = keys.length;
        int batches = (rows + BATCH - 1) / BATCH;
        Page[] result = new Page[batches];
        int b = 0;
        for (int start = 0; start < rows; start += BATCH, b++) {
            int count = Math.min(BATCH, rows - start);
            VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, count, count * 16);
            for (int i = 0; i < count; i++) {
                builder.writeEntry(Slices.wrappedBuffer(keys[start + i]));
            }
            result[b] = new Page(builder.build());
        }
        return result;
    }

    private static int runNitro(Vector[][] batches, int expectedDistinct, PrimitiveArrayPool arrayPool)
    {
        FlatGroupingTable table = null;
        long nextGroupId = 0;
        Vector[] nulls = new Vector[] {null};
        for (Vector[] values : batches) {
            int count = values[0].length();
            if (table == null) {
                FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, false, arrayPool);
                table = new FlatGroupingTable(layout, Math.max(16, expectedDistinct));
            }
            table.beginBatch(values, nulls);
            for (int i = 0; i < count; i++) {
                long group = table.assignGroup(values, i, nextGroupId);
                if (group == nextGroupId) {
                    nextGroupId++;
                }
            }
            table.endBatch();
        }
        return (int) nextGroupId;
    }

    private static int runTrino(Page[] pages, int expectedDistinct)
    {
        TypeOperators typeOperators = new TypeOperators();
        FlatHashStrategyCompiler compiler = new FlatHashStrategyCompiler(typeOperators);
        List<Type> types = List.of(VarcharType.VARCHAR);
        GroupByHash hash = GroupByHash.createGroupByHash(types, false, Math.max(16, expectedDistinct), false, compiler, UpdateMemory.NOOP);
        long sink = 0;
        for (Page page : pages) {
            Work<int[]> work = hash.getGroupIds(page);
            while (!work.process()) {
                // spin until done
            }
            int[] ids = work.getResult();
            sink += ids[ids.length - 1];
        }
        if (sink == Long.MIN_VALUE) {
            System.out.print("");
        }
        return hash.getGroupCount();
    }
}
