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

import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Vector;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.random.RandomGenerator;
import java.util.random.RandomGeneratorFactory;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

/**
 * Isolation microbench for q24's grouping-insert shape (2 long keys + 3 short string keys): Java
 * {@link FlatGroupingTable} vs the native (Rust) grouper. Answers, before any engine wiring, whether the
 * native grouping insert is faster and produces the same distinct-group count.
 */
public final class GroupBench
{
    private static final int BATCH = 8192;
    private static final int STRINGS = 3;

    private GroupBench() {}

    public static void main(String[] args)
            throws Throwable
    {
        EngineResources engineResources = EngineResources.createDefault();
        PrimitiveArrayPool arrayPool = engineResources.primitiveArrays();
        OperatorCodeGenerationResources codeGeneration = engineResources.operatorCodeGeneration();
        int rows = args.length > 0 ? Integer.parseInt(args[0]) : 10_000_000;
        int distinct = args.length > 1 ? Integer.parseInt(args[1]) : 2_000_000;

        RandomGenerator rng = RandomGeneratorFactory.of("Xoshiro256PlusPlus").create(7);
        long[] k0 = new long[distinct];
        long[] k1 = new long[distinct];
        byte[][][] s = new byte[STRINGS][distinct][];
        for (int i = 0; i < distinct; i++) {
            k0[i] = rng.nextLong();
            k1[i] = rng.nextInt(1_000_000);
            for (int c = 0; c < STRINGS; c++) {
                int len = 5 + rng.nextInt(11);
                StringBuilder sb = new StringBuilder(len);
                for (int j = 0; j < len; j++) {
                    sb.append((char) ('a' + rng.nextInt(26)));
                }
                s[c][i] = sb.toString().getBytes(StandardCharsets.UTF_8);
            }
        }
        int[] sample = new int[rows];
        for (int i = 0; i < rows; i++) {
            sample[i] = rng.nextInt(distinct);
        }
        System.out.printf("rows=%d distinct=%d batch=%d%n", rows, distinct, BATCH);

        // Pre-build columnar batches once.
        int batches = (rows + BATCH - 1) / BATCH;
        Vector[][] nitro = new Vector[batches][];
        for (int b = 0; b < batches; b++) {
            int start = b * BATCH;
            int n = Math.min(BATCH, rows - start);
            long[] c0 = new long[n];
            long[] c1 = new long[n];
            int[][] off = new int[STRINGS][n + 1];
            byte[][] data = new byte[STRINGS][];
            int[] total = new int[STRINGS];
            for (int c = 0; c < STRINGS; c++) {
                for (int i = 0; i < n; i++) {
                    total[c] += s[c][sample[start + i]].length;
                }
                data[c] = new byte[total[c]];
            }
            int[] pos = new int[STRINGS];
            for (int i = 0; i < n; i++) {
                int r = sample[start + i];
                c0[i] = k0[r];
                c1[i] = k1[r];
                for (int c = 0; c < STRINGS; c++) {
                    byte[] v = s[c][r];
                    System.arraycopy(v, 0, data[c], pos[c], v.length);
                    off[c][i] = pos[c];
                    pos[c] += v.length;
                    off[c][i + 1] = pos[c];
                }
            }
            nitro[b] = new Vector[] {
                    new I64Vector(c0), new I64Vector(c1),
                    new BinaryVector(n, off[0], data[0]),
                    new BinaryVector(n, off[1], data[1]),
                    new BinaryVector(n, off[2], data[2])};
        }

        Native.load();
        for (int iter = 0; iter < 6; iter++) {
            long t0 = System.nanoTime();
            long jg = runJava(nitro, distinct, arrayPool, codeGeneration);
            long t1 = System.nanoTime();
            long ng = runNative(nitro, distinct, 0);
            long t2 = System.nanoTime();
            long ngg = runNative(nitro, distinct, 0, true);
            long t3 = System.nanoTime();
            System.out.printf("iter%d  JAVA %5d ms (g=%d)   NATIVE-hard %5d ms (g=%d)   NATIVE-generic %5d ms (g=%d)  %s%n",
                    iter, (t1 - t0) / 1_000_000, jg, (t2 - t1) / 1_000_000, ng, (t3 - t2) / 1_000_000, ngg,
                    (jg == ng && ng == ngg) ? "OK" : "MISMATCH!");
        }
    }

    private static long runJava(
            Vector[][] batches,
            int expectedDistinct,
            PrimitiveArrayPool arrayPool,
            OperatorCodeGenerationResources codeGeneration)
    {
        Vector[] nulls = new Vector[] {null, null, null, null, null};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(batches[0], false, arrayPool, codeGeneration, FlatKeyTablePolicy.defaults());
        FlatGroupingTable table = new FlatGroupingTable(layout, expectedDistinct);
        long nextGroupId = 0;
        for (Vector[] values : batches) {
            int n = values[0].length();
            Mask mask = Mask.all(n);
            table.beginBatch(values, nulls);
            table.prepareBatchHashes(values, nulls, mask);
            for (int position : mask) {
                long newGroupId = nextGroupId;
                long groupId = table.assignGroup(values, nulls, position, newGroupId);
                if (groupId == newGroupId) {
                    nextGroupId++;
                }
            }
            table.endBatch();
        }
        return nextGroupId;
    }

    private static long runNative(Vector[][] batches, int expectedDistinct, int prefetch)
            throws Throwable
    {
        return runNative(batches, expectedDistinct, prefetch, false);
    }

    private static long runNative(Vector[][] batches, int expectedDistinct, int prefetch, boolean generic)
            throws Throwable
    {
        long grouper = generic ? Native.newGenericGrouper(expectedDistinct) : Native.newGrouper(expectedDistinct);
        try {
            for (Vector[] values : batches) {
                int n = values[0].length();
                long[] c0 = ((I64Vector) values[0]).values();
                long[] c1 = ((I64Vector) values[1]).values();
                BinaryVector b0 = (BinaryVector) values[2];
                BinaryVector b1 = (BinaryVector) values[3];
                BinaryVector b2 = (BinaryVector) values[4];
                long[] off0 = toLong(b0.offsets(), n);
                long[] off1 = toLong(b1.offsets(), n);
                long[] off2 = toLong(b2.offsets(), n);
                int[] len0 = lengths(b0.offsets(), n);
                int[] len1 = lengths(b1.offsets(), n);
                int[] len2 = lengths(b2.offsets(), n);
                int[] out = new int[n];
                Native.insert(grouper, c0, c1,
                        b0.data(), off0, len0, b1.data(), off1, len1, b2.data(), off2, len2, n, out, prefetch, generic);
            }
            return generic ? Native.gCount(grouper) : Native.count(grouper);
        }
        finally {
            if (generic) {
                Native.gFree(grouper);
            }
            else {
                Native.free(grouper);
            }
        }
    }

    private static long[] toLong(int[] offsets, int n)
    {
        long[] r = new long[n];
        for (int i = 0; i < n; i++) {
            r[i] = offsets[i];
        }
        return r;
    }

    private static int[] lengths(int[] offsets, int n)
    {
        int[] r = new int[n];
        for (int i = 0; i < n; i++) {
            r[i] = offsets[i + 1] - offsets[i];
        }
        return r;
    }

    private static final class Native
    {
        private static MethodHandle newHandle;
        private static MethodHandle freeHandle;
        private static MethodHandle countHandle;
        private static MethodHandle insertHandle;
        private static MethodHandle gNewHandle;
        private static MethodHandle gFreeHandle;
        private static MethodHandle gCountHandle;
        private static MethodHandle gInsertHandle;

        static void load()
        {
            Linker linker = Linker.nativeLinker();
            var lib = java.lang.foreign.SymbolLookup.libraryLookup(
                    Path.of("tools/nitro-native/target/release/libnitro_native.so"), Arena.global());
            newHandle = linker.downcallHandle(lib.find("grouper_new").orElseThrow(),
                    FunctionDescriptor.of(JAVA_LONG, JAVA_LONG));
            freeHandle = linker.downcallHandle(lib.find("grouper_free").orElseThrow(),
                    FunctionDescriptor.ofVoid(JAVA_LONG));
            countHandle = linker.downcallHandle(lib.find("grouper_count").orElseThrow(),
                    FunctionDescriptor.of(JAVA_LONG, JAVA_LONG));
            insertHandle = linker.downcallHandle(lib.find("grouper_insert").orElseThrow(),
                    FunctionDescriptor.ofVoid(JAVA_LONG, ADDRESS, ADDRESS,
                            ADDRESS, ADDRESS, ADDRESS, ADDRESS, ADDRESS, ADDRESS, ADDRESS, ADDRESS, ADDRESS,
                            JAVA_LONG, ADDRESS, JAVA_LONG),
                    Linker.Option.critical(true));
            gNewHandle = linker.downcallHandle(lib.find("grouper_g_new").orElseThrow(), FunctionDescriptor.of(JAVA_LONG, JAVA_LONG));
            gFreeHandle = linker.downcallHandle(lib.find("grouper_g_free").orElseThrow(), FunctionDescriptor.ofVoid(JAVA_LONG));
            gCountHandle = linker.downcallHandle(lib.find("grouper_g_count").orElseThrow(), FunctionDescriptor.of(JAVA_LONG, JAVA_LONG));
            gInsertHandle = linker.downcallHandle(lib.find("grouper_g_insert").orElseThrow(),
                    FunctionDescriptor.ofVoid(JAVA_LONG, ADDRESS, ADDRESS, ADDRESS, ADDRESS, ADDRESS, ADDRESS, ADDRESS, ADDRESS, ADDRESS, ADDRESS, ADDRESS, JAVA_LONG, ADDRESS, JAVA_LONG),
                    Linker.Option.critical(true));
        }

        static long newGenericGrouper(int expected)
                throws Throwable
        {
            return (long) gNewHandle.invoke((long) expected);
        }

        static long gCount(long g)
                throws Throwable
        {
            return (long) gCountHandle.invoke(g);
        }

        static void gFree(long g)
                throws Throwable
        {
            gFreeHandle.invoke(g);
        }

        static long newGrouper(int expected)
                throws Throwable
        {
            return (long) newHandle.invoke((long) expected);
        }

        static long count(long g)
                throws Throwable
        {
            return (long) countHandle.invoke(g);
        }

        static void free(long g)
                throws Throwable
        {
            freeHandle.invoke(g);
        }

        static void insert(long g, long[] k0, long[] k1,
                byte[] d0, long[] o0, int[] l0, byte[] d1, long[] o1, int[] l1, byte[] d2, long[] o2, int[] l2,
                int n, int[] out, int prefetch, boolean generic)
                throws Throwable
        {
            (generic ? gInsertHandle : insertHandle).invoke(g,
                    MemorySegment.ofArray(k0), MemorySegment.ofArray(k1),
                    MemorySegment.ofArray(d0), MemorySegment.ofArray(o0), MemorySegment.ofArray(l0),
                    MemorySegment.ofArray(d1), MemorySegment.ofArray(o1), MemorySegment.ofArray(l1),
                    MemorySegment.ofArray(d2), MemorySegment.ofArray(o2), MemorySegment.ofArray(l2),
                    (long) n, MemorySegment.ofArray(out), (long) prefetch);
        }
    }
}
