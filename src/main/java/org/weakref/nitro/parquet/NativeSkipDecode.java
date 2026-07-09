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

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

/**
 * PROTOTYPE: off-loads ColumnReader.readSelectedLongs' per-page dict skip-decode to a native (Rust) kernel to test
 * whether q20's scan gap is the JVM-scalar-reader vs native-columnar-reader constant factor. Gated by
 * {@code -Dnitro.nativeSkipDecode}. Zero-copy on-heap arrays via {@link Linker.Option#critical}.
 */
public final class NativeSkipDecode
{
    public static final boolean ENABLED;
    private static final MethodHandle SKIP_LONGS;

    static {
        MethodHandle skip = null;
        boolean enabled = false;
        if (Boolean.getBoolean("nitro.nativeSkipDecode")) {
            try {
                String path = System.getProperty("nitro.native.lib", "tools/nitro-native/target/release/libnitro_native.so");
                SymbolLookup lib = SymbolLookup.libraryLookup(Path.of(path), Arena.global());
                Linker linker = Linker.nativeLinker();
                // skip_decode_dict_longs(body*, value_offset, value_bit_width, def_offset, survivors*, survivor_count, dict*, out*, out_offset, nulls_out*)
                skip = linker.downcallHandle(lib.find("skip_decode_dict_longs").orElseThrow(),
                        FunctionDescriptor.ofVoid(JAVA_LONG, JAVA_LONG, JAVA_INT, JAVA_LONG, ADDRESS, JAVA_LONG, ADDRESS, ADDRESS, JAVA_LONG, ADDRESS),
                        Linker.Option.critical(true));
                enabled = true;
            }
            catch (Throwable t) {
                System.err.println("NativeSkipDecode disabled: " + t);
            }
        }
        SKIP_LONGS = skip;
        ENABLED = enabled;
    }

    private NativeSkipDecode() {}

    /**
     * Gather dict[id] (or 0 for a null) at each of the sorted, page-relative {@code survivors} into out[outOffset+i].
     * {@code defOffset < 0} means the page is null-free (value index == position); otherwise the def-level stream at
     * {@code defOffset} (bit width 1) maps positions to value indices.
     */
    public static void skipDecodeDictLongs(MemorySegment body, long valueOffset, int valueBitWidth, long defOffset,
            int[] survivors, int survivorCount, long[] dict, long[] out, int outOffset, byte[] nullsOut)
    {
        try {
            SKIP_LONGS.invoke(body.address(), valueOffset, valueBitWidth, defOffset,
                    MemorySegment.ofArray(survivors), (long) survivorCount,
                    MemorySegment.ofArray(dict), MemorySegment.ofArray(out), (long) outOffset,
                    nullsOut == null ? MemorySegment.NULL : MemorySegment.ofArray(nullsOut));
        }
        catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }
}
