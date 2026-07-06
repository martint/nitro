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

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

/**
 * PROTOTYPE: off-loads the two-key join probe to a native (Rust) batch probe that emits explicit software prefetch
 * — the cache-miss-latency hiding the JVM cannot express. Gated by {@code -Dnitro.nativeProbe}. Uses FFM
 * {@link Linker.Option#critical} with heap access so Nitro's on-heap tags/entries arrays are passed by reference
 * (no off-heap copy); the call is short (one bounded batch) so pinning the arrays is safe.
 */
final class NativeProbe
{
    static final boolean ENABLED;
    static final int DISTANCE = Integer.getInteger("nitro.nativeProbe.distance", 16);
    private static final MethodHandle PROBE_PAIRS;
    private static final MethodHandle PROBE_TRIPLES;

    static {
        MethodHandle pairs = null;
        MethodHandle triples = null;
        boolean enabled = false;
        if (Boolean.getBoolean("nitro.nativeProbe")) {
            try {
                String path = System.getProperty("nitro.native.lib", "tools/nitro-native/target/release/libnitro_native.so");
                SymbolLookup lib = SymbolLookup.libraryLookup(Path.of(path), Arena.global());
                Linker linker = Linker.nativeLinker();
                // probe_pairs(tags*, capacity, entries*, first*, second*, n, out*, prefetch_distance)
                pairs = linker.downcallHandle(lib.find("probe_pairs").orElseThrow(),
                        FunctionDescriptor.ofVoid(ADDRESS, JAVA_LONG, ADDRESS, ADDRESS, ADDRESS, JAVA_LONG, ADDRESS, JAVA_LONG),
                        Linker.Option.critical(true));
                // probe_triples(tags*, capacity, entries*, first*, second*, third*, n, out*, prefetch_distance)
                triples = linker.downcallHandle(lib.find("probe_triples").orElseThrow(),
                        FunctionDescriptor.ofVoid(ADDRESS, JAVA_LONG, ADDRESS, ADDRESS, ADDRESS, ADDRESS, JAVA_LONG, ADDRESS, JAVA_LONG),
                        Linker.Option.critical(true));
                enabled = true;
            }
            catch (Throwable t) {
                System.err.println("NativeProbe disabled: " + t);
            }
        }
        PROBE_PAIRS = pairs;
        PROBE_TRIPLES = triples;
        ENABLED = enabled;
    }

    private NativeProbe() {}

    static void probePairs(byte[] tags, long[] entries, long[] first, long[] second, int n, long[] refs, int distance)
    {
        try {
            PROBE_PAIRS.invoke(
                    MemorySegment.ofArray(tags), (long) tags.length,
                    MemorySegment.ofArray(entries),
                    MemorySegment.ofArray(first),
                    MemorySegment.ofArray(second),
                    (long) n,
                    MemorySegment.ofArray(refs),
                    (long) distance);
        }
        catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    static void probeTriples(byte[] tags, long[] entries, long[] first, long[] second, long[] third, int n, long[] refs, int distance)
    {
        try {
            PROBE_TRIPLES.invoke(
                    MemorySegment.ofArray(tags), (long) tags.length,
                    MemorySegment.ofArray(entries),
                    MemorySegment.ofArray(first),
                    MemorySegment.ofArray(second),
                    MemorySegment.ofArray(third),
                    (long) n,
                    MemorySegment.ofArray(refs),
                    (long) distance);
        }
        catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }
}
