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
package org.weakref.nitro.data;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

/** Best-effort operating-system advice for large recyclable native workspaces. */
public final class NativeBufferAdvice
{
    private static final int PAGE_BYTES = 4096;
    private static final int MADV_HUGEPAGE = 14;
    private static final int MADV_COLLAPSE = 25;
    private static final boolean ENABLED = Boolean.parseBoolean(System.getProperty("nitro.nativeBufferPool.hugePages", "true"));
    private static final MethodHandle MADVISE = findMadvise();

    private NativeBufferAdvice() {}

    /**
     * Asks Linux to back the page-aligned interior of a native workspace with transparent huge pages.
     *
     * <p>This is advisory and deliberately fails closed: unsupported platforms, disabled native access, a missing
     * symbol, or kernel rejection leave the ordinary mapping unchanged.
     */
    public static boolean preferHugePages(MemorySegment segment)
    {
        return advisePageAlignedInterior(segment, MADV_HUGEPAGE);
    }

    /** Attempts an immediate collapse after a reusable native workspace has been populated. */
    public static boolean collapseHugePages(MemorySegment segment)
    {
        return advisePageAlignedInterior(segment, MADV_COLLAPSE);
    }

    private static boolean advisePageAlignedInterior(MemorySegment segment, int advice)
    {
        if (MADVISE == null || segment.byteSize() < PAGE_BYTES) {
            return false;
        }
        long address = segment.address();
        long start = (address + PAGE_BYTES - 1) & -PAGE_BYTES;
        long end = (address + segment.byteSize()) & -PAGE_BYTES;
        if (end <= start) {
            return false;
        }
        try {
            MemorySegment aligned = segment.asSlice(start - address, end - start);
            int result = (int) MADVISE.invokeExact(aligned, end - start, advice);
            return result == 0;
        }
        catch (Throwable ignored) {
            return false;
        }
    }

    private static MethodHandle findMadvise()
    {
        if (!ENABLED || !System.getProperty("os.name", "").equalsIgnoreCase("Linux")) {
            return null;
        }
        try {
            Linker linker = Linker.nativeLinker();
            return linker.downcallHandle(
                    linker.defaultLookup().find("madvise").orElseThrow(),
                    FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, JAVA_INT));
        }
        catch (Throwable ignored) {
            return null;
        }
    }
}
