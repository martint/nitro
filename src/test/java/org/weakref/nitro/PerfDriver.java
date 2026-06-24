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
package org.weakref.nitro;

import java.lang.reflect.Method;

/**
 * Standalone perf-counter driver for any {@code BenchmarkQueries} harness (nitro or trino, any suite). Warms up,
 * then runs one query method N times in steady state so an external {@code sudo perf stat} over the whole process
 * yields ~per-query HW counters. Use the delta method (run twice with different iter counts, subtract) to strip
 * JVM startup/JIT/warmup. Run:
 *   sudo perf stat -e instructions,cycles,L1-dcache-loads,L1-dcache-load-misses taskset -c 0 java ... \
 *     org.weakref.nitro.PerfDriver &lt;benchmarkQueriesClass&gt; &lt;queryNN&gt; &lt;iters&gt; [&lt;warmup&gt;]
 */
public final class PerfDriver
{
    private PerfDriver() {}

    public static void main(String[] args)
            throws Exception
    {
        String className = args[0];
        String query = args[1];
        int iters = args.length > 2 ? Integer.parseInt(args[2]) : 12;
        int warmup = args.length > 3 ? Integer.parseInt(args[3]) : 6;

        Class<?> cls = Class.forName(className);
        Object bench = cls.getDeclaredConstructor().newInstance();
        invoke(cls, bench, "setup");
        Method invocationSetup = optional(cls, "setupInvocation");
        Method q = cls.getDeclaredMethod(query);
        q.setAccessible(true);

        for (int i = 0; i < warmup; i++) {
            if (invocationSetup != null) {
                invocationSetup.invoke(bench);
            }
            q.invoke(bench);
        }
        System.err.println("[warmup done] --- BEGIN MEASURED " + iters + "x " + className + "#" + query + " ---");
        long start = System.nanoTime();
        for (int i = 0; i < iters; i++) {
            if (invocationSetup != null) {
                invocationSetup.invoke(bench);
            }
            q.invoke(bench);
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        System.err.println("[MEASURED] " + query + " " + iters + " iters in " + elapsedMs + "ms = "
                + (elapsedMs / iters) + "ms/iter");
    }

    private static void invoke(Class<?> cls, Object bench, String name)
            throws Exception
    {
        Method m = cls.getDeclaredMethod(name);
        m.setAccessible(true);
        m.invoke(bench);
    }

    private static Method optional(Class<?> cls, String name)
    {
        try {
            Method m = cls.getDeclaredMethod(name);
            m.setAccessible(true);
            return m;
        }
        catch (NoSuchMethodException e) {
            return null;
        }
    }
}
