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
package org.weakref.nitro.core.execution;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static java.util.Objects.requireNonNull;

/// Host-neutral execution controls.
public interface ExecutionContext
{
    MemoryReservation memory();

    ExecutionPolicy policy();

    ExecutionDiagnostics diagnostics();

    boolean isYieldRequested();

    boolean isCancelled();

    /// Cooperates with the host scheduler at a restart-safe execution point.
    ///
    /// A stack-preserving host may park the current driver and return after it is readmitted. A host which must
    /// unwind the driver reports a requested yield with [ExecutionSuspension]. Operators must therefore call this
    /// only after committing progress to instance-owned state and establishing a valid re-entry point.
    default void checkpoint()
    {
        if (isCancelled()) {
            throw new IllegalStateException("execution is cancelled");
        }
        if (isYieldRequested()) {
            throw ExecutionSuspension.yield();
        }
    }

    /// Waits for a continuation at a restart-safe execution point.
    ///
    /// Stack-preserving hosts may park in this method. The default compatibility behavior unwinds to the island
    /// driver, which exposes the continuation to its host and re-enters the pull graph after it completes.
    default void await(CompletionStage<Void> continuation)
    {
        requireNonNull(continuation, "continuation is null");
        CompletableFuture<Void> future = continuation.toCompletableFuture();
        if (!future.isDone()) {
            throw ExecutionSuspension.blocked(continuation);
        }
        future.join();
        checkpoint();
    }

    /// Gives the host a chance to revoke retained memory or request spill.
    void requestMemoryRevocation();
}
