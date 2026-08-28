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

import java.util.Optional;
import java.util.concurrent.CompletionStage;

import static java.util.Objects.requireNonNull;

/// Control transfer from a restart-safe operator checkpoint to its island driver.
///
/// This is not a query failure. It is stackless because cooperative suspension is expected during
/// ordinary execution under hosts which cannot park and resume the current driver stack.
public final class ExecutionSuspension
        extends RuntimeException
{
    public enum Reason
    {
        YIELD,
        BLOCKED
    }

    private static final ExecutionSuspension YIELD = new ExecutionSuspension(Reason.YIELD, null);

    private final Reason reason;
    private final CompletionStage<Void> continuation;

    private ExecutionSuspension(Reason reason, CompletionStage<Void> continuation)
    {
        super(null, null, false, false);
        this.reason = requireNonNull(reason, "reason is null");
        this.continuation = continuation;
    }

    public static ExecutionSuspension yield()
    {
        return YIELD;
    }

    public static ExecutionSuspension blocked(CompletionStage<Void> continuation)
    {
        return new ExecutionSuspension(Reason.BLOCKED, requireNonNull(continuation, "continuation is null"));
    }

    public Reason reason()
    {
        return reason;
    }

    public Optional<CompletionStage<Void>> continuation()
    {
        return Optional.ofNullable(continuation);
    }
}
