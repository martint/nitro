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

import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

public final class OutputDebug
{
    private static final boolean ENABLED = Boolean.getBoolean("nitro.output.debug");

    private static final LongAdder baseOutputs = new LongAdder();
    private static final LongAdder selectedOutputs = new LongAdder();
    private static final LongAdder forwardedOutputs = new LongAdder();
    private static final LongAdder selectIdentityHits = new LongAdder();
    private static final LongAdder selectWrappedOutputs = new LongAdder();
    private static final LongAdder forwardWrappedOutputs = new LongAdder();
    private static final LongAdder outputsWithProjector = new LongAdder();
    private static final LongAdder outputsWithPositionsResolver = new LongAdder();
    private static final LongAdder outputsWithSinglePositionResolver = new LongAdder();
    private static final LongAdder borrowCalls = new LongAdder();
    private static final LongAdder borrowDepthCalls = new LongAdder();
    private static final LongAdder borrowCacheHits = new LongAdder();
    private static final LongAdder borrowResolvedCalls = new LongAdder();
    private static final LongAdder borrowValueCalls = new LongAdder();
    private static final LongAdder borrowNullCalls = new LongAdder();
    private static final LongAdder borrowErrorCalls = new LongAdder();
    private static final LongAdder takeCalls = new LongAdder();
    private static final LongAdder takeDepthCalls = new LongAdder();
    private static final LongAdder takeValueCalls = new LongAdder();
    private static final LongAdder takeNullCalls = new LongAdder();
    private static final LongAdder takeErrorCalls = new LongAdder();
    private static final LongAdder closeCalls = new LongAdder();
    private static final LongAdder closeDepthCalls = new LongAdder();
    private static final LongAdder releaseCalls = new LongAdder();
    private static final LongAdder releaseDepthCalls = new LongAdder();
    private static final LongAdder releaseValueCalls = new LongAdder();
    private static final LongAdder releaseNullCalls = new LongAdder();
    private static final LongAdder releaseErrorCalls = new LongAdder();
    private static final LongAdder projectPositionsCalls = new LongAdder();
    private static final LongAdder projectPositionsDepthCalls = new LongAdder();
    private static final LongAdder projectPositionsHits = new LongAdder();
    private static final LongAdder copyPositionsCalls = new LongAdder();
    private static final LongAdder copyPositionsDepthCalls = new LongAdder();
    private static final LongAdder copyPositionsHits = new LongAdder();
    private static final LongAdder copySinglePositionCalls = new LongAdder();
    private static final LongAdder copySinglePositionDepthCalls = new LongAdder();
    private static final LongAdder copySinglePositionHits = new LongAdder();
    private static final LongAccumulator maxDepth = new LongAccumulator(Long::max, 0);

    private OutputDebug() {}

    public static boolean enabled()
    {
        return ENABLED;
    }

    public static void reset()
    {
        if (!ENABLED) {
            return;
        }
        baseOutputs.reset();
        selectedOutputs.reset();
        forwardedOutputs.reset();
        selectIdentityHits.reset();
        selectWrappedOutputs.reset();
        forwardWrappedOutputs.reset();
        outputsWithProjector.reset();
        outputsWithPositionsResolver.reset();
        outputsWithSinglePositionResolver.reset();
        borrowCalls.reset();
        borrowDepthCalls.reset();
        borrowCacheHits.reset();
        borrowResolvedCalls.reset();
        borrowValueCalls.reset();
        borrowNullCalls.reset();
        borrowErrorCalls.reset();
        takeCalls.reset();
        takeDepthCalls.reset();
        takeValueCalls.reset();
        takeNullCalls.reset();
        takeErrorCalls.reset();
        closeCalls.reset();
        closeDepthCalls.reset();
        releaseCalls.reset();
        releaseDepthCalls.reset();
        releaseValueCalls.reset();
        releaseNullCalls.reset();
        releaseErrorCalls.reset();
        projectPositionsCalls.reset();
        projectPositionsDepthCalls.reset();
        projectPositionsHits.reset();
        copyPositionsCalls.reset();
        copyPositionsDepthCalls.reset();
        copyPositionsHits.reset();
        copySinglePositionCalls.reset();
        copySinglePositionDepthCalls.reset();
        copySinglePositionHits.reset();
        maxDepth.reset();
    }

    public static void recordBaseOutput(boolean hasProjector, boolean hasPositionsResolver, boolean hasSinglePositionResolver)
    {
        if (!ENABLED) {
            return;
        }
        baseOutputs.increment();
        recordCapabilities(hasProjector, hasPositionsResolver, hasSinglePositionResolver);
    }

    public static void recordSelectedOutput(boolean hasProjector, boolean hasPositionsResolver, boolean hasSinglePositionResolver, int depth)
    {
        if (!ENABLED) {
            return;
        }
        selectedOutputs.increment();
        selectWrappedOutputs.increment();
        maxDepth.accumulate(depth);
        recordCapabilities(hasProjector, hasPositionsResolver, hasSinglePositionResolver);
    }

    public static void recordSelectIdentityHit()
    {
        if (!ENABLED) {
            return;
        }
        selectIdentityHits.increment();
    }

    public static void recordForwardedOutput(boolean hasProjector, boolean hasPositionsResolver, boolean hasSinglePositionResolver, int depth)
    {
        if (!ENABLED) {
            return;
        }
        forwardedOutputs.increment();
        forwardWrappedOutputs.increment();
        maxDepth.accumulate(depth);
        recordCapabilities(hasProjector, hasPositionsResolver, hasSinglePositionResolver);
    }

    public static void recordBorrow(int depth, int streamIndex, boolean cacheHit)
    {
        if (!ENABLED) {
            return;
        }
        borrowCalls.increment();
        if (depth > 0) {
            borrowDepthCalls.increment();
            maxDepth.accumulate(depth);
        }
        if (cacheHit) {
            borrowCacheHits.increment();
        }
        else {
            borrowResolvedCalls.increment();
        }
        switch (streamIndex) {
            case 0 -> borrowValueCalls.increment();
            case 1 -> borrowNullCalls.increment();
            case 2 -> borrowErrorCalls.increment();
            default -> throw new IllegalArgumentException("Unknown stream index: " + streamIndex);
        }
    }

    public static void recordCopyPositions(int depth, boolean hit)
    {
        if (!ENABLED) {
            return;
        }
        copyPositionsCalls.increment();
        if (depth > 0) {
            copyPositionsDepthCalls.increment();
            maxDepth.accumulate(depth);
        }
        if (hit) {
            copyPositionsHits.increment();
        }
    }

    public static void recordCopySinglePosition(int depth, boolean hit)
    {
        if (!ENABLED) {
            return;
        }
        copySinglePositionCalls.increment();
        if (depth > 0) {
            copySinglePositionDepthCalls.increment();
            maxDepth.accumulate(depth);
        }
        if (hit) {
            copySinglePositionHits.increment();
        }
    }

    public static void recordTake(int depth, int streamIndex)
    {
        if (!ENABLED) {
            return;
        }
        takeCalls.increment();
        if (depth > 0) {
            takeDepthCalls.increment();
            maxDepth.accumulate(depth);
        }
        switch (streamIndex) {
            case 0 -> takeValueCalls.increment();
            case 1 -> takeNullCalls.increment();
            case 2 -> takeErrorCalls.increment();
            default -> throw new IllegalArgumentException("Unknown stream index: " + streamIndex);
        }
    }

    public static void recordClose(int depth)
    {
        if (!ENABLED) {
            return;
        }
        closeCalls.increment();
        if (depth > 0) {
            closeDepthCalls.increment();
            maxDepth.accumulate(depth);
        }
    }

    public static void recordRelease(int depth, int streamIndex)
    {
        if (!ENABLED) {
            return;
        }
        releaseCalls.increment();
        if (depth > 0) {
            releaseDepthCalls.increment();
            maxDepth.accumulate(depth);
        }
        switch (streamIndex) {
            case 0 -> releaseValueCalls.increment();
            case 1 -> releaseNullCalls.increment();
            case 2 -> releaseErrorCalls.increment();
            default -> throw new IllegalArgumentException("Unknown stream index: " + streamIndex);
        }
    }

    public static String snapshot()
    {
        if (!ENABLED) {
            return "OutputDebug disabled";
        }
        return """
                OutputDebug
                base_outputs=%s
                selected_outputs=%s
                forwarded_outputs=%s
                select_identity_hits=%s
                select_wrapped_outputs=%s
                forward_wrapped_outputs=%s
                outputs_with_projector=%s
                outputs_with_positions_resolver=%s
                outputs_with_single_position_resolver=%s
                borrow_calls=%s
                borrow_depth_calls=%s
                borrow_cache_hits=%s
                borrow_resolved_calls=%s
                borrow_value_calls=%s
                borrow_null_calls=%s
                borrow_error_calls=%s
                take_calls=%s
                take_depth_calls=%s
                take_value_calls=%s
                take_null_calls=%s
                take_error_calls=%s
                close_calls=%s
                close_depth_calls=%s
                release_calls=%s
                release_depth_calls=%s
                release_value_calls=%s
                release_null_calls=%s
                release_error_calls=%s
                project_positions_calls=%s
                project_positions_depth_calls=%s
                project_positions_hits=%s
                copy_positions_calls=%s
                copy_positions_depth_calls=%s
                copy_positions_hits=%s
                copy_single_position_calls=%s
                copy_single_position_depth_calls=%s
                copy_single_position_hits=%s
                max_depth=%s
                """.formatted(
                baseOutputs.sum(),
                selectedOutputs.sum(),
                forwardedOutputs.sum(),
                selectIdentityHits.sum(),
                selectWrappedOutputs.sum(),
                forwardWrappedOutputs.sum(),
                outputsWithProjector.sum(),
                outputsWithPositionsResolver.sum(),
                outputsWithSinglePositionResolver.sum(),
                borrowCalls.sum(),
                borrowDepthCalls.sum(),
                borrowCacheHits.sum(),
                borrowResolvedCalls.sum(),
                borrowValueCalls.sum(),
                borrowNullCalls.sum(),
                borrowErrorCalls.sum(),
                takeCalls.sum(),
                takeDepthCalls.sum(),
                takeValueCalls.sum(),
                takeNullCalls.sum(),
                takeErrorCalls.sum(),
                closeCalls.sum(),
                closeDepthCalls.sum(),
                releaseCalls.sum(),
                releaseDepthCalls.sum(),
                releaseValueCalls.sum(),
                releaseNullCalls.sum(),
                releaseErrorCalls.sum(),
                projectPositionsCalls.sum(),
                projectPositionsDepthCalls.sum(),
                projectPositionsHits.sum(),
                copyPositionsCalls.sum(),
                copyPositionsDepthCalls.sum(),
                copyPositionsHits.sum(),
                copySinglePositionCalls.sum(),
                copySinglePositionDepthCalls.sum(),
                copySinglePositionHits.sum(),
                maxDepth.get());
    }

    private static void recordCapabilities(boolean hasProjector, boolean hasPositionsResolver, boolean hasSinglePositionResolver)
    {
        if (hasProjector) {
            outputsWithProjector.increment();
        }
        if (hasPositionsResolver) {
            outputsWithPositionsResolver.increment();
        }
        if (hasSinglePositionResolver) {
            outputsWithSinglePositionResolver.increment();
        }
    }
}
