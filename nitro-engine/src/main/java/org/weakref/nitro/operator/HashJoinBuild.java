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

import static java.util.Objects.requireNonNull;

/**
 * Immutable, shareable hash-join build state.
 *
 * <p>The owner keeps the retained build payload and index alive. Each probe session receives an
 * independent probe view with its own scratch state over the immutable index storage.
 */
public final class HashJoinBuild
        implements AutoCloseable
{
    private final HashJoinOperator owner;
    private DynamicFilter exactDynamicFilter;
    private int exactDynamicFilterColumn = -1;
    private boolean exactDynamicFilterInitialized;
    private boolean closed;

    HashJoinBuild(HashJoinOperator owner)
    {
        this.owner = requireNonNull(owner, "owner is null");
    }

    JoinIndex newProbeIndex()
    {
        if (closed) {
            throw new IllegalStateException("Hash join build is closed");
        }
        return owner.newPreparedProbeIndex();
    }

    boolean sharePayloadWith(BufferedJoinInput probeInput)
    {
        if (closed) {
            throw new IllegalStateException("Hash join build is closed");
        }
        return probeInput.shareLoadedNonRetainedBatches(owner.bufferedInner());
    }

    /**
     * Returns an exact single-key build membership filter when the prepared join index can expose one without
     * copying its keys. The returned immutable view remains valid until this build is closed.
     */
    public synchronized DynamicFilter exactDynamicFilter(int probeColumn)
    {
        if (closed) {
            throw new IllegalStateException("Hash join build is closed");
        }
        if (!exactDynamicFilterInitialized) {
            exactDynamicFilter = owner.exactBuildDynamicFilter(probeColumn);
            exactDynamicFilterColumn = probeColumn;
            exactDynamicFilterInitialized = true;
        }
        else if (exactDynamicFilterColumn != probeColumn) {
            throw new IllegalArgumentException("Prepared build membership was already targeted to another probe column");
        }
        return exactDynamicFilter;
    }

    boolean separateDynamicFilterCollectionActivated()
    {
        if (closed) {
            throw new IllegalStateException("Hash join build is closed");
        }
        return owner.separateDynamicFilterCollectionActivated();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        owner.close();
    }
}
