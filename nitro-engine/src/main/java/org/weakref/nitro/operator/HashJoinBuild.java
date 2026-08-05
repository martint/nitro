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
