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

/**
 * Reusable scalar and batch list views for join matches.
 */
final class JoinMatchScratch
{
    private final SingleLongList scalarSingle = new SingleLongList();
    private final ChainLongList scalarChain = new ChainLongList();
    private ChainLongList[] batchChains;

    SingleLongList scalarSingle()
    {
        return scalarSingle;
    }

    ChainLongList scalarChain()
    {
        return scalarChain;
    }

    void prepareBatch(int capacity)
    {
        if (batchChains == null || batchChains.length < capacity) {
            batchChains = ChainLongList.createArray(capacity);
        }
    }

    ChainLongList batchChain(int index)
    {
        return batchChains[index];
    }

    long retainedBytes()
    {
        return batchChains == null ? 0 : (long) batchChains.length * Long.BYTES;
    }
}
