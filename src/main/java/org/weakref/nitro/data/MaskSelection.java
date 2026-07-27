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

import org.weakref.nitro.core.batch.Selection;

import static java.util.Objects.checkIndex;
import static java.util.Objects.requireNonNull;

/// Read-only selection facade over a Nitro mask.
public record MaskSelection(Mask mask)
        implements Selection
{
    public MaskSelection
    {
        requireNonNull(mask, "mask is null");
    }

    @Override
    public int positionCount()
    {
        return mask.size();
    }

    @Override
    public int count()
    {
        return mask.count();
    }

    @Override
    public int maxPosition()
    {
        return mask.none() ? -1 : mask.maxPosition();
    }

    @Override
    public boolean isDense()
    {
        return mask.all();
    }

    @Override
    public int position(int index)
    {
        return mask.position(checkIndex(index, count()));
    }
}
