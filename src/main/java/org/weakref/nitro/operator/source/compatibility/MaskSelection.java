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
package org.weakref.nitro.operator.source.compatibility;

import org.weakref.nitro.core.batch.Selection;
import org.weakref.nitro.data.Mask;

import static java.util.Objects.requireNonNull;

/// Zero-copy selection facade used by the legacy native-batch adapter.
final class MaskSelection
        implements Selection
{
    private final Mask mask;

    MaskSelection(Mask mask)
    {
        this.mask = requireNonNull(mask, "mask is null");
    }

    Mask mask()
    {
        return mask;
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
        return mask.position(index);
    }
}
