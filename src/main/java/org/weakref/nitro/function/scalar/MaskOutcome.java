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
package org.weakref.nitro.function.scalar;

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;

public record MaskOutcome(Mask trueMask, Mask nullMask, Mask errorMask)
{
    public int falseCount(Mask domainMask)
    {
        return domainMask.selectedCount() - trueMask.selectedCount() - nullMask.selectedCount() - errorMask.selectedCount();
    }

    public Mask falseMask(Allocator allocator, Allocator.Context context, Mask domainMask)
    {
        Mask withoutTrue = trueMask.none() ? domainMask : allocator.differenceMask(context, domainMask, trueMask);
        Mask withoutNull = nullMask.none() ? withoutTrue : allocator.differenceMask(context, withoutTrue, nullMask);
        return errorMask.none() ? withoutNull : allocator.differenceMask(context, withoutNull, errorMask);
    }

    public Mask survivorsMask(Allocator allocator, Allocator.Context context)
    {
        Mask survivors = trueMask;
        if (!nullMask.none()) {
            survivors = survivors.none() ? nullMask : allocator.unionMask(context, survivors, nullMask);
        }
        if (!errorMask.none()) {
            survivors = survivors.none() ? errorMask : allocator.unionMask(context, survivors, errorMask);
        }
        return survivors;
    }
}
