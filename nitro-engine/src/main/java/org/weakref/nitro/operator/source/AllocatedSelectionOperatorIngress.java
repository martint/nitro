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
package org.weakref.nitro.operator.source;

import org.weakref.nitro.core.batch.Selection;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.MaskSelection;

import static java.util.Objects.requireNonNull;

/// Allocator-owned translation between source selections and native masks.
public final class AllocatedSelectionOperatorIngress
        implements SelectionOperatorIngress
{
    private final Allocator allocator;
    private final Allocator.Context allocationContext;

    public AllocatedSelectionOperatorIngress(Allocator allocator, Allocator.Context allocationContext)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.allocationContext = requireNonNull(allocationContext, "allocationContext is null");
    }

    @Override
    public Mask toMask(Selection selection)
    {
        requireNonNull(selection, "selection is null");
        int positionCount = selection.positionCount();
        int selectedCount = selection.count();
        if (positionCount < 0 || selectedCount < 0 || selectedCount > positionCount) {
            throw new IllegalArgumentException("invalid selection cardinality");
        }

        if (selection.isDense()) {
            if (selectedCount != positionCount) {
                throw new IllegalArgumentException("dense selection does not select every position");
            }
            for (int index = 0; index < selectedCount; index++) {
                if (selection.position(index) != index) {
                    throw new IllegalArgumentException("dense selection positions are not contiguous");
                }
            }
            int expectedMaxPosition = positionCount == 0 ? -1 : positionCount - 1;
            if (selection.maxPosition() != expectedMaxPosition) {
                throw new IllegalArgumentException("selection maxPosition does not match its positions");
            }
            return allocator.allocateAllMask(allocationContext, positionCount);
        }
        if (selectedCount == positionCount) {
            throw new IllegalArgumentException("selection marks every position but is not dense");
        }

        Mask mask = allocator.allocateUninitializedSparseMask(allocationContext, selectedCount, positionCount);
        try {
            int[] positions = mask.positionsArrayForOverwrite(selectedCount);
            int previous = -1;
            for (int index = 0; index < selectedCount; index++) {
                int position = selection.position(index);
                if (position <= previous || position >= positionCount) {
                    throw new IllegalArgumentException("selection positions are not strictly increasing and in bounds");
                }
                positions[index] = position;
                previous = position;
            }
            if (selection.maxPosition() != previous) {
                throw new IllegalArgumentException("selection maxPosition does not match its positions");
            }
            return mask;
        }
        catch (RuntimeException | Error failure) {
            allocator.release(allocationContext, mask);
            throw failure;
        }
    }

    @Override
    public Selection toSelection(Mask mask)
    {
        return new MaskSelection(requireNonNull(mask, "mask is null"));
    }

    @Override
    public Mask takeMask(Mask mask)
    {
        return allocator.transfer(allocationContext, requireNonNull(mask, "mask is null"));
    }

    @Override
    public void releaseMask(Mask mask)
    {
        allocator.release(allocationContext, requireNonNull(mask, "mask is null"));
    }
}
