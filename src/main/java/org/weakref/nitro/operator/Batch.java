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

import org.weakref.nitro.data.Mask;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.checkIndex;
import static java.util.Objects.requireNonNull;

public final class Batch
{
    private Mask mask;
    private final Output[] outputs;
    private final Function<Mask, Mask> maskTakeResolver;
    private final Consumer<Mask> constrainer;
    private boolean maskTaken;

    public Batch(Mask mask, Output... outputs)
    {
        this(mask, _ -> {}, Function.identity(), outputs);
    }

    public Batch(Mask mask, Function<Mask, Mask> maskTakeResolver, Output... outputs)
    {
        this(mask, _ -> {}, maskTakeResolver, outputs);
    }

    public Batch(Mask mask, Consumer<Mask> constrainer, Function<Mask, Mask> maskTakeResolver, Output... outputs)
    {
        this.mask = requireNonNull(mask, "mask is null");
        this.constrainer = requireNonNull(constrainer, "constrainer is null");
        this.maskTakeResolver = requireNonNull(maskTakeResolver, "maskTakeResolver is null");
        this.outputs = Arrays.copyOf(outputs, outputs.length);
    }

    public Mask borrowMask()
    {
        if (maskTaken) {
            throw new IllegalStateException("Mask already taken");
        }
        return mask;
    }

    public Mask takeMask()
    {
        Mask borrowedMask = borrowMask();
        maskTaken = true;
        return requireNonNull(maskTakeResolver.apply(borrowedMask), "maskTakeResolver returned null");
    }

    public void constrain(Mask mask)
    {
        if (maskTaken) {
            throw new IllegalStateException("Mask already taken");
        }
        this.mask = requireNonNull(mask, "mask is null");
        constrainer.accept(mask);
    }

    public Output output(int outputIndex)
    {
        return outputs[checkIndex(outputIndex, outputs.length)];
    }
}
