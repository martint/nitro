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
package org.weakref.nitro.operator.evaluator.functions;

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.EvaluationContext;
import org.weakref.nitro.operator.evaluator.Function;
import org.weakref.nitro.operator.evaluator.Result;

import java.util.function.LongPredicate;

public class I64Predicate
        implements Function
{
    private static final Allocator.Context CONTEXT = new Allocator.Context("I64Predicate");

    private final int input;
    private final LongPredicate predicate;

    public I64Predicate(int input, LongPredicate predicate)
    {
        this.input = input;
        this.predicate = predicate;
    }

    @Override
    public Result apply(Result output, Mask mask, EvaluationContext context)
    {
        Vector vec = context.evaluate(input, mask).values();
        Vector out = context.allocator().allocateOrGrow(CONTEXT, output != null ? output.values() : null, vec.length(), BooleanVector::new);
        BooleanVector result = (BooleanVector) out;

        if (vec instanceof RleVector rle) {
            applyRle(rle, mask, result);
        }
        else {
            applyFlat(vec, mask, result);
        }

        return Result.of(result);
    }

    private void applyFlat(Vector vector, Mask mask, BooleanVector output)
    {
        long[] values = ((I64Vector) vector).values();

        if (mask.all()) {
            for (int i = 0; i <= mask.maxPosition(); i++) {
                output.values()[i] = predicate.test(values[i]);
            }
        }
        else {
            for (int pos : mask) {
                output.values()[pos] = predicate.test(values[pos]);
            }
        }
    }

    private void applyRle(RleVector rle, Mask mask, BooleanVector output)
    {
        I64Vector values = (I64Vector) rle.values();

        int position = 0;
        for (int run = 0; run < rle.counts().length; run++) {
            int runLength = rle.counts()[run];
            int runEnd = position + runLength - 1;

            if (mask.anyTrue(position, runEnd)) {
                boolean result = predicate.test(values.values()[run]);

                if (mask.all()) {
                    for (int i = position; i <= runEnd; i++) {
                        output.values()[i] = result;
                    }
                }
                else {
                    for (int i = position; i <= runEnd; i++) {
                        if (mask.contains(i)) {
                            output.values()[i] = result;
                        }
                    }
                }
            }

            position += runLength;
        }
    }
}
