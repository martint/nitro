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
package org.weakref.nitro.function.scalar.builtin;

import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/** Shared physical mask kernel for every typed SQL IS NULL scalar. */
final class IsNullMaskSupport
{
    private static final boolean DIRECT_MASK =
            Boolean.parseBoolean(System.getProperty("nitro.isNull.directMask", "true"));

    private IsNullMaskSupport() {}

    public static boolean evaluateInPlace(List<Streams> inputs, Mask mask, boolean selectNull)
    {
        if (!DIRECT_MASK) {
            return false;
        }
        checkArgument(inputs.size() == 1, "Unexpected argument count for is_null");
        Vector nulls = inputs.get(0).getOrNull(Stream.NULLS);
        if (VectorAccess.isAllFalseNulls(nulls)) {
            if (selectNull) {
                mask.clear(mask.size());
            }
            return true;
        }

        boolean[] values = VectorAccess.flatBooleans(nulls);
        if (values == null) {
            return false;
        }
        mask.retainBooleans(values, selectNull);
        return true;
    }
}
