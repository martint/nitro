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
package org.weakref.nitro.operator.filter;

import org.weakref.nitro.data.I64VectorWithNulls;
import org.weakref.nitro.data.Vector;

import java.util.function.LongPredicate;

public class I64Predicate
        implements VectorPredicate
{
    private final LongPredicate predicate;

    public I64Predicate(LongPredicate predicate)
    {
        this.predicate = predicate;
    }

    @Override
    public boolean test(Vector vector, int position)
    {
        I64VectorWithNulls i64Vector = (I64VectorWithNulls) vector;
        return !i64Vector.nulls()[position] && predicate.test(i64Vector.values()[position]);
    }
}
