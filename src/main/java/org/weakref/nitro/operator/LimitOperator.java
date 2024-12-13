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
import org.weakref.nitro.data.Vector;

import static java.lang.Math.toIntExact;

public class LimitOperator
        implements Operator
{
    private final long limit;
    private final Operator source;

    private long count;

    public LimitOperator(long limit, Operator source)
    {
        this.limit = limit;
        this.source = source;
    }

    @Override
    public int columnCount()
    {
        return source.columnCount();
    }

    @Override
    public Mask next()
    {
        Mask mask = source.next();

        int remaining = toIntExact(Math.min(limit - this.count, mask.count()));

        mask = mask.first(remaining);
        source.constrain(mask);

        this.count += remaining;

        return mask;
    }

    @Override
    public boolean hasNext()
    {
        return source.hasNext() && count < limit;
    }

    @Override
    public void constrain(Mask mask)
    {
        source.constrain(mask);
    }

    @Override
    public Vector column(int column)
    {
        return source.column(column);
    }

    @Override
    public void close()
    {
        source.close();
    }
}
