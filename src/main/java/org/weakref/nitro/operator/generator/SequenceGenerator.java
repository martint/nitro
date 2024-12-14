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
package org.weakref.nitro.operator.generator;

public class SequenceGenerator
        implements Generator
{
    private final long start;
    private final long max;

    private long current;

    public SequenceGenerator(long start)
    {
        this(start, Long.MAX_VALUE);
    }

    public SequenceGenerator(long start, long max)
    {
        this.start = start;
        this.max = max;

        current = start - 1;
    }

    @Override
    public void next()
    {
        current++;
        if (current == max) {
            current = start;
        }
    }

    @Override
    public long value()
    {
        return current;
    }

    @Override
    public boolean isNull()
    {
        return false;
    }

    @Override
    public void skip(int count)
    {
        current += count;
        if (current >= max) {
            current = start + (current - max);
        }
    }
}
