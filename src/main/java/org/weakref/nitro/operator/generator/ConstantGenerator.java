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

public class ConstantGenerator
        implements I64Generator
{
    private final long value;

    public ConstantGenerator(long value)
    {
        this.value = value;
    }

    @Override
    public void next()
    {
    }

    @Override
    public long value()
    {
        return value;
    }

    @Override
    public boolean isNull()
    {
        return false;
    }

    @Override
    public void skip(int count)
    {
    }
}
