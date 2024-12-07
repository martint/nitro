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
package org.weakref.nitro.data;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

public record Row(Long... values)
{
    public static Row row(Long... values)
    {
        return new Row(values);
    }

    @Override
    public boolean equals(Object o)
    {
        if (o instanceof Row(Long[] values)) {
            return Objects.deepEquals(this.values, values);
        }

        return false;
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(values);
    }

    @Override
    public String toString()
    {
        return "(" + Arrays.stream(values)
                .map(v -> v == null ? "null" : v.toString())
                .collect(Collectors.joining(",")) + ")";
    }
}
