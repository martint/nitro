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

public final class Utf8Traits
{
    public static final BinaryVector.Trait UTF8_STRING = BinaryVector.Trait.flag("utf8_string");
    public static final BinaryVector.Trait ASCII_ONLY = BinaryVector.Trait.flag("ascii_only");

    private Utf8Traits() {}
}
