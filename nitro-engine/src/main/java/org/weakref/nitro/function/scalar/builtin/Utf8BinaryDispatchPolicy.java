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

/**
 * Immutable representation-specialization choices shared by UTF-8 binary functions.
 */
public record Utf8BinaryDispatchPolicy(
        boolean monomorphicDictionaryMask,
        boolean directSingleDictionaryMatch,
        boolean wordEquals,
        boolean flattenedDictionaryEquals,
        boolean flatSingleValueEqualsMask,
        boolean directDictionaryPath,
        boolean sparseDictionaryContains)
{
    public static Utf8BinaryDispatchPolicy defaults()
    {
        return new Utf8BinaryDispatchPolicy(true, true, true, true, true, true, true);
    }

    public static Utf8BinaryDispatchPolicy fromSystemProperties()
    {
        return new Utf8BinaryDispatchPolicy(
                Boolean.parseBoolean(System.getProperty("nitro.utf8.monomorphicDictionaryMask", "true")),
                Boolean.parseBoolean(System.getProperty("nitro.utf8.directSingleDictionaryMatch", "true")),
                Boolean.parseBoolean(System.getProperty("nitro.utf8.wordEquals", "true")),
                Boolean.parseBoolean(System.getProperty("nitro.utf8.flattenedDictionaryEquals", "true")),
                Boolean.parseBoolean(System.getProperty("nitro.utf8.flatSingleValueEqualsMask", "true")),
                Boolean.parseBoolean(System.getProperty("nitro.utf8.directDictionaryPath", "true")),
                Boolean.parseBoolean(System.getProperty("nitro.utf8.sparseDictionaryContains", "true")));
    }
}
