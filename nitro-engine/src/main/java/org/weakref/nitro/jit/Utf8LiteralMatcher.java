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
package org.weakref.nitro.jit;

/**
 * Engine-side invocation protocol for a provider-authored UTF-8 literal comparison.
 *
 * <p>The generated implementation embeds the literal bytes in its instruction stream. It names no function or
 * provider class, so the provider may be loaded in an isolated connector/function class loader.
 */
@FunctionalInterface
interface Utf8LiteralMatcher
{
    boolean matches(byte[] data, int offset, int length);
}
