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
package org.weakref.nitro.core.function;

import org.weakref.nitro.data.ErrorValue;

/**
 * Provider-owned classification of a scalar target failure as a row-local error.
 *
 * <p>The implementation returns a classloader-neutral error for a declared row failure and rethrows every
 * infrastructure, cancellation, linkage, or fatal failure. It is called only after the scalar target throws, not
 * around carrier readers or result writers.
 */
@FunctionalInterface
public interface ScalarFailureMapper
{
    ErrorValue map(Throwable failure)
            throws Throwable;
}
