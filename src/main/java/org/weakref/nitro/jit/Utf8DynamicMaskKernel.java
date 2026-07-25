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

import org.weakref.nitro.data.Mask;

/**
 * Engine-owned raw-buffer protocol for generated UTF-8 mask kernels.
 *
 * <p>Every method has the same classloader-neutral carrier signature. Each entry point gives the generated class a
 * monomorphic physical mapping shape; unused id arrays are null and are never read by that entry point.
 */
interface Utf8DynamicMaskKernel
{
    void retainFlatFlat(byte[] leftData, int[] leftOffsets, int[] leftIds, boolean[] leftNulls, byte[] rightData, int[] rightOffsets, int[] rightIds, boolean[] rightNulls, Mask mask, boolean selectMatches);

    void retainDictionaryDictionary(byte[] leftData, int[] leftOffsets, int[] leftIds, boolean[] leftNulls, byte[] rightData, int[] rightOffsets, int[] rightIds, boolean[] rightNulls, Mask mask, boolean selectMatches);

    void retainDictionaryFlat(byte[] leftData, int[] leftOffsets, int[] leftIds, boolean[] leftNulls, byte[] rightData, int[] rightOffsets, int[] rightIds, boolean[] rightNulls, Mask mask, boolean selectMatches);

    void retainFlatDictionary(byte[] leftData, int[] leftOffsets, int[] leftIds, boolean[] leftNulls, byte[] rightData, int[] rightOffsets, int[] rightIds, boolean[] rightNulls, Mask mask, boolean selectMatches);
}
