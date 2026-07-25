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
package org.weakref.nitro.core.batch;

/// Structural streams exposed by one source column.
///
/// `VALUES` uses a representation admitted by the column's [TypeBinding]. `NULLS` and
/// `ERRORS`, when present, are boolean-valued Nitro vectors and may themselves be encoded.
public enum ColumnStream
{
    VALUES,
    NULLS,
    ERRORS,
}
