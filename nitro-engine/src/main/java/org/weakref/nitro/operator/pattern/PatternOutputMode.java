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
package org.weakref.nitro.operator.pattern;

/// SQL row-pattern output behavior, independent of a host engine's plan representation.
public enum PatternOutputMode
{
    ONE(true, true, false),
    ALL_SHOW_EMPTY(false, true, false),
    ALL_OMIT_EMPTY(false, false, false),
    ALL_WITH_UNMATCHED(false, true, true),
    WINDOW(true, true, true);

    private final boolean oneRow;
    private final boolean emptyMatches;
    private final boolean unmatchedRows;

    PatternOutputMode(boolean oneRow, boolean emptyMatches, boolean unmatchedRows)
    {
        this.oneRow = oneRow;
        this.emptyMatches = emptyMatches;
        this.unmatchedRows = unmatchedRows;
    }

    public boolean isOneRow()
    {
        return oneRow;
    }

    public boolean outputsEmptyMatches()
    {
        return emptyMatches;
    }

    public boolean outputsUnmatchedRows()
    {
        return unmatchedRows;
    }
}
