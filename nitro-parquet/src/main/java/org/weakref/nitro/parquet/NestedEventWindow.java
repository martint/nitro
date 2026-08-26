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
package org.weakref.nitro.parquet;

final class NestedEventWindow
{
    private PhysicalValueDecoder values;
    private int[] repetitionLevels;
    private int[] definitionLevels;
    private int[] valueOrdinals;
    private int[] dictionaryIds;
    private int offset;
    private int length;
    private boolean constantDefinitionLevel;
    private int definitionLevel;
    private boolean physicalValuesPresent;

    void reset(
            PhysicalValueDecoder values,
            int[] repetitionLevels,
            int[] definitionLevels,
            int[] valueOrdinals,
            int[] dictionaryIds,
            int offset,
            int length)
    {
        this.values = values;
        this.repetitionLevels = repetitionLevels;
        this.definitionLevels = definitionLevels;
        this.valueOrdinals = valueOrdinals;
        this.dictionaryIds = dictionaryIds;
        this.offset = offset;
        this.length = length;
        this.constantDefinitionLevel = false;
        this.physicalValuesPresent = false;
    }

    void resetConstantDefinitionLevel(
            PhysicalValueDecoder values,
            int[] repetitionLevels,
            int definitionLevel,
            boolean physicalValuesPresent,
            int[] dictionaryIds,
            int offset,
            int length)
    {
        this.values = values;
        this.repetitionLevels = repetitionLevels;
        this.definitionLevels = null;
        this.valueOrdinals = null;
        this.dictionaryIds = dictionaryIds;
        this.offset = offset;
        this.length = length;
        this.constantDefinitionLevel = true;
        this.definitionLevel = definitionLevel;
        this.physicalValuesPresent = physicalValuesPresent;
    }

    int length()
    {
        return length;
    }

    int repetitionLevel(int index)
    {
        return repetitionLevels[offset + index];
    }

    int definitionLevel(int index)
    {
        return constantDefinitionLevel ? definitionLevel : definitionLevels[offset + index];
    }

    int[] repetitionLevels()
    {
        return repetitionLevels;
    }

    int[] definitionLevels()
    {
        return definitionLevels;
    }

    boolean allDefinitionLevelsAtLeast(int minimum)
    {
        return constantDefinitionLevel && definitionLevel >= minimum;
    }

    int offset()
    {
        return offset;
    }

    void appendTo(NestedValueAccumulator accumulator, int start, int count)
    {
        if (start < 0 || count < 0 || start > length - count) {
            throw new IndexOutOfBoundsException("Invalid nested event subwindow: " + start + ", " + count);
        }
        if (constantDefinitionLevel) {
            if (!physicalValuesPresent) {
                for (int index = 0; index < count; index++) {
                    accumulator.appendNull();
                }
            }
            else if (dictionaryIds == null) {
                accumulator.appendPlainRun(values, offset + start, count);
            }
            else {
                accumulator.appendDictionaryRun(values, dictionaryIds, offset + start, count);
            }
            return;
        }
        accumulator.appendEvents(values, valueOrdinals, dictionaryIds, offset + start, count);
    }
}
