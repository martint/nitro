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

import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.operator.HashJoinOperator.JoinFilter;
import org.weakref.nitro.operator.HashJoinOperator.LongJoinFilterFunction;

import static org.weakref.nitro.data.Mask.ComparisonOperator.LESS_THAN;

public final class JoinFilterFunctions
{
    private JoinFilterFunctions() {}

    public static JoinFilter longNotEqual(int outerColumn, int innerColumn)
    {
        return new JoinFilter(
                outerColumn,
                innerColumn,
                (LongJoinFilterFunction) (outerValue, innerValue) -> outerValue != innerValue);
    }

    public static JoinFilter longLessThan(int outerColumn, int innerColumn)
    {
        return new JoinFilter(
                outerColumn,
                innerColumn,
                new LongJoinFilterFunction()
                {
                    @Override
                    public boolean testLong(long outerValue, long innerValue)
                    {
                        return outerValue < innerValue;
                    }

                    @Override
                    public boolean supportsOuterMaskPruning(Vector outer)
                    {
                        return outer instanceof I64Vector || outer instanceof DictionaryVector || outer instanceof RleVector;
                    }

                    @Override
                    public boolean pruneOuterMask(Vector outer, Mask mask, Vector inner, int innerPosition, boolean[] dictionaryScratch)
                    {
                        long literal = VectorAccess.longValues(inner).value(innerPosition);
                        return switch (outer) {
                            case I64Vector values -> {
                                mask.retainConstantComparison(values.values(), literal, LESS_THAN);
                                yield true;
                            }
                            case DictionaryVector dictionary when dictionary.values().length() == dictionaryScratch.length -> {
                                int domainSize = dictionary.values().length();
                                for (int domain = 0; domain < domainSize; domain++) {
                                    dictionaryScratch[domain] = VectorAccess.longValues(dictionary.values()).value(domain) < literal;
                                }
                                mask.retainDictionaryComparison(dictionary, dictionaryScratch);
                                yield true;
                            }
                            case RleVector rle -> {
                                if (VectorAccess.longValues(rle.values()).value(0) >= literal) {
                                    mask.clear(mask.size());
                                }
                                yield true;
                            }
                            default -> false;
                        };
                    }
                });
    }

    public static JoinFilter longGreaterThan(int outerColumn, int innerColumn)
    {
        return new JoinFilter(
                outerColumn,
                innerColumn,
                (LongJoinFilterFunction) (outerValue, innerValue) -> outerValue > innerValue);
    }

    public static JoinFilter longBitwiseOverlap(int outerColumn, int innerColumn)
    {
        return new JoinFilter(outerColumn, innerColumn, new LongJoinFilterFunction()
        {
            @Override
            public boolean testLong(long outerValue, long innerValue)
            {
                return (outerValue & innerValue) != 0;
            }

            @Override
            public boolean rejectsZeroInnerValue()
            {
                return true;
            }
        });
    }
}
