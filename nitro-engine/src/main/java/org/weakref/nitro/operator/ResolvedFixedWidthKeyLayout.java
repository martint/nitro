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
package org.weakref.nitro.operator;

import org.weakref.nitro.core.type.FixedWidthKeyLayout;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.RegionVector;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.ArrayList;
import java.util.List;

/** One provider-neutral fixed-width key layout resolved across the logical key columns of a consumer. */
record ResolvedFixedWidthKeyLayout(Lane[] lanes, int logicalKeyCount)
{
    record Lane(int logicalKey, List<String> fieldPath, FixedWidthKeyLayout.Carrier carrier) {}

    static ResolvedFixedWidthKeyLayout tryCreate(
            List<TypeBinding> keyTypes,
            StructuralKeyKernel[] kernels,
            Vector[] values)
    {
        if (keyTypes.size() != values.length || kernels.length != values.length) {
            return null;
        }
        ArrayList<Lane> lanes = new ArrayList<>();
        for (int key = 0; key < values.length; key++) {
            FixedWidthKeyLayout fixedWidth = keyTypes.get(key).fixedWidthKeyLayout().orElse(null);
            if (fixedWidth != null) {
                for (FixedWidthKeyLayout.Lane lane : fixedWidth.lanes()) {
                    Lane descriptor = new Lane(key, lane.fieldPath(), lane.carrier());
                    if (!supportsLane(values, descriptor)) {
                        throw new IllegalArgumentException("Type provider fixed-width key lane is incompatible with the admitted vector: " + descriptor);
                    }
                    lanes.add(descriptor);
                }
                continue;
            }
            if (!kernels[key].allowsLegacyPhysicalShortcuts() || !GroupingState.isSingleLongGroupingCandidate(values[key])) {
                return null;
            }
            lanes.add(new Lane(key, List.of(), integerCarrier(values[key])));
        }
        if (lanes.isEmpty() || lanes.size() > AbstractMultiLongGroupingTable.MAX_ARITY) {
            throw new IllegalArgumentException("Unsupported fixed-width key lane count: " + lanes.size());
        }
        return new ResolvedFixedWidthKeyLayout(lanes.toArray(Lane[]::new), values.length);
    }

    Vector laneVector(Vector[] values, int lane)
    {
        return laneVector(values, lanes[lane]);
    }

    private static boolean supportsLane(Vector[] values, Lane descriptor)
    {
        Vector vector = laneVector(values, descriptor);
        return switch (vector) {
            case I32Vector _ -> descriptor.carrier() == FixedWidthKeyLayout.Carrier.I32;
            case I64Vector _ -> descriptor.carrier() == FixedWidthKeyLayout.Carrier.I64;
            case F64Vector _ -> descriptor.carrier() == FixedWidthKeyLayout.Carrier.F64;
            case BooleanVector _ -> descriptor.carrier() == FixedWidthKeyLayout.Carrier.BOOLEAN;
            case RegionVector region -> supportsLane(region.values(), descriptor.carrier());
            case DictionaryVector dictionary -> supportsLane(dictionary.values(), descriptor.carrier());
            case RleVector rle -> supportsLane(rle.values(), descriptor.carrier());
            default -> false;
        };
    }

    private static boolean supportsLane(Vector vector, FixedWidthKeyLayout.Carrier carrier)
    {
        return switch (vector) {
            case I32Vector _ -> carrier == FixedWidthKeyLayout.Carrier.I32;
            case I64Vector _ -> carrier == FixedWidthKeyLayout.Carrier.I64;
            case F64Vector _ -> carrier == FixedWidthKeyLayout.Carrier.F64;
            case BooleanVector _ -> carrier == FixedWidthKeyLayout.Carrier.BOOLEAN;
            case RegionVector region -> supportsLane(region.values(), carrier);
            case DictionaryVector dictionary -> supportsLane(dictionary.values(), carrier);
            case RleVector rle -> supportsLane(rle.values(), carrier);
            default -> false;
        };
    }

    private static FixedWidthKeyLayout.Carrier integerCarrier(Vector vector)
    {
        while (true) {
            vector = switch (vector) {
                case RegionVector region -> region.values();
                case DictionaryVector dictionary -> dictionary.values();
                case RleVector rle -> rle.values();
                default -> vector;
            };
            if (!(vector instanceof RegionVector || vector instanceof DictionaryVector || vector instanceof RleVector)) {
                break;
            }
        }
        return vector instanceof I32Vector ? FixedWidthKeyLayout.Carrier.I32 : FixedWidthKeyLayout.Carrier.I64;
    }

    private static Vector laneVector(Vector[] values, Lane descriptor)
    {
        Vector vector = values[descriptor.logicalKey()];
        for (String field : descriptor.fieldPath()) {
            Streams component = VectorAccess.structField(vector, field);
            if (!VectorAccess.isAllFalseNulls(component.getOrNull(Stream.NULLS))) {
                throw new IllegalArgumentException("Fixed-width key component is nullable: " + descriptor.fieldPath());
            }
            vector = component.values();
        }
        return vector;
    }
}
