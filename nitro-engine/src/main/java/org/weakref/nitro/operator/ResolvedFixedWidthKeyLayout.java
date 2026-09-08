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

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** One provider-neutral fixed-width key layout resolved across the logical key columns of a consumer. */
record ResolvedFixedWidthKeyLayout(Lane[] lanes, int logicalKeyCount, boolean allowNullableComponents)
{
    record Source(int logicalKey, List<String> fieldPath, FixedWidthKeyLayout.Carrier carrier) {}

    record Lane(int logicalKey, Source[] sources, Optional<MethodHandle> projection)
    {
        Lane
        {
            sources = sources.clone();
            projection = Optional.ofNullable(projection.orElse(null));
        }
    }

    ResolvedFixedWidthKeyLayout(Lane[] lanes, int logicalKeyCount)
    {
        this(lanes, logicalKeyCount, false);
    }

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
                int logicalKey = key;
                for (FixedWidthKeyLayout.Lane lane : fixedWidth.lanes()) {
                    Source[] sources = lane.sources().stream()
                            .map(source -> new Source(logicalKey, source.fieldPath(), source.carrier()))
                            .toArray(Source[]::new);
                    for (Source source : sources) {
                        if (!supportsSource(values, source, false)) {
                            throw new IllegalArgumentException("Type provider fixed-width key source is incompatible with the admitted vector: " + source);
                        }
                    }
                    lanes.add(new Lane(key, sources, lane.projection()));
                }
                continue;
            }
            if (!kernels[key].allowsLegacyPhysicalShortcuts() || !GroupingState.isSingleLongGroupingCandidate(values[key])) {
                return null;
            }
            lanes.add(new Lane(
                    key,
                    new Source[] {new Source(key, List.of(), integerCarrier(values[key]))},
                    Optional.empty()));
        }
        if (lanes.isEmpty() || lanes.size() > AbstractFixedWidthKeyTable.MAX_ARITY) {
            throw new IllegalArgumentException("Unsupported fixed-width key lane count: " + lanes.size());
        }
        return new ResolvedFixedWidthKeyLayout(lanes.toArray(Lane[]::new), values.length);
    }

    int sourceCount()
    {
        int count = 0;
        for (Lane lane : lanes) {
            count += lane.sources().length;
        }
        return count;
    }

    Vector sourceVector(Vector[] values, int sourceIndex)
    {
        int index = sourceIndex;
        for (Lane lane : lanes) {
            if (index < lane.sources().length) {
                return sourceVector(values, lane.sources()[index], allowNullableComponents);
            }
            index -= lane.sources().length;
        }
        throw new IndexOutOfBoundsException(sourceIndex);
    }

    List<FixedWidthKeyLayout.Carrier> sourceCarriers()
    {
        ArrayList<FixedWidthKeyLayout.Carrier> carriers = new ArrayList<>();
        for (Lane lane : lanes) {
            for (Source source : lane.sources()) {
                carriers.add(source.carrier());
            }
        }
        return List.copyOf(carriers);
    }

    List<Integer> laneSourceCounts()
    {
        return java.util.Arrays.stream(lanes).map(lane -> lane.sources().length).toList();
    }

    int[] laneLogicalKeys()
    {
        return java.util.Arrays.stream(lanes).mapToInt(Lane::logicalKey).toArray();
    }

    List<Optional<MethodHandle>> projections()
    {
        return java.util.Arrays.stream(lanes).map(Lane::projection).toList();
    }

    static boolean supportsSource(Vector[] values, Source descriptor, boolean allowNullableComponents)
    {
        Vector vector = sourceVector(values, descriptor, allowNullableComponents);
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

    private static Vector sourceVector(Vector[] values, Source descriptor)
    {
        return sourceVector(values, descriptor, false);
    }

    private static Vector sourceVector(Vector[] values, Source descriptor, boolean allowNullableComponents)
    {
        Vector vector = values[descriptor.logicalKey()];
        for (String field : descriptor.fieldPath()) {
            Streams component = VectorAccess.structField(vector, field);
            if (!allowNullableComponents && !VectorAccess.isAllFalseNulls(component.getOrNull(Stream.NULLS))) {
                throw new IllegalArgumentException("Fixed-width key component is nullable: " + descriptor.fieldPath());
            }
            vector = component.values();
        }
        return vector;
    }
}
