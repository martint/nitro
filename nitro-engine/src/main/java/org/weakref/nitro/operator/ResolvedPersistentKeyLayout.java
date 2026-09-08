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
import org.weakref.nitro.core.type.PersistentKeyLayout;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** One provider-neutral persistent-key layout composed from direct, canonical, and recursive product fields. */
record ResolvedPersistentKeyLayout(
        Field[] fields,
        ResolvedFixedWidthKeyLayout canonicalLayout,
        int logicalKeyCount,
        int[] directFieldByLogicalKey)
{
    record Field(
            int logicalKey,
            List<String> fieldPath,
            TypeBinding type,
            int canonicalLane,
            boolean presence,
            List<List<String>> nullPaths)
    {
        Field
        {
            fieldPath = List.copyOf(fieldPath);
            nullPaths = nullPaths.stream().map(List::copyOf).toList();
        }

        boolean canonical()
        {
            return canonicalLane >= 0;
        }

        boolean direct()
        {
            return !presence && !canonical();
        }
    }

    ResolvedPersistentKeyLayout
    {
        fields = fields.clone();
        directFieldByLogicalKey = directFieldByLogicalKey.clone();
    }

    static ResolvedPersistentKeyLayout tryCreate(
            List<TypeBinding> keyTypes,
            StructuralKeyKernel[] kernels,
            Vector[] values)
    {
        if (keyTypes.size() != values.length || kernels.length != values.length) {
            return null;
        }

        ArrayList<Field> fields = new ArrayList<>();
        ArrayList<ResolvedFixedWidthKeyLayout.Lane> canonicalLanes = new ArrayList<>();
        int[] directFieldByLogicalKey = new int[values.length];
        Arrays.fill(directFieldByLogicalKey, -1);

        boolean hasDirect = false;
        boolean hasCanonical = false;
        boolean hasProduct = false;
        for (int key = 0; key < values.length; key++) {
            TypeBinding type = keyTypes.get(key);
            PersistentKeyLayout product = type.persistentKeyLayout().orElse(null);
            if (product != null) {
                int firstField = fields.size();
                if (!resolveProduct(
                        key,
                        List.of(),
                        type,
                        product,
                        List.of(List.of()),
                        values,
                        fields,
                        canonicalLanes)) {
                    return null;
                }
                hasProduct = true;
                for (int field = firstField; field < fields.size(); field++) {
                    hasDirect |= fields.get(field).direct();
                    hasCanonical |= fields.get(field).canonical();
                }
                continue;
            }

            FixedWidthKeyLayout fixedWidth = type.fixedWidthKeyLayout().orElse(null);
            if (fixedWidth != null) {
                if (!resolveFixedWidth(
                        key,
                        List.of(),
                        type,
                        fixedWidth,
                        List.of(List.of()),
                        values,
                        fields,
                        canonicalLanes)) {
                    return null;
                }
                hasCanonical = true;
                continue;
            }

            boolean directIdentity = type.supportsRawKeyIdentity() ||
                    (!type.isSpecified() && kernels[key].allowsLegacyPhysicalShortcuts());
            if (!directIdentity || FlatTypeHandlers.forVector(values[key], type) == null) {
                return null;
            }
            directFieldByLogicalKey[key] = fields.size();
            fields.add(new Field(key, List.of(), type, -1, false, List.of(List.of())));
            hasDirect = true;
        }

        // Homogeneous top-level direct and fixed-width layouts retain their established generated paths. Products
        // always use this composed descriptor because their presence/null boundaries are part of identity.
        if (!hasProduct && (!hasDirect || !hasCanonical)) {
            return null;
        }
        return new ResolvedPersistentKeyLayout(
                fields.toArray(Field[]::new),
                new ResolvedFixedWidthKeyLayout(canonicalLanes.toArray(ResolvedFixedWidthKeyLayout.Lane[]::new), values.length, hasProduct),
                values.length,
                directFieldByLogicalKey);
    }

    private static boolean resolveProduct(
            int logicalKey,
            List<String> path,
            TypeBinding type,
            PersistentKeyLayout product,
            List<List<String>> enclosingNullPaths,
            Vector[] values,
            List<Field> fields,
            List<ResolvedFixedWidthKeyLayout.Lane> canonicalLanes)
    {
        // A generated presence field distinguishes a null product from a non-null product whose children are null.
        fields.add(new Field(logicalKey, path, type, -1, true, enclosingNullPaths));
        for (PersistentKeyLayout.Field child : product.fields()) {
            List<String> childPath = append(path, child.fieldPath());
            List<List<String>> childNullPaths = appendNullPath(enclosingNullPaths, childPath);
            TypeBinding childType = child.type();
            PersistentKeyLayout nested = childType.persistentKeyLayout().orElse(null);
            if (nested != null) {
                if (!resolveProduct(
                        logicalKey,
                        childPath,
                        childType,
                        nested,
                        childNullPaths,
                        values,
                        fields,
                        canonicalLanes)) {
                    return false;
                }
                continue;
            }
            FixedWidthKeyLayout fixedWidth = childType.fixedWidthKeyLayout().orElse(null);
            if (fixedWidth != null) {
                if (!resolveFixedWidth(
                        logicalKey,
                        childPath,
                        childType,
                        fixedWidth,
                        childNullPaths,
                        values,
                        fields,
                        canonicalLanes)) {
                    return false;
                }
                continue;
            }
            Vector childValue;
            try {
                childValue = valueAtPath(values[logicalKey], childPath);
            }
            catch (IllegalArgumentException e) {
                return false;
            }
            if (!childType.supportsRawKeyIdentity() || FlatTypeHandlers.forVector(childValue, childType) == null) {
                return false;
            }
            fields.add(new Field(logicalKey, childPath, childType, -1, false, childNullPaths));
        }
        return true;
    }

    private static boolean resolveFixedWidth(
            int logicalKey,
            List<String> path,
            TypeBinding type,
            FixedWidthKeyLayout fixedWidth,
            List<List<String>> nullPaths,
            Vector[] values,
            List<Field> fields,
            List<ResolvedFixedWidthKeyLayout.Lane> canonicalLanes)
    {
        for (FixedWidthKeyLayout.Lane lane : fixedWidth.lanes()) {
            ResolvedFixedWidthKeyLayout.Source[] sources = lane.sources().stream()
                    .map(source -> new ResolvedFixedWidthKeyLayout.Source(
                            logicalKey,
                            append(path, source.fieldPath()),
                            source.carrier()))
                    .toArray(ResolvedFixedWidthKeyLayout.Source[]::new);
            for (ResolvedFixedWidthKeyLayout.Source source : sources) {
                if (!supportsFixedSource(values, path, source)) {
                    return false;
                }
            }
            int canonicalLane = canonicalLanes.size();
            canonicalLanes.add(new ResolvedFixedWidthKeyLayout.Lane(logicalKey, sources, lane.projection()));
            fields.add(new Field(logicalKey, path, type, canonicalLane, false, nullPaths));
        }
        return true;
    }

    private static boolean supportsFixedSource(
            Vector[] values,
            List<String> nullableLeafPath,
            ResolvedFixedWidthKeyLayout.Source source)
    {
        if (!ResolvedFixedWidthKeyLayout.supportsSource(values, source, true)) {
            return false;
        }
        Vector vector = values[source.logicalKey()];
        int depth = 0;
        for (String field : source.fieldPath()) {
            Streams component = VectorAccess.structField(vector, field);
            depth++;
            if (depth > nullableLeafPath.size() && !VectorAccess.isAllFalseNulls(component.getOrNull(Stream.NULLS))) {
                return false;
            }
            vector = component.values();
        }
        return true;
    }

    Vector[] fieldValues(Vector[] values)
    {
        Vector[] result = new Vector[fields.length];
        for (int field = 0; field < fields.length; field++) {
            Field descriptor = fields[field];
            result[field] = descriptor.direct()
                    ? valueAtPath(values[descriptor.logicalKey()], descriptor.fieldPath())
                    : values[descriptor.logicalKey()];
        }
        return result;
    }

    Vector[][] fieldNullSources(Vector[] values, Vector[] nulls)
    {
        Vector[][] allSources = fieldNullSourceVectors(values, nulls);
        Vector[][] result = new Vector[allSources.length][];
        for (int field = 0; field < fields.length; field++) {
            ArrayList<Vector> sources = new ArrayList<>();
            for (Vector source : allSources[field]) {
                if (!VectorAccess.isAllFalseNulls(source)) {
                    sources.add(source);
                }
            }
            result[field] = sources.toArray(Vector[]::new);
        }
        return result;
    }

    Vector[][] fieldNullSourceVectors(Vector[] values, Vector[] nulls)
    {
        Vector[][] result = new Vector[fields.length][];
        for (int field = 0; field < fields.length; field++) {
            Field descriptor = fields[field];
            result[field] = new Vector[descriptor.nullPaths().size()];
            for (int source = 0; source < descriptor.nullPaths().size(); source++) {
                List<String> path = descriptor.nullPaths().get(source);
                result[field][source] = path.isEmpty()
                        ? nulls != null && descriptor.logicalKey() < nulls.length ? nulls[descriptor.logicalKey()] : null
                        : streamsAtPath(values[descriptor.logicalKey()], path).getOrNull(Stream.NULLS);
            }
        }
        return result;
    }

    List<Integer> nullSourceCounts()
    {
        return Arrays.stream(fields).map(field -> field.nullPaths().size()).toList();
    }

    int[] canonicalFieldIndexes()
    {
        int[] indexes = new int[canonicalLayout.lanes().length];
        for (int field = 0; field < fields.length; field++) {
            if (fields[field].canonical()) {
                indexes[fields[field].canonicalLane()] = field;
            }
        }
        return indexes;
    }

    int[] presenceFieldIndexes()
    {
        return java.util.stream.IntStream.range(0, fields.length)
                .filter(field -> fields[field].presence())
                .toArray();
    }

    private static Vector valueAtPath(Vector value, List<String> path)
    {
        return streamsAtPath(value, path).values();
    }

    private static Streams streamsAtPath(Vector value, List<String> path)
    {
        Streams streams = Streams.ofValues(value);
        for (String field : path) {
            streams = VectorAccess.structField(streams.values(), field);
        }
        return streams;
    }

    private static List<String> append(List<String> prefix, List<String> suffix)
    {
        ArrayList<String> result = new ArrayList<>(prefix.size() + suffix.size());
        result.addAll(prefix);
        result.addAll(suffix);
        return List.copyOf(result);
    }

    private static List<List<String>> appendNullPath(List<List<String>> paths, List<String> path)
    {
        ArrayList<List<String>> result = new ArrayList<>(paths.size() + 1);
        result.addAll(paths);
        result.add(path);
        return List.copyOf(result);
    }
}
