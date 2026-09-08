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

import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Vector;

import java.util.ArrayList;
import java.util.List;

/**
 * One provider-neutral persistent-key layout composed from direct flat fields and canonical fixed-width lanes.
 * Logical types do not select table or generator classes.
 */
record ResolvedPersistentKeyLayout(
        Field[] fields,
        ResolvedFixedWidthKeyLayout canonicalLayout,
        int logicalKeyCount,
        int[] directFieldByLogicalKey)
{
    record Field(int logicalKey, int canonicalLane)
    {
        boolean canonical()
        {
            return canonicalLane >= 0;
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
        java.util.Arrays.fill(directFieldByLogicalKey, -1);

        boolean hasDirect = false;
        boolean hasCanonical = false;
        for (int key = 0; key < values.length; key++) {
            TypeBinding type = keyTypes.get(key);
            if (type.fixedWidthKeyLayout().isPresent()) {
                int logicalKey = key;
                ResolvedFixedWidthKeyLayout resolved = ResolvedFixedWidthKeyLayout.tryCreate(
                        List.of(type),
                        new StructuralKeyKernel[] {kernels[key]},
                        new Vector[] {values[key]});
                if (resolved == null) {
                    return null;
                }
                for (ResolvedFixedWidthKeyLayout.Lane lane : resolved.lanes()) {
                    ResolvedFixedWidthKeyLayout.Source[] sources = java.util.Arrays.stream(lane.sources())
                            .map(source -> new ResolvedFixedWidthKeyLayout.Source(
                                    logicalKey,
                                    source.fieldPath(),
                                    source.carrier()))
                            .toArray(ResolvedFixedWidthKeyLayout.Source[]::new);
                    int canonicalLane = canonicalLanes.size();
                    canonicalLanes.add(new ResolvedFixedWidthKeyLayout.Lane(logicalKey, sources, lane.projection()));
                    fields.add(new Field(logicalKey, canonicalLane));
                    hasCanonical = true;
                }
                continue;
            }

            if (!type.supportsRawKeyIdentity() || FlatTypeHandlers.forVector(values[key], type) == null) {
                return null;
            }
            directFieldByLogicalKey[key] = fields.size();
            fields.add(new Field(key, -1));
            hasDirect = true;
        }

        // Homogeneous direct layouts retain the established flat-table path, while homogeneous fixed-width
        // layouts retain the generated fixed-width table. This layout owns only their general composition.
        if (!hasDirect || !hasCanonical) {
            return null;
        }
        return new ResolvedPersistentKeyLayout(
                fields.toArray(Field[]::new),
                new ResolvedFixedWidthKeyLayout(
                        canonicalLanes.toArray(ResolvedFixedWidthKeyLayout.Lane[]::new),
                        values.length),
                values.length,
                directFieldByLogicalKey);
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
}
