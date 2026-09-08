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

import org.weakref.nitro.core.type.PersistentKeyLayout;
import org.weakref.nitro.core.type.RepeatedKeyLayout;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.RegionVector;
import org.weakref.nitro.data.RepeatedVector;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.ArrayList;
import java.util.List;

/** Resolved provider-neutral repeated identity. */
record ResolvedRepeatedKeyLayout(RepeatedKeyLayout.Order order, List<Output> outputs)
{
    enum Storage
    {
        PRESENCE,
        I32,
        I64,
        BOOLEAN,
        F64,
        BINARY,
    }

    /**
     * One generated physical contribution to every repeated tuple. Product identities expand into a presence
     * contribution plus their recursively resolved leaves. Paths and null paths are relative to the declared
     * repeated output, and therefore retain no logical type or field meaning in the engine.
     */
    record Output(
            int output,
            List<String> fieldPath,
            TypeBinding type,
            Storage storage,
            List<List<String>> nullPaths)
    {
        Output
        {
            fieldPath = List.copyOf(fieldPath);
            nullPaths = nullPaths.stream().map(List::copyOf).toList();
        }

        boolean presence()
        {
            return storage == Storage.PRESENCE;
        }
    }

    ResolvedRepeatedKeyLayout
    {
        outputs = List.copyOf(outputs);
    }

    static ResolvedRepeatedKeyLayout tryCreate(Vector value, RepeatedKeyLayout layout)
    {
        RepeatedVector repeated;
        try {
            repeated = repeated(value);
        }
        catch (IllegalArgumentException e) {
            return null;
        }
        ArrayList<Output> outputs = new ArrayList<>();
        for (RepeatedKeyLayout.Output output : layout.outputs()) {
            if (output.output() >= repeated.repeatedOutputCount()) {
                return null;
            }
            Streams child = repeated.repeatedOutput(output.output());
            if (!resolve(
                    output.output(),
                    List.of(),
                    output.type(),
                    child.values(),
                    List.of(List.of()),
                    outputs)) {
                return null;
            }
        }
        return new ResolvedRepeatedKeyLayout(layout.order(), outputs);
    }

    private static boolean resolve(
            int output,
            List<String> path,
            TypeBinding type,
            Vector value,
            List<List<String>> nullPaths,
            List<Output> outputs)
    {
        // Nested repetition and canonical projection use the same recursive algebra, but their generated emitters
        // are separate capabilities. Reject until those emitters can bind and inline the complete physical shape.
        if (type.repeatedKeyLayout().isPresent() || type.fixedWidthKeyLayout().isPresent()) {
            return false;
        }

        PersistentKeyLayout product = type.persistentKeyLayout().orElse(null);
        if (product != null) {
            outputs.add(new Output(output, path, type, Storage.PRESENCE, nullPaths));
            for (PersistentKeyLayout.Field child : product.fields()) {
                List<String> childPath = append(path, child.fieldPath());
                Vector childValue;
                try {
                    childValue = valueAtPath(value, child.fieldPath());
                }
                catch (IllegalArgumentException e) {
                    return false;
                }
                if (!resolve(
                        output,
                        childPath,
                        child.type(),
                        childValue,
                        appendNullPath(nullPaths, childPath),
                        outputs)) {
                    return false;
                }
            }
            return true;
        }

        if (!type.supportsRawKeyIdentity()) {
            return false;
        }
        FlatTypeHandler handler = FlatTypeHandlers.forVector(value, type);
        if (handler == null || handler.kind() == FlatTypeHandler.Kind.CANONICAL) {
            return false;
        }
        Storage storage = switch (OperatorVectorSupport.flatten(value)) {
            case I32Vector _ -> Storage.I32;
            case I64Vector _ -> Storage.I64;
            case BooleanVector _ -> Storage.BOOLEAN;
            case F64Vector _ -> Storage.F64;
            case BinaryVector _ -> Storage.BINARY;
            default -> null;
        };
        if (storage == null) {
            return false;
        }
        outputs.add(new Output(output, path, type, storage, nullPaths));
        return true;
    }

    private static Vector valueAtPath(Vector value, List<String> path)
    {
        Vector result = value;
        for (String field : path) {
            result = VectorAccess.structField(result, field).values();
        }
        return result;
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

    static RepeatedVector repeated(Vector vector)
    {
        return switch (vector) {
            case RepeatedVector repeated -> repeated;
            case RegionVector region -> repeated(region.values());
            case DictionaryVector dictionary -> repeated(dictionary.values());
            case RleVector rle -> repeated(rle.values());
            default -> throw new IllegalArgumentException("Expected repeated vector but found " + vector.getClass().getSimpleName());
        };
    }
}
