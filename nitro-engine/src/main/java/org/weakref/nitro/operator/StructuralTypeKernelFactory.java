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

import org.weakref.nitro.core.type.BoundTypeComparison;
import org.weakref.nitro.core.type.BoundTypeIdentity;
import org.weakref.nitro.core.type.BoundTypeKey;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeKeyBinder;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.core.type.UnorderedPlacement;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.ArrayVector;
import org.weakref.nitro.data.BinaryDispatchSupport;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.MapVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.StructVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;
import org.weakref.nitro.function.scalar.PrimitiveFunction;
import org.weakref.nitro.function.scalar.ScalarFunction;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.Set;

import static java.lang.invoke.MethodHandles.collectArguments;
import static java.util.Objects.requireNonNull;

/**
 * Engine-owned bridge from registry-supplied carrier operations to structural vector kernels.
 *
 * <p>Provider-private carrier classes are erased behind adapted method handles. Operators retain only the resulting
 * Nitro-vector kernel and never inspect a logical type identity, carrier class, or concrete vector representation.
 */
public final class StructuralTypeKernelFactory
{
    /**
     * Binds hashing and identity supplied by a type registry to its admitted vector shapes.
     *
     * <p>The returned binder can bind independently owned vectors and compare positions across them. Stateful
     * registry implementations can therefore index retained values without recognizing logical types or physical
     * vector classes.
     */
    public TypeKeyBinder bindKey(TypeBinding type)
    {
        StructuralKeyKernel kernel = key(requireNonNull(type, "type is null"));
        return values -> new PublicBoundKey(kernel, kernel.bind(values));
    }

    /**
     * Binds the logical identity supplied by a type registry to its admitted vector shapes.
     *
     * <p>Structural recursion remains engine-owned. A registry-bound function can use the returned operation without
     * recognizing either the logical type or its physical vector representation.
     */
    public BoundTypeIdentity bindIdentity(TypeBinding type)
    {
        return identity(requireNonNull(type, "type is null"));
    }

    private record PublicBoundKey(StructuralKeyKernel kernel, StructuralKeyKernel.Bound key)
            implements BoundTypeKey
    {
        private PublicBoundKey
        {
            requireNonNull(kernel, "kernel is null");
            requireNonNull(key, "key is null");
        }

        @Override
        public long hash(int position)
        {
            return key.hash(position);
        }

        @Override
        public boolean identical(int position, BoundTypeKey other, int otherPosition)
        {
            if (!(other instanceof PublicBoundKey right) || kernel != right.kernel) {
                throw new IllegalArgumentException("bound key was created by a different type binder");
            }
            return key.identical(position, right.key, otherPosition);
        }
    }

    /**
     * Binds the logical ordering supplied by a type registry to its admitted vector shapes.
     *
     * <p>Structural recursion remains engine-owned; consumers receive one opaque operation and do
     * not need to recognize logical or physical type identities.
     */
    public BoundTypeComparison bindComparison(TypeBinding type)
    {
        return bindComparison(type, UnorderedPlacement.LAST);
    }

    /**
     * Binds the requested unordered-value placement through scalar or recursively composed values.
     */
    public BoundTypeComparison bindComparison(TypeBinding type, UnorderedPlacement placement)
    {
        return comparison(
                requireNonNull(type, "type is null"),
                requireNonNull(placement, "placement is null"));
    }

    /**
     * Builds the general null-safe equality function for a registry-bound logical type.
     *
     * <p>The returned function knows no concrete logical type. Its leaf semantics and recursive shape come entirely
     * from the supplied binding, while this factory supplies the common mask, null, and output-vector protocol.
     */
    public PrimitiveFunction identicalFunction(TypeBinding type)
    {
        return new StructuralIdenticalFunction(identity(requireNonNull(type, "type is null")));
    }

    StructuralIdentityKernel identity(TypeBinding type)
    {
        requireNonNull(type, "type is null");
        TypeOperators operators = requireNonNull(type.operators(), "type operators are null");
        if (operators.vectorIdentical().isPresent()) {
            return new DirectStructuralIdentityKernel(type, operators.vectorIdentical().orElseThrow());
        }
        boolean hasValueRead = operators.valueRead().isPresent();
        boolean hasIdentical = operators.identical().isPresent();
        if (!hasValueRead && !hasIdentical) {
            if (type.supportedVectorTypes().contains(StructVector.class)) {
                return new StructStructuralIdentityKernel(type.nestedValueTypes().stream()
                        .map(this::identity)
                        .toArray(StructuralIdentityKernel[]::new));
            }
            if (type.supportedVectorTypes().contains(ArrayVector.class)) {
                return new ArrayStructuralIdentityKernel(identity(requireChildBindings(type, 1)[0]));
            }
            if (type.supportedVectorTypes().contains(MapVector.class)) {
                TypeBinding[] children = requireChildBindings(type, 2);
                return new MapStructuralIdentityKernel(identity(children[0]), identity(children[1]));
            }
            return LegacyStructuralIdentityKernel.INSTANCE;
        }
        if (!hasValueRead || !hasIdentical) {
            throw new IllegalArgumentException("Type %s must provide valueRead and identical together"
                    .formatted(type.identity()));
        }
        return new BoundStructuralIdentityKernel(
                type,
                operators.valueRead().orElseThrow(),
                operators.identical().orElseThrow());
    }

    @ScalarFunction(name = "structural_identical")
    private static final class StructuralIdenticalFunction
            implements PrimitiveFunction
    {
        private static final String ALLOCATION_CONTEXT = "StructuralIdentical";

        private final StructuralIdentityKernel identity;
        private final Allocator.Context allocationContext = new Allocator.Context(ALLOCATION_CONTEXT);

        private StructuralIdenticalFunction(StructuralIdentityKernel identity)
        {
            this.identity = requireNonNull(identity, "identity is null");
        }

        @Override
        public Set<Allocator.Context> allocationContexts()
        {
            return Set.of(allocationContext);
        }

        @Override
        public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
        {
            return PrimitiveFunction.valuesAndNullsWhenRequested(requestedOutputStreams);
        }

        @Override
        public Streams apply(
                List<Streams> inputs,
                Mask mask,
                Set<Stream> requestedStreams,
                Streams output,
                PrimitiveExecutionContext context)
        {
            if (inputs.size() != 2) {
                throw new IllegalArgumentException("Null-safe equality requires two arguments");
            }
            if (!requestedStreams.contains(Stream.VALUES)) {
                return Streams.empty();
            }

            Streams left = inputs.get(0);
            Streams right = inputs.get(1);
            Vector leftValues = left.values();
            Vector rightValues = right.values();
            Vector leftNulls = left.getOrNull(Stream.NULLS);
            Vector rightNulls = right.getOrNull(Stream.NULLS);
            BooleanVector values = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    context.allocationContext(ALLOCATION_CONTEXT),
                    output != null ? output.getOrNull(Stream.VALUES) : null,
                    BinaryDispatchSupport.requiredLength(mask, Math.max(leftValues.length(), rightValues.length())));
            boolean[] result = values.values();
            for (int position : mask) {
                boolean leftNull = OperatorVectorSupport.isNull(leftNulls, position);
                boolean rightNull = OperatorVectorSupport.isNull(rightNulls, position);
                result[position] = leftNull == rightNull &&
                        (leftNull || identity.identical(
                                leftValues, leftNulls, position,
                                rightValues, rightNulls, position));
            }
            return Streams.ofValues(values);
        }
    }

    StructuralKeyKernel key(TypeBinding type)
    {
        requireNonNull(type, "type is null");
        if (type.supportsRawKeyIdentity()) {
            return LegacyStructuralKeyKernel.INSTANCE;
        }
        TypeOperators operators = requireNonNull(type.operators(), "type operators are null");
        boolean hasVectorHash = operators.vectorHash().isPresent();
        boolean hasVectorIdentical = operators.vectorIdentical().isPresent();
        if (hasVectorHash) {
            if (!hasVectorIdentical) {
                throw new IllegalArgumentException("Type %s must provide vectorHash and vectorIdentical together"
                        .formatted(type.identity()));
            }
            return new DirectStructuralKeyKernel(
                    type,
                    operators.vectorHash().orElseThrow(),
                    operators.vectorIdentical().orElseThrow(),
                    type.keyBinder().orElse(null));
        }
        boolean hasValueRead = operators.valueRead().isPresent();
        boolean hasHash = operators.hash().isPresent();
        boolean hasIdentical = operators.identical().isPresent();
        if (!hasValueRead && !hasHash && !hasIdentical) {
            if (type.fixedWidthKeyLayout().isPresent()) {
                return new FixedWidthLayoutOnlyKeyKernel(type.identity().toString());
            }
            if (type.supportedVectorTypes().contains(StructVector.class)) {
                return new StructStructuralKeyKernel(type.nestedValueTypes().stream()
                        .map(this::key)
                        .toArray(StructuralKeyKernel[]::new));
            }
            if (type.supportedVectorTypes().contains(ArrayVector.class)) {
                return new ArrayStructuralKeyKernel(key(requireChildBindings(type, 1)[0]));
            }
            if (type.supportedVectorTypes().contains(MapVector.class)) {
                TypeBinding[] children = requireChildBindings(type, 2);
                return new MapStructuralKeyKernel(key(children[0]), key(children[1]));
            }
            return LegacyStructuralKeyKernel.INSTANCE;
        }
        if (!hasValueRead || !hasHash || !hasIdentical) {
            throw new IllegalArgumentException("Type %s must provide valueRead, hash, and identical together"
                    .formatted(type.identity()));
        }
        return new BoundStructuralKeyKernel(
                type,
                operators.valueRead().orElseThrow(),
                operators.hash().orElseThrow(),
                operators.identical().orElseThrow());
    }

    private TypeBinding[] requireChildBindings(TypeBinding type, int expected)
    {
        if (type.nestedValueTypes().size() != expected) {
            throw new IllegalArgumentException("Type %s has %s child bindings; expected %s"
                    .formatted(type.identity(), type.nestedValueTypes().size(), expected));
        }
        return type.nestedValueTypes().toArray(TypeBinding[]::new);
    }

    private static final class ArrayStructuralIdentityKernel
            implements StructuralIdentityKernel
    {
        private final StructuralIdentityKernel elements;

        private ArrayStructuralIdentityKernel(StructuralIdentityKernel elements)
        {
            this.elements = requireNonNull(elements, "elements is null");
        }

        @Override
        public boolean identical(
                Vector leftValues,
                Vector leftNulls,
                int leftPosition,
                Vector rightValues,
                Vector rightNulls,
                int rightPosition)
        {
            ArrayStructuralKeyKernel.ArrayPosition left = ArrayStructuralKeyKernel.arrayPosition(leftValues, leftPosition);
            ArrayStructuralKeyKernel.ArrayPosition right = ArrayStructuralKeyKernel.arrayPosition(rightValues, rightPosition);
            int length = left.values().length(left.position());
            if (length != right.values().length(right.position())) {
                return false;
            }

            Streams leftElements = left.values().elements();
            Streams rightElements = right.values().elements();
            Vector leftElementNulls = leftElements.getOrNull(Stream.NULLS);
            Vector rightElementNulls = rightElements.getOrNull(Stream.NULLS);
            int leftOffset = left.values().startOffset(left.position());
            int rightOffset = right.values().startOffset(right.position());
            for (int index = 0; index < length; index++) {
                int leftElement = leftOffset + index;
                int rightElement = rightOffset + index;
                boolean leftNull = OperatorVectorSupport.isNull(leftElementNulls, leftElement);
                boolean rightNull = OperatorVectorSupport.isNull(rightElementNulls, rightElement);
                if (leftNull || rightNull) {
                    if (leftNull != rightNull) {
                        return false;
                    }
                    continue;
                }
                if (!elements.identical(
                        leftElements.values(), leftElementNulls, leftElement,
                        rightElements.values(), rightElementNulls, rightElement)) {
                    return false;
                }
            }
            return true;
        }
    }

    private static final class MapStructuralIdentityKernel
            implements StructuralIdentityKernel
    {
        private final StructuralIdentityKernel keys;
        private final StructuralIdentityKernel values;

        private MapStructuralIdentityKernel(StructuralIdentityKernel keys, StructuralIdentityKernel values)
        {
            this.keys = requireNonNull(keys, "keys is null");
            this.values = requireNonNull(values, "values is null");
        }

        @Override
        public boolean identical(
                Vector leftValues,
                Vector leftNulls,
                int leftPosition,
                Vector rightValues,
                Vector rightNulls,
                int rightPosition)
        {
            MapStructuralKeyKernel.MapPosition left = MapStructuralKeyKernel.mapPosition(leftValues, leftPosition);
            MapStructuralKeyKernel.MapPosition right = MapStructuralKeyKernel.mapPosition(rightValues, rightPosition);
            int length = left.values().length(left.position());
            if (length != right.values().length(right.position())) {
                return false;
            }

            Streams leftKeys = left.values().keys();
            Streams rightKeys = right.values().keys();
            Streams leftMapValues = left.values().values();
            Streams rightMapValues = right.values().values();
            Vector leftKeyNulls = leftKeys.getOrNull(Stream.NULLS);
            Vector rightKeyNulls = rightKeys.getOrNull(Stream.NULLS);
            Vector leftValueNulls = leftMapValues.getOrNull(Stream.NULLS);
            Vector rightValueNulls = rightMapValues.getOrNull(Stream.NULLS);
            int rightStart = right.values().startOffset(right.position());
            int rightEnd = right.values().endOffset(right.position());
            for (int leftEntry = left.values().startOffset(left.position());
                    leftEntry < left.values().endOffset(left.position());
                    leftEntry++) {
                boolean found = false;
                for (int rightEntry = rightStart; rightEntry < rightEnd; rightEntry++) {
                    if (!identicalValue(keys, leftKeys, leftKeyNulls, leftEntry, rightKeys, rightKeyNulls, rightEntry)) {
                        continue;
                    }
                    if (!identicalValue(values, leftMapValues, leftValueNulls, leftEntry, rightMapValues, rightValueNulls, rightEntry)) {
                        return false;
                    }
                    found = true;
                    break;
                }
                if (!found) {
                    return false;
                }
            }
            return true;
        }

        private static boolean identicalValue(
                StructuralIdentityKernel kernel,
                Streams left,
                Vector leftNulls,
                int leftPosition,
                Streams right,
                Vector rightNulls,
                int rightPosition)
        {
            boolean leftNull = OperatorVectorSupport.isNull(leftNulls, leftPosition);
            boolean rightNull = OperatorVectorSupport.isNull(rightNulls, rightPosition);
            if (leftNull || rightNull) {
                return leftNull == rightNull;
            }
            return kernel.identical(
                    left.values(), leftNulls, leftPosition,
                    right.values(), rightNulls, rightPosition);
        }
    }

    private static final class StructStructuralIdentityKernel
            implements StructuralIdentityKernel
    {
        private final StructuralIdentityKernel[] fields;

        private StructStructuralIdentityKernel(StructuralIdentityKernel[] fields)
        {
            this.fields = fields.clone();
        }

        @Override
        public boolean identical(
                Vector leftValues,
                Vector leftNulls,
                int leftPosition,
                Vector rightValues,
                Vector rightNulls,
                int rightPosition)
        {
            StructStructuralKeyKernel.StructPosition left = StructStructuralKeyKernel.structPosition(leftValues, leftPosition);
            StructStructuralKeyKernel.StructPosition right = StructStructuralKeyKernel.structPosition(rightValues, rightPosition);
            StructStructuralKeyKernel.requireFieldCount(left.values(), fields.length);
            StructStructuralKeyKernel.requireFieldCount(right.values(), fields.length);
            for (int field = 0; field < fields.length; field++) {
                Streams leftField = left.values().field(field);
                Streams rightField = right.values().field(field);
                Vector leftFieldNulls = leftField.getOrNull(Stream.NULLS);
                Vector rightFieldNulls = rightField.getOrNull(Stream.NULLS);
                boolean leftNull = OperatorVectorSupport.isNull(leftFieldNulls, left.position());
                boolean rightNull = OperatorVectorSupport.isNull(rightFieldNulls, right.position());
                if (leftNull || rightNull) {
                    if (leftNull != rightNull) {
                        return false;
                    }
                    continue;
                }
                if (!fields[field].identical(
                        leftField.values(), leftFieldNulls, left.position(),
                        rightField.values(), rightFieldNulls, right.position())) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Ordered structural semantics for Nitro's physical repeated-value shape.
     */
    private static final class ArrayStructuralKeyKernel
            implements StructuralKeyKernel
    {
        private static final int NULL_HASH = 0x9E37_79B9;

        private final StructuralKeyKernel elements;

        private ArrayStructuralKeyKernel(StructuralKeyKernel elements)
        {
            this.elements = requireNonNull(elements, "elements is null");
        }

        @Override
        public long hash(Vector values, Vector nulls, int position)
        {
            ArrayPosition array = arrayPosition(values, position);
            Streams elementStreams = array.values().elements();
            Vector elementNulls = elementStreams.getOrNull(Stream.NULLS);
            int hash = 1;
            for (int element = array.values().startOffset(array.position());
                    element < array.values().endOffset(array.position());
                    element++) {
                int elementHash = OperatorVectorSupport.isNull(elementNulls, element)
                        ? NULL_HASH
                        : Long.hashCode(elements.hash(elementStreams.values(), elementNulls, element));
                hash = 31 * hash + elementHash;
            }
            return hash;
        }

        @Override
        public boolean identical(
                Vector leftValues,
                Vector leftNulls,
                int leftPosition,
                Vector rightValues,
                Vector rightNulls,
                int rightPosition)
        {
            ArrayPosition left = arrayPosition(leftValues, leftPosition);
            ArrayPosition right = arrayPosition(rightValues, rightPosition);
            int length = left.values().length(left.position());
            if (length != right.values().length(right.position())) {
                return false;
            }

            Streams leftElements = left.values().elements();
            Streams rightElements = right.values().elements();
            Vector leftElementNulls = leftElements.getOrNull(Stream.NULLS);
            Vector rightElementNulls = rightElements.getOrNull(Stream.NULLS);
            int leftOffset = left.values().startOffset(left.position());
            int rightOffset = right.values().startOffset(right.position());
            for (int index = 0; index < length; index++) {
                int leftElement = leftOffset + index;
                int rightElement = rightOffset + index;
                boolean leftNull = OperatorVectorSupport.isNull(leftElementNulls, leftElement);
                boolean rightNull = OperatorVectorSupport.isNull(rightElementNulls, rightElement);
                if (leftNull || rightNull) {
                    if (leftNull != rightNull) {
                        return false;
                    }
                    continue;
                }
                if (!elements.identical(
                        leftElements.values(), leftElementNulls, leftElement,
                        rightElements.values(), rightElementNulls, rightElement)) {
                    return false;
                }
            }
            return true;
        }

        private static ArrayPosition arrayPosition(Vector values, int position)
        {
            return switch (values) {
                case ArrayVector array -> new ArrayPosition(array, position);
                case DictionaryVector dictionary -> arrayPosition(dictionary.values(), dictionary.ids()[position]);
                case RleVector rle -> arrayPosition(rle.values(), OperatorVectorSupport.runIndex(rle, position));
                default -> throw new IllegalArgumentException(
                        "Expected array vector but found " + values.getClass().getSimpleName());
            };
        }

        private record ArrayPosition(ArrayVector values, int position) {}
    }

    /**
     * Order-independent structural semantics for Nitro's physical key/value repeated shape.
     */
    private static final class MapStructuralKeyKernel
            implements StructuralKeyKernel
    {
        private static final long NULL_HASH = 0x9E37_79B9L;

        private final StructuralKeyKernel keys;
        private final StructuralKeyKernel values;

        private MapStructuralKeyKernel(StructuralKeyKernel keys, StructuralKeyKernel values)
        {
            this.keys = requireNonNull(keys, "keys is null");
            this.values = requireNonNull(values, "values is null");
        }

        @Override
        public long hash(Vector valueVector, Vector nulls, int position)
        {
            MapPosition map = mapPosition(valueVector, position);
            Streams keyStreams = map.values().keys();
            Streams valueStreams = map.values().values();
            Vector keyNulls = keyStreams.getOrNull(Stream.NULLS);
            Vector valueNulls = valueStreams.getOrNull(Stream.NULLS);
            long hash = 0;
            for (int entry = map.values().startOffset(map.position());
                    entry < map.values().endOffset(map.position());
                    entry++) {
                long keyHash = OperatorVectorSupport.isNull(keyNulls, entry)
                        ? NULL_HASH
                        : keys.hash(keyStreams.values(), keyNulls, entry);
                long valueHash = OperatorVectorSupport.isNull(valueNulls, entry)
                        ? NULL_HASH
                        : values.hash(valueStreams.values(), valueNulls, entry);
                hash += mix64(keyHash) ^ Long.rotateLeft(mix64(valueHash), 23);
            }
            return hash;
        }

        @Override
        public boolean identical(
                Vector leftValues,
                Vector leftNulls,
                int leftPosition,
                Vector rightValues,
                Vector rightNulls,
                int rightPosition)
        {
            MapPosition left = mapPosition(leftValues, leftPosition);
            MapPosition right = mapPosition(rightValues, rightPosition);
            int length = left.values().length(left.position());
            if (length != right.values().length(right.position())) {
                return false;
            }

            Streams leftKeys = left.values().keys();
            Streams rightKeys = right.values().keys();
            Streams leftMapValues = left.values().values();
            Streams rightMapValues = right.values().values();
            Vector leftKeyNulls = leftKeys.getOrNull(Stream.NULLS);
            Vector rightKeyNulls = rightKeys.getOrNull(Stream.NULLS);
            Vector leftValueNulls = leftMapValues.getOrNull(Stream.NULLS);
            Vector rightValueNulls = rightMapValues.getOrNull(Stream.NULLS);
            int rightStart = right.values().startOffset(right.position());
            int rightEnd = right.values().endOffset(right.position());
            for (int leftEntry = left.values().startOffset(left.position());
                    leftEntry < left.values().endOffset(left.position());
                    leftEntry++) {
                boolean found = false;
                for (int rightEntry = rightStart; rightEntry < rightEnd; rightEntry++) {
                    if (!identicalValue(keys, leftKeys, leftKeyNulls, leftEntry, rightKeys, rightKeyNulls, rightEntry)) {
                        continue;
                    }
                    if (!identicalValue(values, leftMapValues, leftValueNulls, leftEntry, rightMapValues, rightValueNulls, rightEntry)) {
                        return false;
                    }
                    found = true;
                    break;
                }
                if (!found) {
                    return false;
                }
            }
            return true;
        }

        private static boolean identicalValue(
                StructuralKeyKernel kernel,
                Streams left,
                Vector leftNulls,
                int leftPosition,
                Streams right,
                Vector rightNulls,
                int rightPosition)
        {
            boolean leftNull = OperatorVectorSupport.isNull(leftNulls, leftPosition);
            boolean rightNull = OperatorVectorSupport.isNull(rightNulls, rightPosition);
            if (leftNull || rightNull) {
                return leftNull == rightNull;
            }
            return kernel.identical(
                    left.values(), leftNulls, leftPosition,
                    right.values(), rightNulls, rightPosition);
        }

        private static MapPosition mapPosition(Vector values, int position)
        {
            return switch (values) {
                case MapVector map -> new MapPosition(map, position);
                case DictionaryVector dictionary -> mapPosition(dictionary.values(), dictionary.ids()[position]);
                case RleVector rle -> mapPosition(rle.values(), OperatorVectorSupport.runIndex(rle, position));
                default -> throw new IllegalArgumentException(
                        "Expected map vector but found " + values.getClass().getSimpleName());
            };
        }

        private static long mix64(long value)
        {
            value = (value ^ (value >>> 33)) * 0xff51afd7ed558ccdL;
            value = (value ^ (value >>> 33)) * 0xc4ceb9fe1a85ec53L;
            return value ^ (value >>> 33);
        }

        private record MapPosition(MapVector values, int position) {}
    }

    /**
     * Structural semantics derived from a provider's ordered child bindings and Nitro's physical struct shape.
     * The engine does not inspect the provider's logical type identity or carrier class.
     */
    private static final class StructStructuralKeyKernel
            implements StructuralKeyKernel
    {
        private static final int NULL_HASH = 0x9E37_79B9;

        private final StructuralKeyKernel[] fields;

        private StructStructuralKeyKernel(StructuralKeyKernel[] fields)
        {
            this.fields = fields.clone();
        }

        @Override
        public long hash(Vector values, Vector nulls, int position)
        {
            StructPosition row = structPosition(values, position);
            requireFieldCount(row.values(), fields.length);
            int hash = 1;
            for (int field = 0; field < fields.length; field++) {
                Streams streams = row.values().field(field);
                int fieldHash = OperatorVectorSupport.isNull(streams.getOrNull(Stream.NULLS), row.position())
                        ? NULL_HASH
                        : Long.hashCode(fields[field].hash(
                                streams.values(), streams.getOrNull(Stream.NULLS), row.position()));
                hash = 31 * hash + fieldHash;
            }
            return hash;
        }

        @Override
        public boolean identical(
                Vector leftValues,
                Vector leftNulls,
                int leftPosition,
                Vector rightValues,
                Vector rightNulls,
                int rightPosition)
        {
            StructPosition left = structPosition(leftValues, leftPosition);
            StructPosition right = structPosition(rightValues, rightPosition);
            requireFieldCount(left.values(), fields.length);
            requireFieldCount(right.values(), fields.length);
            for (int field = 0; field < fields.length; field++) {
                Streams leftField = left.values().field(field);
                Streams rightField = right.values().field(field);
                Vector leftFieldNulls = leftField.getOrNull(Stream.NULLS);
                Vector rightFieldNulls = rightField.getOrNull(Stream.NULLS);
                boolean leftNull = OperatorVectorSupport.isNull(leftFieldNulls, left.position());
                boolean rightNull = OperatorVectorSupport.isNull(rightFieldNulls, right.position());
                if (leftNull || rightNull) {
                    if (leftNull != rightNull) {
                        return false;
                    }
                    continue;
                }
                if (!fields[field].identical(
                        leftField.values(),
                        leftFieldNulls,
                        left.position(),
                        rightField.values(),
                        rightFieldNulls,
                        right.position())) {
                    return false;
                }
            }
            return true;
        }

        private static StructPosition structPosition(Vector values, int position)
        {
            return switch (values) {
                case StructVector struct -> new StructPosition(struct, position);
                case DictionaryVector dictionary -> structPosition(dictionary.values(), dictionary.ids()[position]);
                case RleVector rle -> structPosition(rle.values(), OperatorVectorSupport.runIndex(rle, position));
                default -> throw new IllegalArgumentException(
                        "Expected struct vector but found " + values.getClass().getSimpleName());
            };
        }

        private static void requireFieldCount(StructVector values, int expected)
        {
            if (values.fields().size() != expected) {
                throw new IllegalArgumentException("Struct field count does not match logical child binding count");
            }
        }

        private record StructPosition(StructVector values, int position) {}
    }

    private static final class BoundStructuralIdentityKernel
            implements StructuralIdentityKernel
    {
        private static final MethodType STRUCTURAL_IDENTICAL_TYPE =
                MethodType.methodType(boolean.class, Vector.class, int.class, Vector.class, int.class);

        private final MethodHandle identical;

        private BoundStructuralIdentityKernel(TypeBinding type, MethodHandle valueRead, MethodHandle identical)
        {
            Class<?> carrier = type.carrierType();
            BoundStructuralComparisonKernel.requireValueReadType(type, valueRead, carrier);
            BoundStructuralComparisonKernel.requireType(
                    type, "identical", identical, MethodType.methodType(boolean.class, carrier, carrier));
            MethodHandle structuralRead =
                    valueRead.asType(MethodType.methodType(carrier, Vector.class, int.class));
            this.identical = BoundStructuralComparisonKernel.bindBinary(
                    structuralRead, identical, STRUCTURAL_IDENTICAL_TYPE);
        }

        @Override
        public boolean identical(
                Vector leftValues,
                Vector leftNulls,
                int leftPosition,
                Vector rightValues,
                Vector rightNulls,
                int rightPosition)
        {
            if (OperatorVectorSupport.isNull(leftNulls, leftPosition) ||
                    OperatorVectorSupport.isNull(rightNulls, rightPosition)) {
                return false;
            }
            try {
                return (boolean) identical.invokeExact(leftValues, leftPosition, rightValues, rightPosition);
            }
            catch (Throwable throwable) {
                throw new IllegalStateException("Structural identity comparison failed", throwable);
            }
        }
    }

    private static final class DirectStructuralIdentityKernel
            implements StructuralIdentityKernel
    {
        private static final MethodType TYPE =
                MethodType.methodType(boolean.class, Vector.class, int.class, Vector.class, int.class);

        private final MethodHandle identical;

        private DirectStructuralIdentityKernel(TypeBinding type, MethodHandle identical)
        {
            this.identical = requireDirectType(type, "vectorIdentical", identical, TYPE);
        }

        @Override
        public boolean identical(
                Vector leftValues,
                Vector leftNulls,
                int leftPosition,
                Vector rightValues,
                Vector rightNulls,
                int rightPosition)
        {
            if (OperatorVectorSupport.isNull(leftNulls, leftPosition) ||
                    OperatorVectorSupport.isNull(rightNulls, rightPosition)) {
                return false;
            }
            try {
                return (boolean) identical.invokeExact(leftValues, leftPosition, rightValues, rightPosition);
            }
            catch (Throwable throwable) {
                throw new IllegalStateException("Direct structural identity comparison failed", throwable);
            }
        }
    }

    StructuralComparisonKernel comparison(TypeBinding type)
    {
        return comparison(type, UnorderedPlacement.LAST);
    }

    StructuralComparisonKernel comparison(TypeBinding type, UnorderedPlacement placement)
    {
        requireNonNull(type, "type is null");
        requireNonNull(placement, "placement is null");
        boolean nestedNullsFirst = placement == UnorderedPlacement.FIRST;
        TypeOperators operators = requireNonNull(type.operators(), "type operators are null");
        boolean hasVectorComparison = operators.vectorComparison(placement).isPresent();
        boolean hasVectorIdentical = operators.vectorIdentical().isPresent();
        if (hasVectorComparison) {
            if (!hasVectorIdentical) {
                throw new IllegalArgumentException("Type %s must provide vectorComparison and vectorIdentical together"
                        .formatted(type.identity()));
            }
            return new DirectStructuralComparisonKernel(
                    type,
                    operators.vectorComparison(placement).orElseThrow(),
                    operators.vectorIdentical().orElseThrow());
        }
        boolean hasValueRead = operators.valueRead().isPresent();
        boolean hasComparison = operators.comparison(placement).isPresent();
        boolean hasIdentical = operators.identical().isPresent();
        if (!hasValueRead && !hasComparison && !hasIdentical) {
            if (type.supportedVectorTypes().contains(StructVector.class)) {
                return new StructStructuralComparisonKernel(
                        type.nestedValueTypes().stream()
                                .map(child -> comparison(child, placement))
                                .toArray(StructuralComparisonKernel[]::new),
                        nestedNullsFirst);
            }
            if (type.supportedVectorTypes().contains(ArrayVector.class)) {
                TypeBinding[] children = type.nestedValueTypes().toArray(TypeBinding[]::new);
                if (children.length != 1) {
                    throw new IllegalArgumentException("Type %s has %s child bindings; expected 1"
                            .formatted(type.identity(), children.length));
                }
                return new ArrayStructuralComparisonKernel(
                        comparison(children[0], placement), nestedNullsFirst);
            }
            return placement == UnorderedPlacement.FIRST
                    ? LegacyStructuralComparisonKernel.UNORDERED_FIRST
                    : LegacyStructuralComparisonKernel.UNORDERED_LAST;
        }
        if (!hasValueRead || !hasComparison || !hasIdentical) {
            throw new IllegalArgumentException("Type %s must provide valueRead, comparison, and identical together"
                    .formatted(type.identity()));
        }
        return new BoundStructuralComparisonKernel(
                type,
                operators.valueRead().orElseThrow(),
                operators.comparison(placement).orElseThrow(),
                operators.identical().orElseThrow());
    }

    private static final class StructStructuralComparisonKernel
            implements StructuralComparisonKernel
    {
        private final StructuralComparisonKernel[] fields;
        private final boolean nullsFirst;

        private StructStructuralComparisonKernel(StructuralComparisonKernel[] fields, boolean nullsFirst)
        {
            this.fields = fields.clone();
            this.nullsFirst = nullsFirst;
        }

        @Override
        public int compare(
                Vector leftValues,
                Vector leftNulls,
                int leftPosition,
                Vector rightValues,
                Vector rightNulls,
                int rightPosition)
        {
            StructStructuralKeyKernel.StructPosition left = StructStructuralKeyKernel.structPosition(leftValues, leftPosition);
            StructStructuralKeyKernel.StructPosition right = StructStructuralKeyKernel.structPosition(rightValues, rightPosition);
            StructStructuralKeyKernel.requireFieldCount(left.values(), fields.length);
            StructStructuralKeyKernel.requireFieldCount(right.values(), fields.length);
            for (int field = 0; field < fields.length; field++) {
                Streams leftField = left.values().field(field);
                Streams rightField = right.values().field(field);
                Vector leftFieldNulls = leftField.getOrNull(Stream.NULLS);
                Vector rightFieldNulls = rightField.getOrNull(Stream.NULLS);
                boolean leftNull = OperatorVectorSupport.isNull(leftFieldNulls, left.position());
                boolean rightNull = OperatorVectorSupport.isNull(rightFieldNulls, right.position());
                if (leftNull || rightNull) {
                    int comparison = compareNulls(leftNull, rightNull, nullsFirst);
                    if (comparison != 0) {
                        return comparison;
                    }
                    continue;
                }
                int comparison = fields[field].compare(
                        leftField.values(), leftFieldNulls, left.position(),
                        rightField.values(), rightFieldNulls, right.position());
                if (comparison != 0) {
                    return comparison;
                }
            }
            return 0;
        }

        @Override
        public boolean identical(
                Vector leftValues,
                Vector leftNulls,
                int leftPosition,
                Vector rightValues,
                Vector rightNulls,
                int rightPosition)
        {
            StructStructuralKeyKernel.StructPosition left = StructStructuralKeyKernel.structPosition(leftValues, leftPosition);
            StructStructuralKeyKernel.StructPosition right = StructStructuralKeyKernel.structPosition(rightValues, rightPosition);
            StructStructuralKeyKernel.requireFieldCount(left.values(), fields.length);
            StructStructuralKeyKernel.requireFieldCount(right.values(), fields.length);
            for (int field = 0; field < fields.length; field++) {
                Streams leftField = left.values().field(field);
                Streams rightField = right.values().field(field);
                Vector leftFieldNulls = leftField.getOrNull(Stream.NULLS);
                Vector rightFieldNulls = rightField.getOrNull(Stream.NULLS);
                boolean leftNull = OperatorVectorSupport.isNull(leftFieldNulls, left.position());
                boolean rightNull = OperatorVectorSupport.isNull(rightFieldNulls, right.position());
                if (leftNull || rightNull) {
                    if (leftNull != rightNull) {
                        return false;
                    }
                    continue;
                }
                if (!fields[field].identical(
                        leftField.values(), leftFieldNulls, left.position(),
                        rightField.values(), rightFieldNulls, right.position())) {
                    return false;
                }
            }
            return true;
        }
    }

    private static final class ArrayStructuralComparisonKernel
            implements StructuralComparisonKernel
    {
        private final StructuralComparisonKernel elements;
        private final boolean nullsFirst;

        private ArrayStructuralComparisonKernel(StructuralComparisonKernel elements, boolean nullsFirst)
        {
            this.elements = requireNonNull(elements, "elements is null");
            this.nullsFirst = nullsFirst;
        }

        @Override
        public int compare(
                Vector leftValues,
                Vector leftNulls,
                int leftPosition,
                Vector rightValues,
                Vector rightNulls,
                int rightPosition)
        {
            ArrayStructuralKeyKernel.ArrayPosition left = ArrayStructuralKeyKernel.arrayPosition(leftValues, leftPosition);
            ArrayStructuralKeyKernel.ArrayPosition right = ArrayStructuralKeyKernel.arrayPosition(rightValues, rightPosition);
            Streams leftElements = left.values().elements();
            Streams rightElements = right.values().elements();
            Vector leftElementNulls = leftElements.getOrNull(Stream.NULLS);
            Vector rightElementNulls = rightElements.getOrNull(Stream.NULLS);
            int leftOffset = left.values().startOffset(left.position());
            int rightOffset = right.values().startOffset(right.position());
            int length = Math.min(left.values().length(left.position()), right.values().length(right.position()));
            for (int index = 0; index < length; index++) {
                int leftElement = leftOffset + index;
                int rightElement = rightOffset + index;
                boolean leftNull = OperatorVectorSupport.isNull(leftElementNulls, leftElement);
                boolean rightNull = OperatorVectorSupport.isNull(rightElementNulls, rightElement);
                if (leftNull || rightNull) {
                    int comparison = compareNulls(leftNull, rightNull, nullsFirst);
                    if (comparison != 0) {
                        return comparison;
                    }
                    continue;
                }
                int comparison = elements.compare(
                        leftElements.values(), leftElementNulls, leftElement,
                        rightElements.values(), rightElementNulls, rightElement);
                if (comparison != 0) {
                    return comparison;
                }
            }
            return Integer.compare(left.values().length(left.position()), right.values().length(right.position()));
        }

        @Override
        public boolean identical(
                Vector leftValues,
                Vector leftNulls,
                int leftPosition,
                Vector rightValues,
                Vector rightNulls,
                int rightPosition)
        {
            ArrayStructuralKeyKernel.ArrayPosition left = ArrayStructuralKeyKernel.arrayPosition(leftValues, leftPosition);
            ArrayStructuralKeyKernel.ArrayPosition right = ArrayStructuralKeyKernel.arrayPosition(rightValues, rightPosition);
            int length = left.values().length(left.position());
            if (length != right.values().length(right.position())) {
                return false;
            }
            Streams leftElements = left.values().elements();
            Streams rightElements = right.values().elements();
            Vector leftElementNulls = leftElements.getOrNull(Stream.NULLS);
            Vector rightElementNulls = rightElements.getOrNull(Stream.NULLS);
            int leftOffset = left.values().startOffset(left.position());
            int rightOffset = right.values().startOffset(right.position());
            for (int index = 0; index < length; index++) {
                int leftElement = leftOffset + index;
                int rightElement = rightOffset + index;
                boolean leftNull = OperatorVectorSupport.isNull(leftElementNulls, leftElement);
                boolean rightNull = OperatorVectorSupport.isNull(rightElementNulls, rightElement);
                if (leftNull || rightNull) {
                    if (leftNull != rightNull) {
                        return false;
                    }
                    continue;
                }
                if (!elements.identical(
                        leftElements.values(), leftElementNulls, leftElement,
                        rightElements.values(), rightElementNulls, rightElement)) {
                    return false;
                }
            }
            return true;
        }
    }

    private static int compareNulls(boolean leftNull, boolean rightNull, boolean nullsFirst)
    {
        if (leftNull == rightNull) {
            return 0;
        }
        return leftNull == nullsFirst ? -1 : 1;
    }

    private static final class BoundStructuralKeyKernel
            implements StructuralKeyKernel
    {
        private static final MethodType STRUCTURAL_HASH_TYPE =
                MethodType.methodType(long.class, Vector.class, int.class);
        private static final MethodType STRUCTURAL_IDENTICAL_TYPE =
                MethodType.methodType(boolean.class, Vector.class, int.class, Vector.class, int.class);

        private final MethodHandle hash;
        private final MethodHandle identical;

        private BoundStructuralKeyKernel(
                TypeBinding type,
                MethodHandle valueRead,
                MethodHandle hash,
                MethodHandle identical)
        {
            Class<?> carrier = type.carrierType();
            BoundStructuralComparisonKernel.requireValueReadType(type, valueRead, carrier);
            BoundStructuralComparisonKernel.requireType(
                    type, "hash", hash, MethodType.methodType(long.class, carrier));
            BoundStructuralComparisonKernel.requireType(
                    type, "identical", identical, MethodType.methodType(boolean.class, carrier, carrier));
            MethodHandle structuralRead =
                    valueRead.asType(MethodType.methodType(carrier, Vector.class, int.class));
            this.hash = collectArguments(hash, 0, structuralRead);
            if (!this.hash.type().equals(STRUCTURAL_HASH_TYPE)) {
                throw new IllegalStateException("Unexpected structural hash method-handle type: " + this.hash.type());
            }
            this.identical = BoundStructuralComparisonKernel.bindBinary(
                    structuralRead, identical, STRUCTURAL_IDENTICAL_TYPE);
        }

        @Override
        public long hash(Vector values, Vector nulls, int position)
        {
            if (OperatorVectorSupport.isNull(nulls, position)) {
                return 0;
            }
            try {
                return (long) hash.invokeExact(values, position);
            }
            catch (Throwable throwable) {
                throw new IllegalStateException("Structural hash failed", throwable);
            }
        }

        @Override
        public boolean identical(
                Vector leftValues,
                Vector leftNulls,
                int leftPosition,
                Vector rightValues,
                Vector rightNulls,
                int rightPosition)
        {
            if (OperatorVectorSupport.isNull(leftNulls, leftPosition) ||
                    OperatorVectorSupport.isNull(rightNulls, rightPosition)) {
                return false;
            }
            try {
                return (boolean) identical.invokeExact(leftValues, leftPosition, rightValues, rightPosition);
            }
            catch (Throwable throwable) {
                throw new IllegalStateException("Structural identity comparison failed", throwable);
            }
        }
    }

    private static final class DirectStructuralKeyKernel
            implements StructuralKeyKernel
    {
        private static final MethodType HASH_TYPE =
                MethodType.methodType(long.class, Vector.class, int.class);
        private static final MethodType IDENTICAL_TYPE =
                MethodType.methodType(boolean.class, Vector.class, int.class, Vector.class, int.class);

        private final MethodHandle hash;
        private final MethodHandle identical;
        private final TypeKeyBinder keyBinder;

        private DirectStructuralKeyKernel(
                TypeBinding type,
                MethodHandle hash,
                MethodHandle identical,
                TypeKeyBinder keyBinder)
        {
            this.hash = requireDirectType(type, "vectorHash", hash, HASH_TYPE);
            this.identical = requireDirectType(type, "vectorIdentical", identical, IDENTICAL_TYPE);
            this.keyBinder = keyBinder;
        }

        @Override
        public Bound bind(Vector values)
        {
            if (keyBinder == null) {
                return StructuralKeyKernel.super.bind(values);
            }
            return new ProviderBoundKey(this, requireNonNull(keyBinder.bind(values), "key binder returned null"));
        }

        @Override
        public long hash(Vector values, Vector nulls, int position)
        {
            if (OperatorVectorSupport.isNull(nulls, position)) {
                return 0;
            }
            try {
                return (long) hash.invokeExact(values, position);
            }
            catch (Throwable throwable) {
                throw new IllegalStateException("Direct structural hash failed", throwable);
            }
        }

        @Override
        public boolean identical(
                Vector leftValues,
                Vector leftNulls,
                int leftPosition,
                Vector rightValues,
                Vector rightNulls,
                int rightPosition)
        {
            if (OperatorVectorSupport.isNull(leftNulls, leftPosition) ||
                    OperatorVectorSupport.isNull(rightNulls, rightPosition)) {
                return false;
            }
            try {
                return (boolean) identical.invokeExact(leftValues, leftPosition, rightValues, rightPosition);
            }
            catch (Throwable throwable) {
                throw new IllegalStateException("Direct structural identity comparison failed", throwable);
            }
        }
    }

    private static final class ProviderBoundKey
            implements StructuralKeyKernel.Bound
    {
        private final StructuralKeyKernel owner;
        private final BoundTypeKey key;

        private ProviderBoundKey(StructuralKeyKernel owner, BoundTypeKey key)
        {
            this.owner = owner;
            this.key = key;
        }

        @Override
        public long hash(int position)
        {
            return key.hash(position);
        }

        @Override
        public boolean identical(int position, StructuralKeyKernel.Bound other, int otherPosition)
        {
            if (!(other instanceof ProviderBoundKey right) || owner != right.owner) {
                throw new IllegalArgumentException("bound key was created by a different kernel");
            }
            return key.identical(position, right.key, otherPosition);
        }
    }

    private static final class DirectStructuralComparisonKernel
            implements StructuralComparisonKernel
    {
        private static final MethodType COMPARISON_TYPE =
                MethodType.methodType(int.class, Vector.class, int.class, Vector.class, int.class);
        private static final MethodType IDENTICAL_TYPE =
                MethodType.methodType(boolean.class, Vector.class, int.class, Vector.class, int.class);

        private final MethodHandle comparison;
        private final MethodHandle identical;

        private DirectStructuralComparisonKernel(TypeBinding type, MethodHandle comparison, MethodHandle identical)
        {
            this.comparison = requireDirectType(type, "vectorComparison", comparison, COMPARISON_TYPE);
            this.identical = requireDirectType(type, "vectorIdentical", identical, IDENTICAL_TYPE);
        }

        @Override
        public int compare(
                Vector leftValues,
                Vector leftNulls,
                int leftPosition,
                Vector rightValues,
                Vector rightNulls,
                int rightPosition)
        {
            boolean leftNull = OperatorVectorSupport.isNull(leftNulls, leftPosition);
            boolean rightNull = OperatorVectorSupport.isNull(rightNulls, rightPosition);
            if (leftNull || rightNull) {
                if (leftNull == rightNull) {
                    return 0;
                }
                return leftNull ? 1 : -1;
            }
            try {
                return (int) comparison.invokeExact(leftValues, leftPosition, rightValues, rightPosition);
            }
            catch (Throwable throwable) {
                throw new IllegalStateException("Direct structural comparison failed", throwable);
            }
        }

        @Override
        public boolean identical(
                Vector leftValues,
                Vector leftNulls,
                int leftPosition,
                Vector rightValues,
                Vector rightNulls,
                int rightPosition)
        {
            if (OperatorVectorSupport.isNull(leftNulls, leftPosition) ||
                    OperatorVectorSupport.isNull(rightNulls, rightPosition)) {
                return false;
            }
            try {
                return (boolean) identical.invokeExact(leftValues, leftPosition, rightValues, rightPosition);
            }
            catch (Throwable throwable) {
                throw new IllegalStateException("Direct structural identity comparison failed", throwable);
            }
        }
    }

    private static MethodHandle requireDirectType(
            TypeBinding type,
            String name,
            MethodHandle handle,
            MethodType expected)
    {
        if (!handle.type().equals(expected)) {
            throw new IllegalArgumentException("Type %s %s handle must have type %s, but is %s"
                    .formatted(type.identity(), name, expected, handle.type()));
        }
        return handle;
    }

    private static final class BoundStructuralComparisonKernel
            implements StructuralComparisonKernel
    {
        private static final MethodType STRUCTURAL_COMPARISON_TYPE =
                MethodType.methodType(int.class, Vector.class, int.class, Vector.class, int.class);
        private static final MethodType STRUCTURAL_IDENTICAL_TYPE =
                MethodType.methodType(boolean.class, Vector.class, int.class, Vector.class, int.class);

        private final MethodHandle comparison;
        private final MethodHandle identical;

        private BoundStructuralComparisonKernel(
                TypeBinding type,
                MethodHandle valueRead,
                MethodHandle comparison,
                MethodHandle identical)
        {
            Class<?> carrier = type.carrierType();
            requireValueReadType(type, valueRead, carrier);
            requireType(type, "comparison", comparison, MethodType.methodType(int.class, carrier, carrier));
            requireType(type, "identical", identical, MethodType.methodType(boolean.class, carrier, carrier));
            MethodHandle structuralRead =
                    valueRead.asType(MethodType.methodType(carrier, Vector.class, int.class));
            this.comparison = bindBinary(structuralRead, comparison, STRUCTURAL_COMPARISON_TYPE);
            this.identical = bindBinary(structuralRead, identical, STRUCTURAL_IDENTICAL_TYPE);
        }

        @Override
        public int compare(
                Vector leftValues,
                Vector leftNulls,
                int leftPosition,
                Vector rightValues,
                Vector rightNulls,
                int rightPosition)
        {
            boolean leftNull = OperatorVectorSupport.isNull(leftNulls, leftPosition);
            boolean rightNull = OperatorVectorSupport.isNull(rightNulls, rightPosition);
            if (leftNull || rightNull) {
                if (leftNull == rightNull) {
                    return 0;
                }
                return leftNull ? 1 : -1;
            }
            try {
                return (int) comparison.invokeExact(leftValues, leftPosition, rightValues, rightPosition);
            }
            catch (Throwable throwable) {
                throw new IllegalStateException("Structural comparison failed", throwable);
            }
        }

        @Override
        public boolean identical(
                Vector leftValues,
                Vector leftNulls,
                int leftPosition,
                Vector rightValues,
                Vector rightNulls,
                int rightPosition)
        {
            if (OperatorVectorSupport.isNull(leftNulls, leftPosition) ||
                    OperatorVectorSupport.isNull(rightNulls, rightPosition)) {
                return false;
            }
            try {
                return (boolean) identical.invokeExact(leftValues, leftPosition, rightValues, rightPosition);
            }
            catch (Throwable throwable) {
                throw new IllegalStateException("Structural identity comparison failed", throwable);
            }
        }

        private static void requireType(TypeBinding type, String name, MethodHandle handle, MethodType expected)
        {
            requireNonNull(handle, name + " is null");
            if (!handle.type().equals(expected)) {
                throw new IllegalArgumentException("Type %s %s has signature %s; expected %s"
                        .formatted(type.identity(), name, handle.type(), expected));
            }
        }

        private static void requireValueReadType(TypeBinding type, MethodHandle valueRead, Class<?> carrier)
        {
            requireNonNull(valueRead, "valueRead is null");
            MethodType actual = valueRead.type();
            if (actual.parameterCount() != 2 ||
                    !Vector.class.isAssignableFrom(actual.parameterType(0)) ||
                    actual.parameterType(1) != int.class ||
                    actual.returnType() != carrier) {
                throw new IllegalArgumentException(
                        "Type %s valueRead has signature %s; expected (Vector, int)%s"
                                .formatted(type.identity(), actual, carrier.getTypeName()));
            }
        }

        private static MethodHandle bindBinary(
                MethodHandle structuralRead,
                MethodHandle operation,
                MethodType structuralType)
        {
            MethodHandle bound = collectArguments(operation, 0, structuralRead);
            bound = collectArguments(bound, 2, structuralRead);
            if (!bound.type().equals(structuralType)) {
                throw new IllegalStateException("Unexpected structural method-handle type: " + bound.type());
            }
            return bound;
        }
    }

    private enum LegacyStructuralIdentityKernel
            implements StructuralIdentityKernel
    {
        INSTANCE;

        @Override
        public boolean identical(
                Vector leftValues,
                Vector leftNulls,
                int leftPosition,
                Vector rightValues,
                Vector rightNulls,
                int rightPosition)
        {
            return OperatorEqualitySemantics.equal(
                    leftValues, leftNulls, leftPosition, rightValues, rightNulls, rightPosition);
        }
    }

    private enum LegacyStructuralKeyKernel
            implements StructuralKeyKernel
    {
        INSTANCE;

        @Override
        public boolean allowsLegacyPhysicalShortcuts()
        {
            return true;
        }

        @Override
        public long hash(Vector values, Vector nulls, int position)
        {
            return OperatorKeySemantics.hash(values, nulls, position);
        }

        @Override
        public boolean identical(
                Vector leftValues,
                Vector leftNulls,
                int leftPosition,
                Vector rightValues,
                Vector rightNulls,
                int rightPosition)
        {
            return OperatorEqualitySemantics.equal(
                    leftValues, leftNulls, leftPosition, rightValues, rightNulls, rightPosition);
        }
    }

    /**
     * Admission marker for a provider whose persistent identity is completely described by a generated fixed-width
     * layout but which does not expose separate semantic row operations. Persistent consumers resolve the layout
     * before invoking this kernel; other consumers fail rather than interpreting raw physical bits as identity.
     */
    private record FixedWidthLayoutOnlyKeyKernel(String type)
            implements StructuralKeyKernel
    {
        private FixedWidthLayoutOnlyKeyKernel
        {
            requireNonNull(type, "type is null");
        }

        @Override
        public long hash(Vector values, Vector nulls, int position)
        {
            throw unsupportedSemanticOperation();
        }

        @Override
        public boolean identical(
                Vector leftValues,
                Vector leftNulls,
                int leftPosition,
                Vector rightValues,
                Vector rightNulls,
                int rightPosition)
        {
            throw unsupportedSemanticOperation();
        }

        private UnsupportedOperationException unsupportedSemanticOperation()
        {
            return new UnsupportedOperationException(
                    "Type %s declares generated fixed-width key identity but no semantic row key operations".formatted(type));
        }
    }

    private enum LegacyStructuralComparisonKernel
            implements StructuralComparisonKernel
    {
        UNORDERED_FIRST(UnorderedPlacement.FIRST),
        UNORDERED_LAST(UnorderedPlacement.LAST);

        private final UnorderedPlacement placement;

        LegacyStructuralComparisonKernel(UnorderedPlacement placement)
        {
            this.placement = requireNonNull(placement, "placement is null");
        }

        @Override
        public boolean allowsLegacyPhysicalShortcuts()
        {
            return true;
        }

        @Override
        public PositionEquality bindPartitionEquality(
                Vector leftValues,
                Vector leftNulls,
                Vector rightValues,
                Vector rightNulls)
        {
            return OperatorEqualitySemantics.bindPartitionEquality(leftValues, leftNulls, rightValues, rightNulls);
        }

        @Override
        public int compare(
                Vector leftValues,
                Vector leftNulls,
                int leftPosition,
                Vector rightValues,
                Vector rightNulls,
                int rightPosition)
        {
            return OperatorOrderingSemantics.compare(
                    leftValues, leftNulls, leftPosition, rightValues, rightNulls, rightPosition, placement);
        }

        @Override
        public boolean identical(
                Vector leftValues,
                Vector leftNulls,
                int leftPosition,
                Vector rightValues,
                Vector rightNulls,
                int rightPosition)
        {
            return OperatorEqualitySemantics.equal(
                    leftValues, leftNulls, leftPosition, rightValues, rightNulls, rightPosition);
        }
    }
}
