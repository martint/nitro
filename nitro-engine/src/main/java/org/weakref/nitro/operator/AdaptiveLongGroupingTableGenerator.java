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

import org.weakref.nitro.data.PrimitiveArrayPool;

import java.lang.classfile.ClassFile;
import java.lang.classfile.CodeBuilder;
import java.lang.classfile.Label;
import java.lang.constant.ClassDesc;
import java.lang.constant.MethodTypeDesc;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.constant.ConstantDescs.CD_boolean;
import static java.lang.constant.ConstantDescs.CD_byte;
import static java.lang.constant.ConstantDescs.CD_int;
import static java.lang.constant.ConstantDescs.CD_long;
import static java.lang.constant.ConstantDescs.CD_void;

/** Emits the arity-dependent loops for {@link AdaptiveLongGroupingTable}. */
final class AdaptiveLongGroupingTableGenerator
        implements AutoCloseable
{
    private static final ClassDesc CD_BASE = ClassDesc.of("org.weakref.nitro.operator.AdaptiveLongGroupingTable");
    private static final ClassDesc CD_PRIMITIVE_ARRAY_POOL = ClassDesc.of("org.weakref.nitro.data.PrimitiveArrayPool");
    private static final ClassDesc CD_CODE_GENERATION = ClassDesc.of("org.weakref.nitro.operator.OperatorCodeGenerationResources");
    private static final ClassDesc CD_POLICY = ClassDesc.of("org.weakref.nitro.operator.AdaptiveLongGroupingPolicy");
    private static final ClassDesc CD_INT_ARRAY = CD_int.arrayType();
    private static final ClassDesc CD_LONG_ARRAY = CD_long.arrayType();
    private static final ClassDesc CD_LONG_ARRAY_2D = CD_LONG_ARRAY.arrayType();
    private static final ClassDesc CD_BYTE_ARRAY = CD_byte.arrayType();
    private static final ClassDesc CD_LONG_VALUES = ClassDesc.of("org.weakref.nitro.data.VectorAccess$LongValues");
    private static final ClassDesc CD_BOOLEAN_VALUES = ClassDesc.of("org.weakref.nitro.data.VectorAccess$BooleanValues");
    private static final ClassDesc CD_LONG_VALUES_ARRAY = CD_LONG_VALUES.arrayType();
    private static final ClassDesc CD_BOOLEAN_VALUES_ARRAY = CD_BOOLEAN_VALUES.arrayType();

    private final ConcurrentHashMap<Integer, MethodHandle> constructors = new ConcurrentHashMap<>();
    private boolean closed;

    AdaptiveLongGroupingTable create(
            int arity,
            int expectedSize,
            boolean groupedProbeEligible,
            PrimitiveArrayPool arrayPool,
            OperatorCodeGenerationResources codeGeneration,
            AdaptiveLongGroupingPolicy policy)
    {
        checkOpen();
        int shape = arity << 1 | (groupedProbeEligible ? 1 : 0);
        MethodHandle constructor = constructors.computeIfAbsent(
                shape,
                key -> generate(key >>> 1, (key & 1) != 0));
        try {
            return (AdaptiveLongGroupingTable) constructor.invoke(arrayPool, codeGeneration, policy, expectedSize);
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to instantiate generated compact grouping table for arity " + arity, e);
        }
    }

    @Override
    public void close()
    {
        closed = true;
        constructors.clear();
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("Adaptive long grouping table generator is closed");
        }
    }

    private static MethodHandle generate(int arity, boolean groupedProbeEligible)
    {
        if (arity < 2 || arity > AbstractMultiLongGroupingTable.MAX_ARITY) {
            throw new IllegalArgumentException("Unsupported grouping arity: " + arity);
        }
        ClassDesc thisClass = ClassDesc.of("org.weakref.nitro.operator.GeneratedAdaptiveLongGroupingTable" + arity +
                (groupedProbeEligible ? "Distinct" : "Grouping"));
        byte[] bytes = ClassFile.of().build(thisClass, builder -> {
            builder.withSuperclass(CD_BASE);
            builder.withFlags(ClassFile.ACC_FINAL | ClassFile.ACC_SYNTHETIC);
            builder.withMethodBody("<init>", MethodTypeDesc.of(CD_void, CD_PRIMITIVE_ARRAY_POOL, CD_CODE_GENERATION, CD_POLICY, CD_int), ClassFile.ACC_PUBLIC, code -> {
                code.aload(0);
                code.aload(1);
                code.aload(2);
                code.aload(3);
                code.loadConstant(arity);
                code.iload(4);
                code.loadConstant(groupedProbeEligible ? 1 : 0);
                code.invokespecial(CD_BASE, "<init>", MethodTypeDesc.of(CD_void, CD_PRIMITIVE_ARRAY_POOL, CD_CODE_GENERATION, CD_POLICY, CD_int, CD_int, CD_boolean));
                code.return_();
            });
            builder.withMethodBody("normalizeBatch", normalizeType(), ClassFile.ACC_PUBLIC, code -> emitNormalize(code, arity));
            builder.withMethodBody("equalsRecord", MethodTypeDesc.of(CD_boolean, CD_int, CD_int), ClassFile.ACC_PUBLIC, code -> emitEquals(code, arity));
            builder.withMethodBody("storeRecord", MethodTypeDesc.of(CD_void, CD_int, CD_int), ClassFile.ACC_PUBLIC, code -> emitStore(code, arity));
            builder.withMethodBody("assignCompactBatch", assignType(), ClassFile.ACC_PUBLIC, code -> emitAssign(code, arity, false, false, false));
            builder.withMethodBody("assignCompactNullFreeBatch", assignType(), ClassFile.ACC_PUBLIC, code -> emitAssign(code, arity, true, false, false));
            builder.withMethodBody("assignCompactDenseBatch", assignType(), ClassFile.ACC_PUBLIC, code -> emitAssign(code, arity, false, true, false));
            builder.withMethodBody("assignCompactDenseNullFreeBatch", assignType(), ClassFile.ACC_PUBLIC, code -> emitAssign(code, arity, true, true, false));
            builder.withMethodBody("assignCompactDistinctNullFreeBatch", assignDistinctType(), ClassFile.ACC_PUBLIC, code -> emitAssign(code, arity, true, false, true));
            builder.withMethodBody("assignCompactDenseDistinctNullFreeBatch", assignDistinctType(), ClassFile.ACC_PUBLIC, code -> emitAssign(code, arity, true, true, true));
        });
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup()
                    .defineHiddenClass(bytes, true, MethodHandles.Lookup.ClassOption.NESTMATE);
            return lookup.findConstructor(
                    lookup.lookupClass(),
                    MethodType.methodType(
                            void.class,
                            PrimitiveArrayPool.class,
                            OperatorCodeGenerationResources.class,
                            AdaptiveLongGroupingPolicy.class,
                            int.class));
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to generate compact grouping table for arity " + arity, e);
        }
    }

    private static MethodTypeDesc normalizeType()
    {
        return MethodTypeDesc.of(CD_boolean, CD_LONG_VALUES_ARRAY, CD_BOOLEAN_VALUES_ARRAY, CD_INT_ARRAY, CD_int);
    }

    private static MethodTypeDesc assignType()
    {
        return MethodTypeDesc.of(CD_long, CD_LONG_VALUES_ARRAY, CD_BOOLEAN_VALUES_ARRAY, CD_INT_ARRAY, CD_int, CD_long.arrayType(), CD_long);
    }

    private static MethodTypeDesc assignDistinctType()
    {
        return MethodTypeDesc.of(CD_long, CD_LONG_VALUES_ARRAY, CD_BOOLEAN_VALUES_ARRAY, CD_INT_ARRAY, CD_int, CD_INT_ARRAY, CD_long);
    }

    // boolean normalizeBatch(LongValues[] keys, BooleanValues[] nulls, int[] positions, int count)
    private static void emitNormalize(CodeBuilder code, int arity)
    {
        int keyAccessorBase = 5;
        int nullAccessorBase = keyAccessorBase + arity;
        int rowVar = nullAccessorBase + arity;
        int positionVar = rowVar + 1;
        int baseVar = rowVar + 2;
        int nullMaskVar = rowVar + 3;
        int hashVar = rowVar + 4;
        int valueVar = rowVar + 6;
        int isNullVar = rowVar + 8;

        for (int column = 0; column < arity; column++) {
            code.aload(1);
            code.loadConstant(column);
            code.aaload();
            code.astore(keyAccessorBase + column);
            code.aload(2);
            code.loadConstant(column);
            code.aaload();
            code.astore(nullAccessorBase + column);
        }

        code.loadConstant(0);
        code.istore(rowVar);
        Label loop = code.newLabel();
        Label done = code.newLabel();
        code.labelBinding(loop);
        code.iload(rowVar);
        code.iload(4);
        code.if_icmpge(done);

        code.aload(3);
        code.iload(rowVar);
        code.iaload();
        code.istore(positionVar);
        code.iload(rowVar);
        code.loadConstant(arity);
        code.imul();
        code.istore(baseVar);
        code.loadConstant(0);
        code.istore(nullMaskVar);
        code.loadConstant(0L);
        code.lstore(hashVar);

        for (int column = 0; column < arity; column++) {
            code.aload(nullAccessorBase + column);
            code.iload(positionVar);
            code.invokeinterface(CD_BOOLEAN_VALUES, "value", MethodTypeDesc.of(CD_boolean, CD_int));
            code.istore(isNullVar);

            Label nonNull = code.newLabel();
            Label valueReady = code.newLabel();
            code.iload(isNullVar);
            code.ifeq(nonNull);
            code.loadConstant(0L);
            code.lstore(valueVar);
            code.iload(nullMaskVar);
            code.loadConstant(1 << column);
            code.ior();
            code.istore(nullMaskVar);
            code.goto_(valueReady);
            code.labelBinding(nonNull);
            code.aload(keyAccessorBase + column);
            code.iload(positionVar);
            code.invokeinterface(CD_LONG_VALUES, "value", MethodTypeDesc.of(CD_long, CD_int));
            code.lstore(valueVar);
            // Exact signed-32 admission: value == (long) (int) value.
            code.lload(valueVar);
            code.lload(valueVar);
            code.l2i();
            code.i2l();
            code.lcmp();
            Label fits = code.newLabel();
            code.ifeq(fits);
            code.loadConstant(0);
            code.ireturn();
            code.labelBinding(fits);
            code.labelBinding(valueReady);

            code.aload(0);
            code.getfield(CD_BASE, "batchKeys", CD_INT_ARRAY);
            code.iload(baseVar);
            code.loadConstant(column);
            code.iadd();
            code.lload(valueVar);
            code.l2i();
            code.iastore();

            code.lload(hashVar);
            code.lload(valueVar);
            code.loadConstant(AbstractMultiLongGroupingTable.HASH_PRIMES[column]);
            code.lmul();
            code.ladd();
            code.lstore(hashVar);
        }

        code.aload(0);
        code.getfield(CD_BASE, "batchNullMasks", CD_BYTE_ARRAY);
        code.iload(rowVar);
        code.iload(nullMaskVar);
        code.bastore();
        code.aload(0);
        code.getfield(CD_BASE, "batchHashes", CD_INT_ARRAY);
        code.iload(rowVar);
        code.lload(hashVar);
        code.iload(nullMaskVar);
        code.i2l();
        code.ladd();
        code.invokestatic(CD_BASE, "mixHash", MethodTypeDesc.of(CD_int, CD_long));
        code.iastore();

        code.iinc(rowVar, 1);
        code.goto_(loop);
        code.labelBinding(done);
        code.loadConstant(1);
        code.ireturn();
    }

    // Fused per-row kernel. Arity is a generation-time constant; no field loop or staged key frame remains.
    private static void emitAssign(CodeBuilder code, int arity, boolean nullFree, boolean dense, boolean distinct)
    {
        int keyAccessorBase = 8;
        int nullAccessorBase = keyAccessorBase + arity;
        int rowVar = nullAccessorBase + (nullFree ? 0 : arity);
        int positionVar = rowVar + 1;
        int keyBase = rowVar + 2;
        int packedPairBase = keyBase + arity;
        int packedTailVar = packedPairBase + (arity / 2) * 2;
        int nullMaskVar = packedTailVar + ((arity & 1) != 0 ? 1 : 0);
        int hashVar = nullMaskVar + 1;
        int valueVar = hashVar + 2;
        int fragmentVar = valueVar + 2;
        int slotVar = fragmentVar + 1;
        int encodedVar = slotVar + 1;
        int groupIdVar = encodedVar + 1;
        int slotsArrayVar = groupIdVar + 1;
        int pairArrayBase = slotsArrayVar + 1;
        int tailArrayVar = pairArrayBase + arity / 2;
        int nullArrayVar = tailArrayVar + 1;
        int distinctCountVar = nullArrayVar + 1;

        for (int column = 0; column < arity; column++) {
            code.aload(1);
            code.loadConstant(column);
            code.aaload();
            code.astore(keyAccessorBase + column);
            if (!nullFree) {
                code.aload(2);
                code.loadConstant(column);
                code.aaload();
                code.astore(nullAccessorBase + column);
            }
        }
        code.aload(0);
        code.getfield(CD_BASE, "slots", CD_INT_ARRAY);
        code.astore(slotsArrayVar);
        for (int pair = 0; pair < arity / 2; pair++) {
            code.aload(0);
            code.getfield(CD_BASE, "keyPairsByGroup", CD_LONG_ARRAY_2D);
            code.loadConstant(pair);
            code.aaload();
            code.astore(pairArrayBase + pair);
        }
        if ((arity & 1) != 0) {
            code.aload(0);
            code.getfield(CD_BASE, "tailKeysByGroup", CD_INT_ARRAY);
            code.astore(tailArrayVar);
        }
        else if (!nullFree) {
            code.aload(0);
            code.getfield(CD_BASE, "nullMasksByGroup", CD_BYTE_ARRAY);
            code.astore(nullArrayVar);
        }
        if (distinct) {
            code.loadConstant(0);
            code.istore(distinctCountVar);
        }
        code.loadConstant(0);
        code.istore(rowVar);
        Label rowLoop = code.newLabel();
        Label done = code.newLabel();
        code.labelBinding(rowLoop);
        code.iload(rowVar);
        code.iload(4);
        code.if_icmpge(done);

        if (dense) {
            code.iload(rowVar);
        }
        else {
            code.aload(3);
            code.iload(rowVar);
            code.iaload();
        }
        code.istore(positionVar);
        code.loadConstant(0);
        code.istore(nullMaskVar);
        code.loadConstant(0L);
        code.lstore(hashVar);

        for (int column = 0; column < arity; column++) {
            Label valueReady = nullFree ? null : code.newLabel();
            if (!nullFree) {
                Label nonNull = code.newLabel();
                code.aload(nullAccessorBase + column);
                code.ifnull(nonNull);
                code.aload(nullAccessorBase + column);
                code.iload(positionVar);
                code.invokeinterface(CD_BOOLEAN_VALUES, "value", MethodTypeDesc.of(CD_boolean, CD_int));
                code.ifeq(nonNull);
                code.loadConstant(0);
                code.istore(keyBase + column);
                code.iload(nullMaskVar);
                code.loadConstant(1 << column);
                code.ior();
                code.istore(nullMaskVar);
                code.goto_(valueReady);
                code.labelBinding(nonNull);
            }
            code.aload(keyAccessorBase + column);
            code.iload(positionVar);
            code.invokeinterface(CD_LONG_VALUES, "value", MethodTypeDesc.of(CD_long, CD_int));
            code.lstore(valueVar);
            code.lload(valueVar);
            code.lload(valueVar);
            code.l2i();
            code.i2l();
            code.lcmp();
            Label fits = code.newLabel();
            code.ifeq(fits);
            code.loadConstant(AdaptiveLongGroupingTable.COMPACT_DOMAIN_EXCEEDED);
            code.lreturn();
            code.labelBinding(fits);
            code.lload(valueVar);
            code.l2i();
            code.istore(keyBase + column);
            if (column == arity - 1 && (arity & 1) != 0) {
                code.iload(keyBase + column);
                code.loadConstant(arity);
                code.invokestatic(CD_BASE, "tailValueFits", MethodTypeDesc.of(CD_boolean, CD_int, CD_int));
                Label tailFits = code.newLabel();
                code.ifne(tailFits);
                code.loadConstant(AdaptiveLongGroupingTable.COMPACT_DOMAIN_EXCEEDED);
                code.lreturn();
                code.labelBinding(tailFits);
            }
            if (!nullFree) {
                code.labelBinding(valueReady);
            }
        }

        // Pack each structural lane once per row. The same concrete key is consumed by hashing, every probe
        // comparison, and insertion, so rebuilding it in each of those paths is pure generated-code overhead.
        for (int pair = 0; pair < arity / 2; pair++) {
            int column = pair * 2;
            code.iload(keyBase + column);
            code.iload(keyBase + column + 1);
            code.invokestatic(CD_BASE, "packPair", MethodTypeDesc.of(CD_long, CD_int, CD_int));
            code.lstore(packedPairBase + pair * 2);
        }
        if ((arity & 1) != 0) {
            code.iload(keyBase + arity - 1);
            code.iload(nullMaskVar);
            code.loadConstant(arity);
            code.invokestatic(CD_BASE, "packTail", MethodTypeDesc.of(CD_int, CD_int, CD_int, CD_int));
            code.istore(packedTailVar);
        }

        code.iload(nullMaskVar);
        code.i2l();
        code.lstore(hashVar);
        if (arity / 2 != 0) {
            code.lload(hashVar);
            code.lload(packedPairBase);
            code.ladd();
            code.lstore(hashVar);
            for (int pair = 1; pair < arity / 2; pair++) {
                int column = pair * 2;
                code.lload(hashVar);
                code.lload(packedPairBase + pair * 2);
                code.loadConstant(AdaptiveLongGroupingTable.compactHashPrime(pair));
                code.lmul();
                code.ladd();
                code.lstore(hashVar);
            }
        }
        if ((arity & 1) != 0) {
            code.lload(hashVar);
            code.iload(keyBase + arity - 1);
            code.i2l();
            code.loadConstant(AdaptiveLongGroupingTable.compactHashPrime(arity / 2));
            code.lmul();
            code.ladd();
            code.lstore(hashVar);
        }
        code.lload(hashVar);
        code.invokestatic(CD_BASE, "mixHash", MethodTypeDesc.of(CD_int, CD_long));
        // Keep one hash copy for the slot and derive the fragment from the other.
        code.dup();
        code.loadConstant(24);
        code.iushr();
        code.loadConstant(0x80);
        code.ior();
        code.istore(fragmentVar);
        code.aload(0);
        code.getfield(CD_BASE, "slotMask", CD_int);
        code.iand();
        code.istore(slotVar);

        Label probe = code.newLabel();
        Label empty = code.newLabel();
        Label advance = code.newLabel();
        Label matched = code.newLabel();
        code.labelBinding(probe);
        code.aload(0);
        code.aload(slotsArrayVar);
        code.iload(slotVar);
        code.aload(0);
        code.getfield(CD_BASE, "slotMask", CD_int);
        code.iload(fragmentVar);
        code.invokevirtual(CD_BASE, "nextProbeCandidate", MethodTypeDesc.of(CD_int, CD_INT_ARRAY, CD_int, CD_int, CD_int));
        code.istore(slotVar);
        code.aload(slotsArrayVar);
        code.iload(slotVar);
        code.iaload();
        code.istore(encodedVar);
        code.iload(encodedVar);
        code.ifeq(empty);
        code.iload(encodedVar);
        code.loadConstant(24);
        code.iushr();
        code.iload(fragmentVar);
        code.if_icmpne(advance);
        code.iload(encodedVar);
        code.loadConstant(0x00FF_FFFF);
        code.iand();
        code.istore(groupIdVar);
        if ((arity & 1) == 0 && !nullFree) {
            code.aload(nullArrayVar);
            code.iload(groupIdVar);
            code.baload();
            code.loadConstant(0xFF);
            code.iand();
            code.iload(nullMaskVar);
            code.if_icmpne(advance);
        }
        for (int pair = 0; pair < arity / 2; pair++) {
            code.lload(packedPairBase + pair * 2);
            code.aload(pairArrayBase + pair);
            code.iload(groupIdVar);
            code.laload();
            code.lcmp();
            code.ifne(advance);
        }
        if ((arity & 1) != 0) {
            code.iload(packedTailVar);
            code.aload(tailArrayVar);
            code.iload(groupIdVar);
            code.iaload();
            code.if_icmpne(advance);
        }
        code.goto_(matched);

        code.labelBinding(empty);
        code.lload(6);
        code.l2i();
        code.istore(groupIdVar);
        Label reverseUnchanged = code.newLabel();
        code.iload(groupIdVar);
        code.aload(0);
        code.getfield(CD_BASE, "reverseCapacity", CD_int);
        code.if_icmplt(reverseUnchanged);
        code.aload(0);
        code.iload(groupIdVar);
        code.invokevirtual(CD_BASE, "ensureReverseCapacity", MethodTypeDesc.of(CD_boolean, CD_int));
        code.pop();
        // Growth may replace reverse arrays; refresh generated lane locals before storing/probing again.
        for (int pair = 0; pair < arity / 2; pair++) {
            code.aload(0);
            code.getfield(CD_BASE, "keyPairsByGroup", CD_LONG_ARRAY_2D);
            code.loadConstant(pair);
            code.aaload();
            code.astore(pairArrayBase + pair);
        }
        if ((arity & 1) != 0) {
            code.aload(0);
            code.getfield(CD_BASE, "tailKeysByGroup", CD_INT_ARRAY);
            code.astore(tailArrayVar);
        }
        else if (!nullFree) {
            code.aload(0);
            code.getfield(CD_BASE, "nullMasksByGroup", CD_BYTE_ARRAY);
            code.astore(nullArrayVar);
        }
        code.labelBinding(reverseUnchanged);
        for (int pair = 0; pair < arity / 2; pair++) {
            code.aload(pairArrayBase + pair);
            code.iload(groupIdVar);
            code.lload(packedPairBase + pair * 2);
            code.lastore();
        }
        if ((arity & 1) != 0) {
            code.aload(tailArrayVar);
            code.iload(groupIdVar);
            code.iload(packedTailVar);
            code.iastore();
        }
        if ((arity & 1) == 0 && !nullFree) {
            code.aload(nullArrayVar);
            code.iload(groupIdVar);
            code.iload(nullMaskVar);
            code.bastore();
        }
        code.aload(slotsArrayVar);
        code.iload(slotVar);
        code.iload(fragmentVar);
        code.loadConstant(24);
        code.ishl();
        code.iload(groupIdVar);
        code.ior();
        code.iastore();
        code.aload(0);
        code.dup();
        code.getfield(CD_BASE, "size", CD_int);
        code.loadConstant(1);
        code.iadd();
        code.dup_x1();
        code.putfield(CD_BASE, "size", CD_int);
        code.aload(0);
        code.getfield(CD_BASE, "maxFill", CD_int);
        Label noRehash = code.newLabel();
        code.if_icmplt(noRehash);
        code.aload(0);
        code.aload(0);
        code.getfield(CD_BASE, "slots", CD_INT_ARRAY);
        code.arraylength();
        code.loadConstant(2);
        code.imul();
        code.invokevirtual(CD_BASE, "rehash", MethodTypeDesc.of(CD_void, CD_int));
        code.aload(0);
        code.getfield(CD_BASE, "slots", CD_INT_ARRAY);
        code.astore(slotsArrayVar);
        code.labelBinding(noRehash);
        code.lload(6);
        code.loadConstant(1L);
        code.ladd();
        code.lstore(6);

        if (distinct) {
            code.aload(5);
            code.iload(distinctCountVar);
            code.iload(positionVar);
            code.iastore();
            code.iinc(distinctCountVar, 1);
        }

        code.labelBinding(matched);
        if (!distinct) {
            code.aload(5);
            code.iload(positionVar);
            code.iload(groupIdVar);
            code.i2l();
            code.lastore();
        }
        code.iinc(rowVar, 1);
        code.goto_(rowLoop);

        code.labelBinding(advance);
        code.iload(slotVar);
        code.loadConstant(1);
        code.iadd();
        code.aload(0);
        code.getfield(CD_BASE, "slotMask", CD_int);
        code.iand();
        code.istore(slotVar);
        code.goto_(probe);

        code.labelBinding(done);
        code.lload(6);
        code.lreturn();
    }

    private static void emitEquals(CodeBuilder code, int arity)
    {
        Label notEqual = code.newLabel();
        for (int pair = 0; pair < arity / 2; pair++) {
            int column = pair * 2;
            code.aload(0);
            code.getfield(CD_BASE, "batchKeys", CD_INT_ARRAY);
            code.iload(1);
            code.loadConstant(column);
            code.iadd();
            code.iaload();
            code.aload(0);
            code.getfield(CD_BASE, "batchKeys", CD_INT_ARRAY);
            code.iload(1);
            code.loadConstant(column + 1);
            code.iadd();
            code.iaload();
            code.invokestatic(CD_BASE, "packPair", MethodTypeDesc.of(CD_long, CD_int, CD_int));
            code.aload(0);
            code.getfield(CD_BASE, "keyPairsByGroup", CD_LONG_ARRAY_2D);
            code.loadConstant(pair);
            code.aaload();
            code.iload(2);
            code.laload();
            code.lcmp();
            code.ifne(notEqual);
        }
        if ((arity & 1) != 0) {
            code.aload(0);
            code.getfield(CD_BASE, "batchKeys", CD_INT_ARRAY);
            code.iload(1);
            code.loadConstant(arity - 1);
            code.iadd();
            code.iaload();
            code.aload(0);
            code.getfield(CD_BASE, "tailKeysByGroup", CD_INT_ARRAY);
            code.iload(2);
            code.iaload();
            code.if_icmpne(notEqual);
        }
        code.loadConstant(1);
        code.ireturn();
        code.labelBinding(notEqual);
        code.loadConstant(0);
        code.ireturn();
    }

    private static void emitStore(CodeBuilder code, int arity)
    {
        for (int pair = 0; pair < arity / 2; pair++) {
            int column = pair * 2;
            code.aload(0);
            code.getfield(CD_BASE, "keyPairsByGroup", CD_LONG_ARRAY_2D);
            code.loadConstant(pair);
            code.aaload();
            code.iload(2);
            code.aload(0);
            code.getfield(CD_BASE, "batchKeys", CD_INT_ARRAY);
            code.iload(1);
            code.loadConstant(column);
            code.iadd();
            code.iaload();
            code.aload(0);
            code.getfield(CD_BASE, "batchKeys", CD_INT_ARRAY);
            code.iload(1);
            code.loadConstant(column + 1);
            code.iadd();
            code.iaload();
            code.invokestatic(CD_BASE, "packPair", MethodTypeDesc.of(CD_long, CD_int, CD_int));
            code.lastore();
        }
        if ((arity & 1) != 0) {
            code.aload(0);
            code.getfield(CD_BASE, "tailKeysByGroup", CD_INT_ARRAY);
            code.iload(2);
            code.aload(0);
            code.getfield(CD_BASE, "batchKeys", CD_INT_ARRAY);
            code.iload(1);
            code.loadConstant(arity - 1);
            code.iadd();
            code.iaload();
            code.iastore();
        }
        code.return_();
    }
}
