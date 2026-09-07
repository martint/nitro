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
import org.weakref.nitro.data.PrimitiveArrayPool;

import java.lang.classfile.ClassFile;
import java.lang.classfile.CodeBuilder;
import java.lang.classfile.Label;
import java.lang.constant.ClassDesc;
import java.lang.constant.DirectMethodHandleDesc;
import java.lang.constant.DynamicCallSiteDesc;
import java.lang.constant.MethodTypeDesc;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

import static java.lang.constant.ConstantDescs.CD_CallSite;
import static java.lang.constant.ConstantDescs.CD_boolean;
import static java.lang.constant.ConstantDescs.CD_byte;
import static java.lang.constant.ConstantDescs.CD_double;
import static java.lang.constant.ConstantDescs.CD_int;
import static java.lang.constant.ConstantDescs.CD_long;
import static java.lang.constant.ConstantDescs.CD_void;
import static java.lang.constant.DirectMethodHandleDesc.Kind.STATIC;
import static java.lang.constant.MethodHandleDesc.ofMethod;
import static java.util.Objects.requireNonNull;

/**
 * Generates a concrete {@link AbstractFixedWidthKeyTable} for each exact physical layout. Generated hot paths load
 * the layout's primitive sources into locals, apply any constant-linked canonical projections, and probe with no
 * per-row type dispatch or scratch tuple. Logical types never select a generator or table class.
 */
final class FixedWidthKeyTableGenerator
        implements AutoCloseable
{
    private static final ClassDesc CD_BASE = ClassDesc.of("org.weakref.nitro.operator.AbstractFixedWidthKeyTable");
    private static final ClassDesc CD_POLICY = ClassDesc.of("org.weakref.nitro.operator.AdaptiveLongGroupingPolicy");
    private static final ClassDesc CD_PRIMITIVE_ARRAY_POOL = ClassDesc.of("org.weakref.nitro.data.PrimitiveArrayPool");
    private static final ClassDesc CD_LONG_ARRAY = CD_long.arrayType();
    private static final ClassDesc CD_RAW_INT_ARRAY = CD_int.arrayType();
    private static final ClassDesc CD_DOUBLE_ARRAY = java.lang.constant.ConstantDescs.CD_double.arrayType();
    private static final ClassDesc CD_BOOLEAN_ARRAY = CD_boolean.arrayType();
    private static final ClassDesc CD_OBJECT_ARRAY = java.lang.constant.ConstantDescs.CD_Object.arrayType();
    private static final ClassDesc CD_LONG_ARRAY_2D = CD_long.arrayType().arrayType();
    private static final ClassDesc CD_BYTE_ARRAY = CD_byte.arrayType();
    private static final ClassDesc CD_INT_ARRAY = CD_int.arrayType();
    private static final ClassDesc CD_INT_ARRAY_2D = CD_int.arrayType().arrayType();
    private static final ClassDesc CD_LONG_VALUES = ClassDesc.of("org.weakref.nitro.data.VectorAccess$LongValues");
    private static final ClassDesc CD_BOOLEAN_VALUES = ClassDesc.of("org.weakref.nitro.data.VectorAccess$BooleanValues");
    private static final ClassDesc CD_LONG_VALUES_ARRAY = CD_LONG_VALUES.arrayType();
    private static final ClassDesc CD_BOOLEAN_VALUES_ARRAY = CD_BOOLEAN_VALUES.arrayType();
    private static final ClassDesc CD_PROJECTION_BOOTSTRAP = ClassDesc.of("org.weakref.nitro.operator.FixedWidthKeyProjectionBootstrap");
    private static final DirectMethodHandleDesc BSM_PROJECTION = ofMethod(
            STATIC,
            CD_PROJECTION_BOOTSTRAP,
            "bootstrap",
            MethodTypeDesc.of(CD_CallSite, ClassDesc.of("java.lang.invoke.MethodHandles$Lookup"), ClassDesc.of("java.lang.String"), ClassDesc.of("java.lang.invoke.MethodType")));

    private static final int MURMUR_SHIFT = 33;
    private static final long MURMUR_C1 = 0xFF51AFD7ED558CCDL;
    private static final long MURMUR_C2 = 0xC4CEB9FE1A85EC53L;

    private final ConcurrentHashMap<GenerationShape, MethodHandle> constructors = new ConcurrentHashMap<>();
    private final AtomicInteger nextClassId = new AtomicInteger();
    private boolean closed;

    AbstractFixedWidthKeyTable create(
            FixedWidthKeyTableLayout layout,
            int expectedSize,
            PrimitiveArrayPool arrayPool,
            AdaptiveLongGroupingPolicy policy)
    {
        return create(new GenerationShape(layout, 0), expectedSize, true, true, 0, arrayPool, policy);
    }

    AbstractFixedWidthKeyTable create(
            FixedWidthKeyTableLayout layout,
            int expectedSize,
            int compactRetainedColumns,
            PrimitiveArrayPool arrayPool,
            AdaptiveLongGroupingPolicy policy)
    {
        return create(new GenerationShape(layout, compactRetainedColumns), expectedSize, true, true, compactRetainedColumns, arrayPool, policy);
    }

    AbstractFixedWidthKeyTable createDistinct(
            FixedWidthKeyTableLayout layout,
            int expectedSize,
            PrimitiveArrayPool arrayPool,
            AdaptiveLongGroupingPolicy policy)
    {
        return create(new GenerationShape(layout, 0), expectedSize, false, false, 0, arrayPool, policy);
    }

    AbstractFixedWidthKeyTable createDiscardingResults(
            FixedWidthKeyTableLayout layout,
            int expectedSize,
            PrimitiveArrayPool arrayPool,
            AdaptiveLongGroupingPolicy policy)
    {
        return createDiscardingResults(layout, expectedSize, 0, arrayPool, policy);
    }

    AbstractFixedWidthKeyTable createDiscardingResults(
            FixedWidthKeyTableLayout layout,
            int expectedSize,
            int compactRetainedColumns,
            PrimitiveArrayPool arrayPool,
            AdaptiveLongGroupingPolicy policy)
    {
        return create(new GenerationShape(layout, compactRetainedColumns), expectedSize, false, true, compactRetainedColumns, arrayPool, policy);
    }

    private AbstractFixedWidthKeyTable create(
            GenerationShape shape,
            int expectedSize,
            boolean storesGroupIds,
            boolean retainGroupKeys,
            int compactRetainedColumns,
            PrimitiveArrayPool arrayPool,
            AdaptiveLongGroupingPolicy policy)
    {
        checkOpen();
        int arity = shape.layout().laneCount();
        if (arity < 1 || arity > AbstractFixedWidthKeyTable.MAX_ARITY) {
            throw new IllegalArgumentException("Unsupported grouping arity: " + arity);
        }
        MethodHandle constructor = constructors.computeIfAbsent(shape, this::generate);
        try {
            return (AbstractFixedWidthKeyTable) constructor.invoke(arrayPool, expectedSize, storesGroupIds, retainGroupKeys, compactRetainedColumns, policy);
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to instantiate generated grouping table for arity " + arity, e);
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
            throw new IllegalStateException("Fixed-width key table generator is closed");
        }
    }

    private MethodHandle generate(GenerationShape shape)
    {
        int arity = shape.layout().laneCount();
        int compactRetainedColumns = shape.compactRetainedColumns();
        ClassDesc thisClass = ClassDesc.of("org.weakref.nitro.operator.GeneratedFixedWidthKeyTable" + nextClassId.incrementAndGet());
        MethodTypeDesc assignGroupType = assignGroupType(arity);
        MethodTypeDesc findGroupType = assignGroupType;

        byte[] bytes = ClassFile.of().build(thisClass, builder -> {
            builder.withSuperclass(CD_BASE);
            builder.withFlags(ClassFile.ACC_FINAL | ClassFile.ACC_SYNTHETIC);

            // <init>(PrimitiveArrayPool, int, boolean, boolean, int, policy) {
            //     super(pool, arity, expectedSize, storesGroupIds, retainGroupKeys, compactRetainedColumns, policy);
            // }
            builder.withMethodBody("<init>", MethodTypeDesc.of(CD_void, CD_PRIMITIVE_ARRAY_POOL, CD_int, CD_boolean, CD_boolean, CD_int, CD_POLICY), ClassFile.ACC_PUBLIC, code -> {
                code.aload(0);
                code.aload(1);
                code.loadConstant(arity);
                code.iload(2);
                code.iload(3);
                code.iload(4);
                code.iload(5);
                code.aload(6);
                code.invokespecial(CD_BASE, "<init>", MethodTypeDesc.of(CD_void, CD_PRIMITIVE_ARRAY_POOL, CD_int, CD_int, CD_boolean, CD_boolean, CD_int, CD_POLICY));
                code.return_();
            });

            builder.withMethodBody("assignGroup", assignGroupType, ClassFile.ACC_PUBLIC, code -> emitAssignGroup(code, arity, thisClass, true, true, compactRetainedColumns, true, true));
            builder.withMethodBody("assignRetainedGroup", assignGroupType, ClassFile.ACC_PRIVATE, code -> emitAssignGroup(code, arity, thisClass, false, true, compactRetainedColumns, true, true));
            builder.withMethodBody("assignDistinctGroup", assignGroupType, ClassFile.ACC_PRIVATE, code -> emitAssignGroup(code, arity, thisClass, false, false, 0, false, true));
            builder.withMethodBody("findGroup", findGroupType, ClassFile.ACC_PRIVATE, code -> emitAssignGroup(code, arity, thisClass, true, true, compactRetainedColumns, true, false));
            builder.withMethodBody("assignBatch", assignBatchType(), ClassFile.ACC_PUBLIC, code -> emitAssignBatch(code, arity, thisClass, assignGroupType, "assignGroup", false, false, true));
            builder.withMethodBody("assignPhysicalBatch", assignPhysicalBatchType(false), ClassFile.ACC_PUBLIC, code -> emitAssignPhysicalBatch(code, shape, thisClass, assignGroupType, false, false));
            builder.withMethodBody("assignPhysicalNonNullBatch", assignPhysicalBatchType(false), ClassFile.ACC_PUBLIC, code -> emitAssignPhysicalBatch(code, shape, thisClass, assignGroupType, false, true));
            builder.withMethodBody("assignPhysicalDistinctBatch", assignPhysicalBatchType(true), ClassFile.ACC_PUBLIC, code -> emitAssignPhysicalBatch(code, shape, thisClass, assignGroupType, true, true));
            builder.withMethodBody("assignPhysicalDistinctRetainingNullBatch", assignPhysicalBatchType(true), ClassFile.ACC_PUBLIC, code -> emitAssignPhysicalBatch(code, shape, thisClass, assignGroupType, true, false));
            builder.withMethodBody("findPhysicalBatch", findPhysicalBatchType(), ClassFile.ACC_PUBLIC, code -> emitFindPhysicalBatch(code, shape, thisClass, findGroupType));
            builder.withMethodBody("extractPhysicalKey", extractPhysicalKeyType(), ClassFile.ACC_PUBLIC, code -> emitExtractPhysicalKey(code, shape));
            builder.withMethodBody("assignBatchDiscardingResults", assignBatchDiscardingResultsType(), ClassFile.ACC_PUBLIC, code -> emitAssignBatchDiscardingResults(code, thisClass));
            builder.withMethodBody("assignBatchWithoutResults", assignBatchType(), ClassFile.ACC_PRIVATE, code -> emitAssignBatch(code, arity, thisClass, assignGroupType, "assignGroup", false, false, false));
            builder.withMethodBody("assignRetainedBatchWithoutResults", assignBatchType(), ClassFile.ACC_PRIVATE, code -> emitAssignBatch(code, arity, thisClass, assignGroupType, "assignRetainedGroup", false, false, false));
            builder.withMethodBody("assignDistinctBatch", assignDistinctBatchType(), ClassFile.ACC_PUBLIC, code -> emitAssignBatch(code, arity, thisClass, assignGroupType, "assignDistinctGroup", true, false, false));
            builder.withMethodBody("assignDistinctBatchNullFree", assignDistinctBatchType(), ClassFile.ACC_PUBLIC, code -> emitAssignBatch(code, arity, thisClass, assignGroupType, "assignDistinctGroup", true, true, false));
            builder.withMethodBody("hashEntry", MethodTypeDesc.of(CD_int, CD_LONG_ARRAY, CD_int, CD_byte), ClassFile.ACC_PUBLIC, code -> emitHashEntry(code, arity));
        });

        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup()
                    .defineHiddenClassWithClassData(bytes, shape.layout().projections(), true, MethodHandles.Lookup.ClassOption.NESTMATE);
            return lookup.findConstructor(
                    lookup.lookupClass(),
                    MethodType.methodType(
                            void.class,
                            PrimitiveArrayPool.class,
                            int.class,
                            boolean.class,
                            boolean.class,
                            int.class,
                            AdaptiveLongGroupingPolicy.class));
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to generate grouping table for arity " + arity, e);
        }
    }

    private record GenerationShape(FixedWidthKeyTableLayout layout, int compactRetainedColumns)
    {
        private GenerationShape
        {
            requireNonNull(layout, "layout is null");
        }
    }

    private static MethodTypeDesc assignGroupType(int arity)
    {
        List<ClassDesc> params = new ArrayList<>();
        for (int key = 0; key < arity; key++) {
            params.add(CD_long);
        }
        params.add(CD_byte);
        params.add(CD_long);
        return MethodTypeDesc.of(CD_long, params);
    }

    private static MethodTypeDesc assignBatchType()
    {
        return MethodTypeDesc.of(CD_long, CD_LONG_VALUES_ARRAY, CD_BOOLEAN_VALUES_ARRAY, CD_INT_ARRAY, CD_int, CD_LONG_ARRAY, CD_long);
    }

    private static MethodTypeDesc assignPhysicalBatchType(boolean distinct)
    {
        return MethodTypeDesc.of(
                distinct ? CD_int : CD_long,
                CD_OBJECT_ARRAY, CD_INT_ARRAY_2D, CD_INT_ARRAY, CD_INT_ARRAY,
                CD_BOOLEAN_ARRAY.arrayType(), CD_INT_ARRAY_2D, CD_INT_ARRAY, CD_INT_ARRAY,
                CD_INT_ARRAY, CD_int, distinct ? CD_INT_ARRAY : CD_LONG_ARRAY, CD_long);
    }

    private static MethodTypeDesc findPhysicalBatchType()
    {
        return MethodTypeDesc.of(
                CD_int,
                CD_OBJECT_ARRAY, CD_INT_ARRAY_2D, CD_INT_ARRAY, CD_INT_ARRAY,
                CD_BOOLEAN_ARRAY.arrayType(), CD_INT_ARRAY_2D, CD_INT_ARRAY, CD_INT_ARRAY,
                CD_INT_ARRAY, CD_int, CD_LONG_ARRAY);
    }

    private static MethodTypeDesc extractPhysicalKeyType()
    {
        return MethodTypeDesc.of(
                CD_byte,
                CD_OBJECT_ARRAY, CD_INT_ARRAY_2D, CD_INT_ARRAY, CD_INT_ARRAY,
                CD_BOOLEAN_ARRAY.arrayType(), CD_INT_ARRAY_2D, CD_INT_ARRAY, CD_INT_ARRAY,
                CD_INT_ARRAY, CD_INT_ARRAY, CD_LONG_ARRAY);
    }

    private static MethodTypeDesc assignDistinctBatchType()
    {
        return MethodTypeDesc.of(CD_int, CD_LONG_VALUES_ARRAY, CD_BOOLEAN_VALUES_ARRAY, CD_INT_ARRAY, CD_int, CD_INT_ARRAY, CD_long);
    }

    private static MethodTypeDesc assignBatchDiscardingResultsType()
    {
        return MethodTypeDesc.of(CD_long, CD_LONG_VALUES_ARRAY, CD_BOOLEAN_VALUES_ARRAY, CD_INT_ARRAY, CD_int, CD_long);
    }

    private static void emitAssignBatchDiscardingResults(CodeBuilder code, ClassDesc thisClass)
    {
        code.aload(0);
        code.getfield(CD_BASE, "storesGroupIds", CD_boolean);
        Label retainedOnly = code.newLabel();
        code.ifeq(retainedOnly);
        code.aload(0);
        code.aload(1);
        code.aload(2);
        code.aload(3);
        code.iload(4);
        code.aconst_null();
        code.lload(5);
        code.invokevirtual(thisClass, "assignBatchWithoutResults", assignBatchType());
        code.lreturn();
        code.labelBinding(retainedOnly);
        code.aload(0);
        code.aload(1);
        code.aload(2);
        code.aload(3);
        code.iload(4);
        code.aconst_null();
        code.lload(5);
        code.invokevirtual(thisClass, "assignRetainedBatchWithoutResults", assignBatchType());
        code.lreturn();
    }

    // long assignGroup(long k0..kN-1, byte nullMask, long newGroupId)
    // locals: this=0, k_i=1+2i, nullMask=1+2N, newGroupId=2+2N
    private static void emitAssignGroup(
            CodeBuilder code,
            int arity,
            ClassDesc thisClass,
            boolean storesGroupIds,
            boolean retainGroupKeys,
            int compactRetainedColumns,
            boolean identityGroupIdSlots,
            boolean insertOnMiss)
    {
        int nullMaskSlot = 1 + 2 * arity;
        int newGroupIdSlot = 2 + 2 * arity;
        int slotVar = 4 + 2 * arity;
        int baseVar = 5 + 2 * arity;
        int fragVar = 6 + 2 * arity;
        int controlVar = 7 + 2 * arity;
        int compactKeysVar = 8 + 2 * arity;
        int retainedGroupIdVar = 9 + 2 * arity;
        int recordStride = arity + (storesGroupIds ? 1 : 0);

        // hash = hash(keys, nullMask)
        emitHash(code, arity, key -> code.lload(1 + 2 * key), () -> code.iload(nullMaskSlot));
        // frag = controlFragment(hash) = (byte) ((hash >>> 24) | 0x80) ; consumes one copy of hash
        code.dup();
        code.loadConstant(24);
        code.iushr();
        code.loadConstant(0x80);
        code.ior();
        code.i2b();
        code.istore(fragVar);
        // slot = hash & mask
        code.aload(0);
        code.getfield(CD_BASE, "mask", CD_int);
        code.iand();
        code.istore(slotVar);

        Label probeTop = code.newLabel();
        Label empty = code.newLabel();
        Label advance = code.newLabel();
        code.labelBinding(probeTop);

        // control byte: 0 => empty slot, else compare the hash fragment before touching the fat record
        code.aload(0);
        code.getfield(CD_BASE, "control", CD_BYTE_ARRAY);
        code.iload(slotVar);
        code.baload();
        code.istore(controlVar);
        code.iload(controlVar);
        code.ifeq(empty);
        code.iload(controlVar);
        code.iload(fragVar);
        code.if_icmpne(advance);

        // ---- fragment match: compare keys then nullMask ----
        if (identityGroupIdSlots) {
            code.aload(0);
            code.getfield(CD_BASE, "groupIds", CD_INT_ARRAY);
            code.iload(slotVar);
            code.iaload();
            code.istore(retainedGroupIdVar);
        }
        else {
            // base = slot * stride
            code.iload(slotVar);
            code.loadConstant(recordStride);
            code.imul();
            code.istore(baseVar);
        }
        for (int key = 0; key < arity; key++) {
            if (identityGroupIdSlots) {
                code.aload(0);
                code.loadConstant(key);
                code.iload(retainedGroupIdVar);
                code.invokevirtual(CD_BASE, "groupedValue", MethodTypeDesc.of(CD_long, CD_int, CD_int));
            }
            else {
                code.aload(0);
                code.getfield(CD_BASE, "entries", CD_LONG_ARRAY);
                code.iload(baseVar);
                code.loadConstant(key);
                code.iadd();
                code.laload();
            }
            code.lload(1 + 2 * key);
            code.lxor();
            if (key > 0) {
                code.lor();
            }
        }
        code.loadConstant(0L);
        code.lcmp();
        code.ifne(advance);
        code.aload(0);
        code.getfield(CD_BASE, identityGroupIdSlots ? "nullMasksByGroup" : "nullMasks", CD_BYTE_ARRAY);
        code.iload(identityGroupIdSlots ? retainedGroupIdVar : slotVar);
        code.baload();
        code.iload(nullMaskSlot);
        code.if_icmpne(advance);
        // Grouping returns the stored id. DISTINCT only needs a value unequal to newGroupId.
        if (identityGroupIdSlots) {
            code.iload(retainedGroupIdVar);
            code.i2l();
        }
        else if (storesGroupIds) {
            code.aload(0);
            code.getfield(CD_BASE, "entries", CD_LONG_ARRAY);
            code.iload(baseVar);
            code.loadConstant(arity);
            code.iadd();
            code.laload();
        }
        else {
            code.loadConstant(AbstractFixedWidthKeyTable.EMPTY_GROUP_ID);
        }
        code.lreturn();

        // ---- empty slot: insert new group ----
        code.labelBinding(empty);
        if (!insertOnMiss) {
            code.loadConstant(AbstractFixedWidthKeyTable.EMPTY_GROUP_ID);
            code.lreturn();
        }
        if (!identityGroupIdSlots) {
            // base = slot * stride
            code.iload(slotVar);
            code.loadConstant(recordStride);
            code.imul();
            code.istore(baseVar);
            for (int key = 0; key < arity; key++) {
                code.aload(0);
                code.getfield(CD_BASE, "entries", CD_LONG_ARRAY);
                code.iload(baseVar);
                code.loadConstant(key);
                code.iadd();
                code.lload(1 + 2 * key);
                code.lastore();
            }
            if (storesGroupIds) {
                code.aload(0);
                code.getfield(CD_BASE, "entries", CD_LONG_ARRAY);
                code.iload(baseVar);
                code.loadConstant(arity);
                code.iadd();
                code.lload(newGroupIdSlot);
                code.lastore();
            }
            code.aload(0);
            code.getfield(CD_BASE, "nullMasks", CD_BYTE_ARRAY);
            code.iload(slotVar);
            code.iload(nullMaskSlot);
            code.bastore();
        }
        // control[slot] = frag  (mark the slot occupied with its hash fragment)
        code.aload(0);
        code.getfield(CD_BASE, "control", CD_BYTE_ARRAY);
        code.iload(slotVar);
        code.iload(fragVar);
        code.bastore();
        if (retainGroupKeys) {
            // ensureReverseCapacity((int) newGroupId)
            code.aload(0);
            code.lload(newGroupIdSlot);
            code.l2i();
            code.invokevirtual(CD_BASE, "ensureReverseCapacity", MethodTypeDesc.of(CD_void, CD_int));
            // keysByGroup[key][(int) newGroupId] = k_key
            for (int key = 0; key < arity; key++) {
                if ((compactRetainedColumns & (1 << key)) != 0) {
                    Label widened = code.newLabel();
                    Label stored = code.newLabel();
                    code.aload(0);
                    code.getfield(CD_BASE, "compactKeysByGroup", CD_INT_ARRAY_2D);
                    code.loadConstant(key);
                    code.aaload();
                    code.astore(compactKeysVar);
                    code.aload(compactKeysVar);
                    code.ifnull(widened);
                    code.lload(1 + 2 * key);
                    code.dup2();
                    code.l2i();
                    code.i2l();
                    code.lcmp();
                    code.ifne(widened);
                    code.aload(compactKeysVar);
                    code.lload(newGroupIdSlot);
                    code.l2i();
                    code.lload(1 + 2 * key);
                    code.l2i();
                    code.iastore();
                    code.goto_(stored);
                    code.labelBinding(widened);
                    code.aload(0);
                    code.loadConstant(key);
                    code.lload(newGroupIdSlot);
                    code.l2i();
                    code.lload(1 + 2 * key);
                    code.invokevirtual(CD_BASE, "storeCompactRetainedKey", MethodTypeDesc.of(CD_void, CD_int, CD_int, CD_long));
                    code.labelBinding(stored);
                }
                else {
                    code.aload(0);
                    code.getfield(CD_BASE, "keysByGroup", CD_LONG_ARRAY_2D);
                    code.loadConstant(key);
                    code.aaload();
                    code.lload(newGroupIdSlot);
                    code.l2i();
                    code.lload(1 + 2 * key);
                    code.lastore();
                }
            }
            code.aload(0);
            code.getfield(CD_BASE, "nullMasksByGroup", CD_BYTE_ARRAY);
            code.lload(newGroupIdSlot);
            code.l2i();
            code.iload(nullMaskSlot);
            code.bastore();
        }
        if (identityGroupIdSlots) {
            code.aload(0);
            code.getfield(CD_BASE, "groupIds", CD_INT_ARRAY);
            code.iload(slotVar);
            code.lload(newGroupIdSlot);
            code.l2i();
            code.iastore();
        }
        // size++
        code.aload(0);
        code.dup();
        code.getfield(CD_BASE, "size", CD_int);
        code.loadConstant(1);
        code.iadd();
        code.putfield(CD_BASE, "size", CD_int);
        // if (size >= maxFill) rehash()
        code.aload(0);
        code.getfield(CD_BASE, "size", CD_int);
        code.aload(0);
        code.getfield(CD_BASE, "maxFill", CD_int);
        Label skipRehash = code.newLabel();
        code.if_icmplt(skipRehash);
        code.aload(0);
        code.invokevirtual(CD_BASE, "rehash", MethodTypeDesc.of(CD_void));
        code.labelBinding(skipRehash);
        code.lload(newGroupIdSlot);
        code.lreturn();

        // ---- advance to next slot ----
        code.labelBinding(advance);
        code.iload(slotVar);
        code.loadConstant(1);
        code.iadd();
        code.aload(0);
        code.getfield(CD_BASE, "mask", CD_int);
        code.iand();
        code.istore(slotVar);
        code.goto_(probeTop);
    }

    // long assignBatch(LongValues[] keyAcc, BooleanValues[] nullAcc, int[] positions, int count, long[] result, long startGroupId)
    // locals: this=0, keyAcc=1, nullAcc=2, positions=3, count=4, result=5, startGroupId/groupId=6
    private static void emitAssignBatch(CodeBuilder code, int arity, ClassDesc thisClass, MethodTypeDesc assignGroupType, String assignGroupMethod, boolean distinct, boolean nullFree, boolean writeResults)
    {
        int accBase = 8;                       // hoisted key accessors a0..a(N-1)
        int nullAccBase = 8 + arity;           // hoisted null accessors n0..n(N-1)
        int p = 8 + 2 * arity;
        int pos = 9 + 2 * arity;
        int keyBase = 10 + 2 * arity;          // k_i at keyBase + 2*i
        int nullMaskVar = 10 + 4 * arity;
        int gidVar = 11 + 4 * arity;
        int distinctCountVar = 13 + 4 * arity;
        int groupIdVar = 6;

        if (distinct) {
            code.loadConstant(0);
            code.istore(distinctCountVar);
        }

        for (int key = 0; key < arity; key++) {
            code.aload(1);
            code.loadConstant(key);
            code.aaload();
            code.astore(accBase + key);
            if (!nullFree) {
                code.aload(2);
                code.loadConstant(key);
                code.aaload();
                code.astore(nullAccBase + key);
            }
        }

        code.loadConstant(0);
        code.istore(p);
        Label loopTop = code.newLabel();
        Label loopEnd = code.newLabel();
        code.labelBinding(loopTop);
        code.iload(p);
        code.iload(4);
        code.if_icmpge(loopEnd);

        // pos = positions[p]
        code.aload(3);
        code.iload(p);
        code.iaload();
        code.istore(pos);
        // nullMask = 0
        code.loadConstant(0);
        code.istore(nullMaskVar);

        for (int key = 0; key < arity; key++) {
            if (nullFree) {
                code.aload(accBase + key);
                code.iload(pos);
                code.invokeinterface(CD_LONG_VALUES, "value", MethodTypeDesc.of(CD_long, CD_int));
                code.lstore(keyBase + 2 * key);
            }
            else {
                code.aload(nullAccBase + key);
                code.iload(pos);
                code.invokeinterface(CD_BOOLEAN_VALUES, "value", MethodTypeDesc.of(CD_boolean, CD_int));
                Label notNull = code.newLabel();
                Label done = code.newLabel();
                code.ifeq(notNull);
                // null: nullMask |= (1 << key); k_key = 0
                code.iload(nullMaskVar);
                code.loadConstant(1 << key);
                code.ior();
                code.istore(nullMaskVar);
                code.loadConstant(0L);
                code.lstore(keyBase + 2 * key);
                code.goto_(done);
                code.labelBinding(notNull);
                code.aload(accBase + key);
                code.iload(pos);
                code.invokeinterface(CD_LONG_VALUES, "value", MethodTypeDesc.of(CD_long, CD_int));
                code.lstore(keyBase + 2 * key);
                code.labelBinding(done);
            }
        }
        // nullMask = (byte) nullMask
        code.iload(nullMaskVar);
        code.i2b();
        code.istore(nullMaskVar);

        // gid = this.assignGroup(k0..kN-1, nullMask, groupId)
        code.aload(0);
        for (int key = 0; key < arity; key++) {
            code.lload(keyBase + 2 * key);
        }
        code.iload(nullMaskVar);
        code.lload(groupIdVar);
        code.invokevirtual(thisClass, assignGroupMethod, assignGroupType);
        code.lstore(gidVar);
        if (writeResults) {
            // result[pos] = gid
            code.aload(5);
            code.iload(pos);
            code.lload(gidVar);
            code.lastore();
        }
        // if (gid == groupId) groupId++
        code.lload(gidVar);
        code.lload(groupIdVar);
        code.lcmp();
        Label notNew = code.newLabel();
        code.ifne(notNew);
        if (distinct) {
            code.aload(5);
            code.iload(distinctCountVar);
            code.iload(pos);
            code.iastore();
            code.iinc(distinctCountVar, 1);
        }
        code.lload(groupIdVar);
        code.loadConstant(1L);
        code.ladd();
        code.lstore(groupIdVar);
        code.labelBinding(notNew);

        code.iinc(p, 1);
        code.goto_(loopTop);
        code.labelBinding(loopEnd);
        if (distinct) {
            code.iload(distinctCountVar);
            code.ireturn();
        }
        else {
            code.lload(groupIdVar);
            code.lreturn();
        }
    }

    // Physical carrier arrays and their exact row mappings are cast and hoisted once. The loop contains direct
    // primitive/mapping loads and the generated exact probe, with no VectorAccess or interface invocation.
    private static void emitAssignPhysicalBatch(
            CodeBuilder code,
            GenerationShape shape,
            ClassDesc thisClass,
            MethodTypeDesc assignGroupType,
            boolean distinct,
            boolean skipNulls)
    {
        int arity = shape.layout().laneSourceCounts().size();
        int sourceCount = shape.layout().sourceCarriers().size();
        int arrayBase = 14;
        int nullBase = arrayBase + sourceCount;
        int keyMappingBase = nullBase + arity;
        int nullMappingBase = keyMappingBase + sourceCount;
        int index = nullMappingBase + arity;
        int position = index + 1;
        int physicalPosition = position + 1;
        int keyBase = physicalPosition + 1;
        int nullMask = keyBase + 2 * arity;
        int groupId = nullMask + 1;
        int distinctCount = groupId + 2;

        for (int source = 0; source < sourceCount; source++) {
            code.aload(1);
            code.loadConstant(source);
            code.aaload();
            code.checkcast(switch (shape.layout().sourceCarriers().get(source)) {
                case I32 -> CD_RAW_INT_ARRAY;
                case I64 -> CD_LONG_ARRAY;
                case F64 -> CD_DOUBLE_ARRAY;
                case BOOLEAN -> CD_BOOLEAN_ARRAY;
            });
            code.astore(arrayBase + source);

            code.aload(2);
            code.loadConstant(source);
            code.aaload();
            code.astore(keyMappingBase + source);
        }

        for (int lane = 0; lane < arity; lane++) {
            code.aload(5);
            code.loadConstant(lane);
            code.aaload();
            code.astore(nullBase + lane);
            code.aload(6);
            code.loadConstant(lane);
            code.aaload();
            code.astore(nullMappingBase + lane);
        }

        code.loadConstant(0);
        code.istore(index);
        if (distinct) {
            code.loadConstant(0);
            code.istore(distinctCount);
        }
        Label loop = code.newLabel();
        Label end = code.newLabel();
        code.labelBinding(loop);
        code.iload(index);
        code.iload(10);
        code.if_icmpge(end);

        Label dense = code.newLabel();
        Label positionBound = code.newLabel();
        code.aload(9);
        code.ifnull(dense);
        code.aload(9);
        code.iload(index);
        code.iaload();
        code.istore(position);
        code.goto_(positionBound);
        code.labelBinding(dense);
        code.iload(index);
        code.istore(position);
        code.labelBinding(positionBound);

        code.loadConstant(0);
        code.istore(nullMask);
        int sourceOffset = 0;
        for (int lane = 0; lane < arity; lane++) {
            Label notNull = code.newLabel();
            Label loaded = code.newLabel();
            code.aload(nullBase + lane);
            code.ifnull(notNull);
            emitPhysicalPosition(code, position, physicalPosition, nullMappingBase + lane, 7, 8, lane);
            code.aload(nullBase + lane);
            code.iload(physicalPosition);
            code.baload();
            code.ifeq(notNull);
            code.iload(nullMask);
            code.loadConstant(1 << lane);
            code.ior();
            code.istore(nullMask);
            code.loadConstant(0L);
            code.lstore(keyBase + 2 * lane);
            code.goto_(loaded);
            code.labelBinding(notNull);
            emitProjectedLane(code, shape, lane, sourceOffset, position, physicalPosition, arrayBase, keyMappingBase, 3, 4);
            code.lstore(keyBase + 2 * lane);
            code.labelBinding(loaded);
            sourceOffset += shape.layout().laneSourceCounts().get(lane);
        }

        Label next = code.newLabel();
        if (skipNulls) {
            Label completeKey = code.newLabel();
            code.iload(nullMask);
            code.ifeq(completeKey);
            if (!distinct) {
                code.aload(11);
                code.iload(position);
                code.loadConstant(AbstractFixedWidthKeyTable.EMPTY_GROUP_ID);
                code.lastore();
            }
            code.goto_(next);
            code.labelBinding(completeKey);
        }

        code.aload(0);
        for (int lane = 0; lane < arity; lane++) {
            code.lload(keyBase + 2 * lane);
        }
        code.iload(nullMask);
        code.i2b();
        code.lload(12);
        code.invokevirtual(thisClass, distinct ? "assignDistinctGroup" : "assignGroup", assignGroupType);
        code.lstore(groupId);

        if (!distinct) {
            code.aload(11);
            code.iload(position);
            code.lload(groupId);
            code.lastore();
        }
        code.lload(groupId);
        code.lload(12);
        code.lcmp();
        Label existing = code.newLabel();
        code.ifne(existing);
        if (distinct) {
            code.aload(11);
            code.iload(distinctCount);
            code.iload(position);
            code.iastore();
            code.iinc(distinctCount, 1);
        }
        code.lload(12);
        code.loadConstant(1L);
        code.ladd();
        code.lstore(12);
        code.labelBinding(existing);

        code.labelBinding(next);
        code.iinc(index, 1);
        code.goto_(loop);
        code.labelBinding(end);
        if (distinct) {
            code.iload(distinctCount);
            code.ireturn();
        }
        else {
            code.lload(12);
            code.lreturn();
        }
    }

    private static void emitFindPhysicalBatch(
            CodeBuilder code,
            GenerationShape shape,
            ClassDesc thisClass,
            MethodTypeDesc findGroupType)
    {
        int arity = shape.layout().laneSourceCounts().size();
        int sourceCount = shape.layout().sourceCarriers().size();
        int arrayBase = 12;
        int nullBase = arrayBase + sourceCount;
        int keyMappingBase = nullBase + arity;
        int nullMappingBase = keyMappingBase + sourceCount;
        int index = nullMappingBase + arity;
        int position = index + 1;
        int physicalPosition = position + 1;
        int keyBase = physicalPosition + 1;
        int nullMask = keyBase + 2 * arity;
        int groupId = nullMask + 1;
        int matchCount = groupId + 2;

        for (int source = 0; source < sourceCount; source++) {
            code.aload(1);
            code.loadConstant(source);
            code.aaload();
            code.checkcast(switch (shape.layout().sourceCarriers().get(source)) {
                case I32 -> CD_RAW_INT_ARRAY;
                case I64 -> CD_LONG_ARRAY;
                case F64 -> CD_DOUBLE_ARRAY;
                case BOOLEAN -> CD_BOOLEAN_ARRAY;
            });
            code.astore(arrayBase + source);

            code.aload(2);
            code.loadConstant(source);
            code.aaload();
            code.astore(keyMappingBase + source);
        }

        for (int lane = 0; lane < arity; lane++) {
            code.aload(5);
            code.loadConstant(lane);
            code.aaload();
            code.astore(nullBase + lane);
            code.aload(6);
            code.loadConstant(lane);
            code.aaload();
            code.astore(nullMappingBase + lane);
        }

        code.loadConstant(0);
        code.istore(index);
        code.loadConstant(0);
        code.istore(matchCount);
        Label loop = code.newLabel();
        Label end = code.newLabel();
        code.labelBinding(loop);
        code.iload(index);
        code.iload(10);
        code.if_icmpge(end);

        Label dense = code.newLabel();
        Label positionBound = code.newLabel();
        code.aload(9);
        code.ifnull(dense);
        code.aload(9);
        code.iload(index);
        code.iaload();
        code.istore(position);
        code.goto_(positionBound);
        code.labelBinding(dense);
        code.iload(index);
        code.istore(position);
        code.labelBinding(positionBound);

        code.loadConstant(0);
        code.istore(nullMask);
        int sourceOffset = 0;
        for (int lane = 0; lane < arity; lane++) {
            Label notNull = code.newLabel();
            Label loaded = code.newLabel();
            code.aload(nullBase + lane);
            code.ifnull(notNull);
            emitPhysicalPosition(code, position, physicalPosition, nullMappingBase + lane, 7, 8, lane);
            code.aload(nullBase + lane);
            code.iload(physicalPosition);
            code.baload();
            code.ifeq(notNull);
            code.iload(nullMask);
            code.loadConstant(1 << lane);
            code.ior();
            code.istore(nullMask);
            code.loadConstant(0L);
            code.lstore(keyBase + 2 * lane);
            code.goto_(loaded);
            code.labelBinding(notNull);
            emitProjectedLane(code, shape, lane, sourceOffset, position, physicalPosition, arrayBase, keyMappingBase, 3, 4);
            code.lstore(keyBase + 2 * lane);
            code.labelBinding(loaded);
            sourceOffset += shape.layout().laneSourceCounts().get(lane);
        }

        Label lookup = code.newLabel();
        Label next = code.newLabel();
        code.iload(nullMask);
        code.ifeq(lookup);
        code.loadConstant(AbstractFixedWidthKeyTable.EMPTY_GROUP_ID);
        code.lstore(groupId);
        code.goto_(next);

        code.labelBinding(lookup);
        code.aload(0);
        for (int lane = 0; lane < arity; lane++) {
            code.lload(keyBase + 2 * lane);
        }
        code.loadConstant(0);
        code.i2b();
        code.loadConstant(0L);
        code.invokevirtual(thisClass, "findGroup", findGroupType);
        code.lstore(groupId);

        code.labelBinding(next);
        code.aload(11);
        code.iload(position);
        code.lload(groupId);
        code.lastore();
        code.lload(groupId);
        code.loadConstant(AbstractFixedWidthKeyTable.EMPTY_GROUP_ID);
        code.lcmp();
        Label miss = code.newLabel();
        code.ifeq(miss);
        code.iinc(matchCount, 1);
        code.labelBinding(miss);
        code.iinc(index, 1);
        code.goto_(loop);
        code.labelBinding(end);
        code.iload(matchCount);
        code.ireturn();
    }

    private static void emitExtractPhysicalKey(CodeBuilder code, GenerationShape shape)
    {
        int arity = shape.layout().laneSourceCounts().size();
        int sourceCount = shape.layout().sourceCarriers().size();
        int arrayBase = 12;
        int nullBase = arrayBase + sourceCount;
        int keyMappingBase = nullBase + arity;
        int nullMappingBase = keyMappingBase + sourceCount;
        int logicalPosition = nullMappingBase + arity;
        int physicalPosition = logicalPosition + 1;
        int nullMask = physicalPosition + 1;

        for (int source = 0; source < sourceCount; source++) {
            code.aload(1);
            code.loadConstant(source);
            code.aaload();
            code.checkcast(switch (shape.layout().sourceCarriers().get(source)) {
                case I32 -> CD_RAW_INT_ARRAY;
                case I64 -> CD_LONG_ARRAY;
                case F64 -> CD_DOUBLE_ARRAY;
                case BOOLEAN -> CD_BOOLEAN_ARRAY;
            });
            code.astore(arrayBase + source);
            code.aload(2);
            code.loadConstant(source);
            code.aaload();
            code.astore(keyMappingBase + source);
        }
        for (int lane = 0; lane < arity; lane++) {
            code.aload(5);
            code.loadConstant(lane);
            code.aaload();
            code.astore(nullBase + lane);
            code.aload(6);
            code.loadConstant(lane);
            code.aaload();
            code.astore(nullMappingBase + lane);
        }

        code.loadConstant(0);
        code.istore(nullMask);
        int sourceOffset = 0;
        for (int lane = 0; lane < arity; lane++) {
            code.aload(9);
            code.aload(10);
            code.loadConstant(lane);
            code.iaload();
            code.iaload();
            code.istore(logicalPosition);

            Label notNull = code.newLabel();
            Label loaded = code.newLabel();
            code.aload(nullBase + lane);
            code.ifnull(notNull);
            emitPhysicalPosition(code, logicalPosition, physicalPosition, nullMappingBase + lane, 7, 8, lane);
            code.aload(nullBase + lane);
            code.iload(physicalPosition);
            code.baload();
            code.ifeq(notNull);
            code.iload(nullMask);
            code.loadConstant(1 << lane);
            code.ior();
            code.istore(nullMask);
            code.aload(11);
            code.loadConstant(lane);
            code.loadConstant(0L);
            code.lastore();
            code.goto_(loaded);

            code.labelBinding(notNull);
            code.aload(11);
            code.loadConstant(lane);
            emitProjectedLane(code, shape, lane, sourceOffset, logicalPosition, physicalPosition, arrayBase, keyMappingBase, 3, 4);
            code.lastore();
            code.labelBinding(loaded);
            sourceOffset += shape.layout().laneSourceCounts().get(lane);
        }
        code.iload(nullMask);
        code.i2b();
        code.ireturn();
    }

    private static void emitProjectedLane(
            CodeBuilder code,
            GenerationShape shape,
            int lane,
            int sourceOffset,
            int logicalPosition,
            int physicalPosition,
            int arrayBase,
            int keyMappingBase,
            int mappingOffsetsParameter,
            int baseOffsetsParameter)
    {
        int sourceCount = shape.layout().laneSourceCounts().get(lane);
        List<ClassDesc> parameters = new ArrayList<>(sourceCount);
        for (int sourceInLane = 0; sourceInLane < sourceCount; sourceInLane++) {
            int source = sourceOffset + sourceInLane;
            FixedWidthKeyLayout.Carrier carrier = shape.layout().sourceCarriers().get(source);
            emitPhysicalPosition(
                    code,
                    logicalPosition,
                    physicalPosition,
                    keyMappingBase + source,
                    mappingOffsetsParameter,
                    baseOffsetsParameter,
                    source);
            code.aload(arrayBase + source);
            code.iload(physicalPosition);
            switch (carrier) {
                case I32 -> code.iaload();
                case I64 -> code.laload();
                case F64 -> code.daload();
                case BOOLEAN -> code.baload();
            }
            parameters.add(carrierDescriptor(carrier));
        }
        if (shape.layout().projections().get(lane).isPresent()) {
            code.invokedynamic(DynamicCallSiteDesc.of(
                    BSM_PROJECTION,
                    "projection" + lane,
                    MethodTypeDesc.of(CD_long, parameters.toArray(ClassDesc[]::new))));
            return;
        }
        switch (shape.layout().sourceCarriers().get(sourceOffset)) {
            case I32, BOOLEAN -> code.i2l();
            case I64 -> {}
            case F64 -> code.invokestatic(
                    ClassDesc.of("java.lang.Double"),
                    "doubleToRawLongBits",
                    MethodTypeDesc.of(CD_long, CD_double));
        }
    }

    private static ClassDesc carrierDescriptor(FixedWidthKeyLayout.Carrier carrier)
    {
        return switch (carrier) {
            case I32 -> CD_int;
            case I64 -> CD_long;
            case F64 -> CD_double;
            case BOOLEAN -> CD_boolean;
        };
    }

    private static void emitPhysicalPosition(
            CodeBuilder code,
            int logicalPosition,
            int physicalPosition,
            int mappingLocal,
            int mappingOffsetsParameter,
            int baseOffsetsParameter,
            int lane)
    {
        Label direct = code.newLabel();
        Label mapped = code.newLabel();
        code.aload(mappingLocal);
        code.ifnull(direct);
        code.aload(mappingLocal);
        code.iload(logicalPosition);
        code.aload(mappingOffsetsParameter);
        code.loadConstant(lane);
        code.iaload();
        code.iadd();
        code.iaload();
        code.istore(physicalPosition);
        code.goto_(mapped);
        code.labelBinding(direct);
        code.iload(logicalPosition);
        code.istore(physicalPosition);
        code.labelBinding(mapped);
        code.iload(physicalPosition);
        code.aload(baseOffsetsParameter);
        code.loadConstant(lane);
        code.iaload();
        code.iadd();
        code.istore(physicalPosition);
    }

    // int hashEntry(long[] table, int base, byte nullMask): locals this=0, table=1, base=2, nullMask=3
    private static void emitHashEntry(CodeBuilder code, int arity)
    {
        emitHash(code, arity,
                key -> {
                    code.aload(1);
                    code.iload(2);
                    code.loadConstant(key);
                    code.iadd();
                    code.laload();
                },
                () -> code.iload(3));
        code.ireturn();
    }

    // Pushes (int) Murmur3_fmix64( nullMask + sum_i keys[i] * HASH_PRIMES[i] ) onto the stack.
    private static void emitHash(CodeBuilder code, int arity, IntConsumer keyLoader, Runnable nullMaskLoader)
    {
        nullMaskLoader.run();
        code.i2l();
        for (int key = 0; key < arity; key++) {
            keyLoader.accept(key);
            code.loadConstant(AbstractFixedWidthKeyTable.HASH_PRIMES[key]);
            code.lmul();
            code.ladd();
        }
        emitXorShift(code);
        code.loadConstant(MURMUR_C1);
        code.lmul();
        emitXorShift(code);
        code.loadConstant(MURMUR_C2);
        code.lmul();
        emitXorShift(code);
        code.l2i();
    }

    // h ^= h >>> 33  (h on top of stack)
    private static void emitXorShift(CodeBuilder code)
    {
        code.dup2();
        code.loadConstant(MURMUR_SHIFT);
        code.lushr();
        code.lxor();
    }
}
