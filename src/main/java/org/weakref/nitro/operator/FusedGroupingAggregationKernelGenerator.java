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

import org.weakref.nitro.operator.aggregation.FusedAccumulatorSpec;

import java.lang.classfile.ClassFile;
import java.lang.classfile.CodeBuilder;
import java.lang.classfile.Label;
import java.lang.constant.ClassDesc;
import java.lang.constant.MethodTypeDesc;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.constant.ConstantDescs.CD_Object;
import static java.lang.constant.ConstantDescs.CD_int;
import static java.lang.constant.ConstantDescs.CD_long;
import static java.lang.constant.ConstantDescs.CD_void;

/**
 * Generates, once per accumulator-set shape, a {@link FusedGroupingKernel} whose hot loop is emitted
 * as JVM bytecode with the {@code java.lang.classfile} API. The generated {@code accumulate} inlines
 * the single-long open-addressed probe and, per accumulator, a single {@code increment(group, value)}
 * call against its state vector — so there is no group-id vector round-trip and every increment call
 * site is monomorphic. The generator hard-codes no aggregate function: it emits exactly what each
 * {@link FusedAccumulatorSpec} declares (state-vector type + value column or constant), so the set of
 * fusible aggregates grows by accumulators, not by changes here.
 */
final class FusedGroupingAggregationKernelGenerator
{
    private FusedGroupingAggregationKernelGenerator() {}

    private static final ClassDesc CD_KERNEL = ClassDesc.of("org.weakref.nitro.operator.FusedGroupingKernel");
    private static final ClassDesc CD_GROUPING_STATE = ClassDesc.of("org.weakref.nitro.operator.GroupingState");
    private static final ClassDesc CD_INT_ARRAY = CD_int.arrayType();
    private static final ClassDesc CD_LONG_ARRAY = CD_long.arrayType();
    private static final ClassDesc CD_LONG_ARRAY_2D = CD_long.arrayType().arrayType();
    private static final ClassDesc CD_OBJECT_ARRAY = CD_Object.arrayType();
    private static final MethodTypeDesc INCREMENT_TYPE = MethodTypeDesc.of(CD_void, CD_int, CD_long);

    // Parameter slots of accumulate(int[],int,long[],long[],int[],int,long[],long,long[][],Object[]).
    private static final int POSITIONS = 1;
    private static final int COUNT = 2;
    private static final int KEYS = 3;
    private static final int TABLE_KEYS = 4;
    private static final int TABLE_IDS = 5;
    private static final int TABLE_MASK = 6;
    private static final int KEYS_BY_GROUP = 7;
    private static final int START_NEXT_ID = 8;     // long, occupies 8-9
    private static final int INPUTS = 10;
    private static final int STATES = 11;
    // Locals.
    private static final int NEXT_ID = 12;          // long, occupies 12-13
    private static final int INDEX = 14;
    private static final int POSITION = 15;
    private static final int KEY = 16;              // long, occupies 16-17
    private static final int SLOT = 18;
    private static final int GROUP = 19;
    private static final int ID = 20;

    private static final ConcurrentHashMap<String, FusedGroupingKernel> KERNELS = new ConcurrentHashMap<>();

    static FusedGroupingKernel create(List<FusedAccumulatorSpec> specs)
    {
        return KERNELS.computeIfAbsent(cacheKey(specs), key -> generate(specs));
    }

    private static String cacheKey(List<FusedAccumulatorSpec> specs)
    {
        StringBuilder key = new StringBuilder();
        for (FusedAccumulatorSpec spec : specs) {
            key.append(spec.stateVectorType().getName()).append(spec.readsValue() ? ":v|" : ":c|");
        }
        return key.toString();
    }

    private static FusedGroupingKernel generate(List<FusedAccumulatorSpec> specs)
    {
        ClassDesc thisClass = ClassDesc.of("org.weakref.nitro.operator.GeneratedFusedGroupingKernel" + (KERNELS.size() + 1));
        MethodTypeDesc accumulateType = MethodTypeDesc.of(
                CD_long,
                CD_INT_ARRAY, CD_int, CD_LONG_ARRAY,
                CD_LONG_ARRAY, CD_INT_ARRAY, CD_int, CD_LONG_ARRAY,
                CD_long, CD_LONG_ARRAY_2D, CD_OBJECT_ARRAY);

        byte[] bytes = ClassFile.of().build(thisClass, builder -> {
            builder.withSuperclass(CD_Object);
            builder.withInterfaceSymbols(CD_KERNEL);
            builder.withFlags(ClassFile.ACC_FINAL | ClassFile.ACC_SYNTHETIC);

            builder.withMethodBody("<init>", MethodTypeDesc.of(CD_void), ClassFile.ACC_PUBLIC, code -> {
                code.aload(0);
                code.invokespecial(CD_Object, "<init>", MethodTypeDesc.of(CD_void));
                code.return_();
            });

            builder.withMethodBody("accumulate", accumulateType, ClassFile.ACC_PUBLIC, code -> emitAccumulate(code, specs));
        });

        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup()
                    .defineHiddenClass(bytes, true, MethodHandles.Lookup.ClassOption.NESTMATE);
            return (FusedGroupingKernel) lookup.findConstructor(lookup.lookupClass(), java.lang.invoke.MethodType.methodType(void.class))
                    .invoke();
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to generate fused grouping kernel", e);
        }
    }

    private static void emitAccumulate(CodeBuilder code, List<FusedAccumulatorSpec> specs)
    {
        // long nextId = startNextId;
        code.lload(START_NEXT_ID);
        code.lstore(NEXT_ID);

        Label sparse = code.newLabel();
        Label end = code.newLabel();

        // if (positions == null) dense else sparse
        code.aload(POSITIONS);
        code.ifnonnull(sparse);

        emitLoop(code, specs, false);
        code.goto_(end);

        code.labelBinding(sparse);
        emitLoop(code, specs, true);

        code.labelBinding(end);
        code.lload(NEXT_ID);
        code.lreturn();
    }

    // for (int index = 0; index < count; index++) { position = sparse ? positions[index] : index; <body> }
    private static void emitLoop(CodeBuilder code, List<FusedAccumulatorSpec> specs, boolean sparse)
    {
        code.loadConstant(0);
        code.istore(INDEX);

        Label top = code.newLabel();
        Label exit = code.newLabel();
        code.labelBinding(top);
        code.iload(INDEX);
        code.iload(COUNT);
        code.if_icmpge(exit);

        if (sparse) {
            code.aload(POSITIONS);
            code.iload(INDEX);
            code.iaload();
            code.istore(POSITION);
        }
        else {
            code.iload(INDEX);
            code.istore(POSITION);
        }

        emitProbeAndAccumulate(code, specs);

        code.iinc(INDEX, 1);
        code.goto_(top);
        code.labelBinding(exit);
    }

    private static void emitProbeAndAccumulate(CodeBuilder code, List<FusedAccumulatorSpec> specs)
    {
        // long key = keys[position];
        code.aload(KEYS);
        code.iload(POSITION);
        code.laload();
        code.lstore(KEY);

        // int slot = GroupingState.hashLong(key) & tableMask;
        code.lload(KEY);
        code.invokestatic(CD_GROUPING_STATE, "hashLong", MethodTypeDesc.of(CD_int, CD_long));
        code.iload(TABLE_MASK);
        code.iand();
        code.istore(SLOT);

        Label probeTop = code.newLabel();
        Label notEmpty = code.newLabel();
        Label advance = code.newLabel();
        Label accumulate = code.newLabel();

        code.labelBinding(probeTop);
        // int id = tableIds[slot];
        code.aload(TABLE_IDS);
        code.iload(SLOT);
        code.iaload();
        code.istore(ID);
        // if (id != -1) goto notEmpty;
        code.iload(ID);
        code.loadConstant(-1);
        code.if_icmpne(notEmpty);

        // empty slot: group = (int) nextId; nextId++;
        code.lload(NEXT_ID);
        code.l2i();
        code.istore(GROUP);
        code.lload(NEXT_ID);
        code.loadConstant(1L);
        code.ladd();
        code.lstore(NEXT_ID);
        // tableKeys[slot] = key;
        code.aload(TABLE_KEYS);
        code.iload(SLOT);
        code.lload(KEY);
        code.lastore();
        // tableIds[slot] = group;
        code.aload(TABLE_IDS);
        code.iload(SLOT);
        code.iload(GROUP);
        code.iastore();
        // keysByGroup[group] = key;
        code.aload(KEYS_BY_GROUP);
        code.iload(GROUP);
        code.lload(KEY);
        code.lastore();
        code.goto_(accumulate);

        // occupied slot: if (tableKeys[slot] == key) { group = id; goto accumulate; }
        code.labelBinding(notEmpty);
        code.aload(TABLE_KEYS);
        code.iload(SLOT);
        code.laload();
        code.lload(KEY);
        code.lcmp();
        code.ifne(advance);
        code.iload(ID);
        code.istore(GROUP);
        code.goto_(accumulate);

        // advance: slot = (slot + 1) & tableMask; goto probeTop;
        code.labelBinding(advance);
        code.iload(SLOT);
        code.loadConstant(1);
        code.iadd();
        code.iload(TABLE_MASK);
        code.iand();
        code.istore(SLOT);
        code.goto_(probeTop);

        code.labelBinding(accumulate);
        for (int accumulator = 0; accumulator < specs.size(); accumulator++) {
            FusedAccumulatorSpec spec = specs.get(accumulator);
            ClassDesc stateType = ClassDesc.of(spec.stateVectorType().getName());
            // ((StateType) states[a]).increment(group, value);
            code.aload(STATES);
            code.loadConstant(accumulator);
            code.aaload();
            code.checkcast(stateType);
            code.iload(GROUP);
            if (spec.readsValue()) {
                code.aload(INPUTS);
                code.loadConstant(accumulator);
                code.aaload();
                code.iload(POSITION);
                code.laload();
            }
            else {
                code.loadConstant(1L);
            }
            code.invokevirtual(stateType, "increment", INCREMENT_TYPE);
        }
    }
}
