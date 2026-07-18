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
import java.util.Arrays;
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
    private static final ClassDesc CD_INT_ARRAY_2D = ClassDesc.ofDescriptor("[[I");
    private static final ClassDesc CD_BOOLEAN_ARRAY_2D = ClassDesc.ofDescriptor("[[Z");
    private static final ClassDesc CD_OBJECT_ARRAY = CD_Object.arrayType();
    private static final MethodTypeDesc INCREMENT_TYPE = MethodTypeDesc.of(CD_void, CD_int, CD_long);

    // Parameter slots of FusedGroupingKernel.accumulate.
    private static final int POSITIONS = 1;
    private static final int COUNT = 2;
    private static final int KEYS = 3;
    private static final int KEY_IDS = 4;
    private static final int TABLE_KEYS = 5;
    private static final int TABLE_IDS = 6;
    private static final int TABLE_MASK = 7;
    private static final int KEYS_BY_GROUP = 8;
    private static final int START_NEXT_ID = 9;     // long, occupies 9-10
    private static final int OUTPUT_GROUPS = 11;
    private static final int INPUTS = 12;
    private static final int INPUT_IDS = 13;
    private static final int INPUT_NULLS = 14;
    private static final int INPUT_NULL_IDS = 15;
    private static final int STATES = 16;
    // Locals.
    private static final int NEXT_ID = 17;          // long, occupies 17-18
    private static final int INDEX = 19;
    private static final int POSITION = 20;
    private static final int KEY = 21;              // long, occupies 21-22
    private static final int SLOT = 23;
    private static final int GROUP = 24;
    private static final int ID = 25;
    private static final int KEY_POSITION = 26;
    private static final int CACHED_VALID = 27;
    private static final int CACHED_KEY = 28;       // long, occupies 28-29
    private static final int CACHED_GROUP = 30;
    private static final int INPUT_ARRAY_BASE = 31;
    private static final int ID_INDEXED_GROUP_MASK = 0x03FF_FFFF;

    private static final ConcurrentHashMap<String, FusedGroupingKernel> KERNELS = new ConcurrentHashMap<>();

    static FusedGroupingKernel create(
            List<FusedAccumulatorSpec> specs,
            boolean writeGroups,
            boolean intKey,
            boolean keyMapped,
            boolean runCache,
            boolean constantRuns,
            boolean directGrouping,
            boolean idIndexedGrouping,
            boolean[] intInputs,
            boolean[] mappedInputs,
            boolean[] mappedInputNulls,
            boolean[] inputUsesKeyIds,
            boolean[] inputNullUsesKeyIds)
    {
        String physicalShape = (intKey ? "i" : "l") + ":km=" + keyMapped + ":runs=" + runCache + ":constantRuns=" + constantRuns + ":direct=" + directGrouping + ":idIndexed=" + idIndexedGrouping + Arrays.toString(intInputs) + Arrays.toString(mappedInputs) + Arrays.toString(mappedInputNulls) + Arrays.toString(inputUsesKeyIds) + Arrays.toString(inputNullUsesKeyIds);
        return KERNELS.computeIfAbsent(
                cacheKey(specs) + ":groups=" + writeGroups + ":physical=" + physicalShape,
                key -> generate(specs, writeGroups, intKey, keyMapped, runCache, constantRuns, directGrouping, idIndexedGrouping, intInputs, mappedInputs, mappedInputNulls, inputUsesKeyIds, inputNullUsesKeyIds));
    }

    private static String cacheKey(List<FusedAccumulatorSpec> specs)
    {
        StringBuilder key = new StringBuilder();
        for (FusedAccumulatorSpec spec : specs) {
            key.append(spec.stateVectorType().getName()).append(':').append(spec.update()).append('|');
        }
        return key.toString();
    }

    private static FusedGroupingKernel generate(
            List<FusedAccumulatorSpec> specs,
            boolean writeGroups,
            boolean intKey,
            boolean keyMapped,
            boolean runCache,
            boolean constantRuns,
            boolean directGrouping,
            boolean idIndexedGrouping,
            boolean[] intInputs,
            boolean[] mappedInputs,
            boolean[] mappedInputNulls,
            boolean[] inputUsesKeyIds,
            boolean[] inputNullUsesKeyIds)
    {
        ClassDesc thisClass = ClassDesc.of("org.weakref.nitro.operator.GeneratedFusedGroupingKernel" + (KERNELS.size() + 1));
        MethodTypeDesc accumulateType = MethodTypeDesc.of(
                CD_long,
                CD_INT_ARRAY, CD_int, CD_Object, CD_INT_ARRAY,
                CD_LONG_ARRAY, CD_INT_ARRAY, CD_int, CD_LONG_ARRAY,
                CD_long, CD_LONG_ARRAY, CD_OBJECT_ARRAY, CD_INT_ARRAY_2D,
                CD_BOOLEAN_ARRAY_2D, CD_INT_ARRAY_2D, CD_OBJECT_ARRAY);

        byte[] bytes = ClassFile.of().build(thisClass, builder -> {
            builder.withSuperclass(CD_Object);
            builder.withInterfaceSymbols(CD_KERNEL);
            builder.withFlags(ClassFile.ACC_FINAL | ClassFile.ACC_SYNTHETIC);

            builder.withMethodBody("<init>", MethodTypeDesc.of(CD_void), ClassFile.ACC_PUBLIC, code -> {
                code.aload(0);
                code.invokespecial(CD_Object, "<init>", MethodTypeDesc.of(CD_void));
                code.return_();
            });

            builder.withMethodBody("accumulate", accumulateType, ClassFile.ACC_PUBLIC,
                    code -> emitAccumulate(code, specs, writeGroups, intKey, keyMapped, runCache, constantRuns, directGrouping, idIndexedGrouping, intInputs, mappedInputs, mappedInputNulls, inputUsesKeyIds, inputNullUsesKeyIds));
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

    private static void emitAccumulate(
            CodeBuilder code,
            List<FusedAccumulatorSpec> specs,
            boolean writeGroups,
            boolean intKey,
            boolean keyMapped,
            boolean runCache,
            boolean constantRuns,
            boolean directGrouping,
            boolean idIndexedGrouping,
            boolean[] intInputs,
            boolean[] mappedInputs,
            boolean[] mappedInputNulls,
            boolean[] inputUsesKeyIds,
            boolean[] inputNullUsesKeyIds)
    {
        // Resolve physical arrays once outside the row loop. The generated class has one concrete shape, so no
        // representation test or interface dispatch remains in the hot path.
        code.aload(KEYS);
        code.checkcast(intKey ? CD_INT_ARRAY : CD_LONG_ARRAY);
        code.astore(KEYS);
        for (int accumulator = 0; accumulator < specs.size(); accumulator++) {
            if (!specs.get(accumulator).readsValue()) {
                continue;
            }
            code.aload(INPUTS);
            code.loadConstant(accumulator);
            code.aaload();
            code.checkcast(intInputs[accumulator] ? CD_INT_ARRAY : CD_LONG_ARRAY);
            code.astore(INPUT_ARRAY_BASE + accumulator);
        }

        // long nextId = startNextId;
        code.lload(START_NEXT_ID);
        code.lstore(NEXT_ID);
        if (runCache) {
            code.loadConstant(0);
            code.istore(CACHED_VALID);
            code.loadConstant(0L);
            code.lstore(CACHED_KEY);
            code.loadConstant(0);
            code.istore(CACHED_GROUP);
        }
        if (batchesConstantRuns(specs, constantRuns)) {
            code.loadConstant(0);
            code.istore(runGroupLocal(specs));
            code.loadConstant(0);
            code.istore(runCountLocal(specs));
        }

        Label sparse = code.newLabel();
        Label end = code.newLabel();

        // if (positions == null) dense else sparse
        code.aload(POSITIONS);
        code.ifnonnull(sparse);

        emitLoop(code, specs, false, writeGroups, intKey, keyMapped, runCache, constantRuns, directGrouping, idIndexedGrouping, intInputs, mappedInputs, mappedInputNulls, inputUsesKeyIds, inputNullUsesKeyIds);
        code.goto_(end);

        code.labelBinding(sparse);
        emitLoop(code, specs, true, writeGroups, intKey, keyMapped, runCache, constantRuns, directGrouping, idIndexedGrouping, intInputs, mappedInputs, mappedInputNulls, inputUsesKeyIds, inputNullUsesKeyIds);

        code.labelBinding(end);
        code.lload(NEXT_ID);
        code.lreturn();
    }

    // for (int index = 0; index < count; index++) { position = sparse ? positions[index] : index; <body> }
    private static void emitLoop(CodeBuilder code, List<FusedAccumulatorSpec> specs, boolean sparse, boolean writeGroups, boolean intKey, boolean keyMapped, boolean runCache, boolean constantRuns, boolean directGrouping, boolean idIndexedGrouping, boolean[] intInputs, boolean[] mappedInputs, boolean[] mappedInputNulls, boolean[] inputUsesKeyIds, boolean[] inputNullUsesKeyIds)
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

        boolean batchConstantRuns = batchesConstantRuns(specs, constantRuns);
        emitProbeAndAccumulate(code, specs, writeGroups, intKey, keyMapped, runCache, batchConstantRuns, directGrouping, idIndexedGrouping, intInputs, mappedInputs, mappedInputNulls, inputUsesKeyIds, inputNullUsesKeyIds);

        code.iinc(INDEX, 1);
        code.goto_(top);
        code.labelBinding(exit);
        if (batchConstantRuns) {
            emitConstantRunFlush(code, specs);
        }
    }

    private static void emitProbeAndAccumulate(CodeBuilder code, List<FusedAccumulatorSpec> specs, boolean writeGroups, boolean intKey, boolean keyMapped, boolean runCache, boolean batchConstantRuns, boolean directGrouping, boolean idIndexedGrouping, boolean[] intInputs, boolean[] mappedInputs, boolean[] mappedInputNulls, boolean[] inputUsesKeyIds, boolean[] inputNullUsesKeyIds)
    {
        // long key = keys[position];
        code.aload(KEYS);
        if (keyMapped) {
            code.aload(KEY_IDS);
            code.iload(POSITION);
            code.iaload();
            code.dup();
            code.istore(KEY_POSITION);
        }
        else {
            code.iload(POSITION);
        }
        if (intKey) {
            code.iaload();
            code.i2l();
        }
        else {
            code.laload();
        }
        code.lstore(KEY);

        Label probeTop = code.newLabel();
        Label notEmpty = code.newLabel();
        Label advance = code.newLabel();
        Label accumulateNewRun = code.newLabel();
        Label accumulateSameRun = batchConstantRuns ? code.newLabel() : accumulateNewRun;
        Label accumulateCommon = batchConstantRuns ? code.newLabel() : accumulateNewRun;

        if (runCache) {
            Label probe = code.newLabel();
            code.iload(CACHED_VALID);
            code.ifeq(probe);
            code.lload(CACHED_KEY);
            code.lload(KEY);
            code.lcmp();
            code.ifne(probe);
            code.iload(CACHED_GROUP);
            code.istore(GROUP);
            code.goto_(accumulateSameRun);
            code.labelBinding(probe);
        }

        if (directGrouping) {
            // The batch boundary proved every key is a non-negative index into this table. Entries store group+1
            // so zero is the empty sentinel; there is no hash, collision loop, or key-equality branch.
            code.lload(KEY);
            code.l2i();
            code.istore(SLOT);
            code.aload(TABLE_IDS);
            code.iload(SLOT);
            code.iaload();
            code.istore(ID);
            code.iload(ID);
            code.ifne(notEmpty);

            code.lload(NEXT_ID);
            code.l2i();
            code.istore(GROUP);
            code.lload(NEXT_ID);
            code.loadConstant(1L);
            code.ladd();
            code.lstore(NEXT_ID);
            code.aload(TABLE_IDS);
            code.iload(SLOT);
            code.iload(GROUP);
            code.loadConstant(1);
            code.iadd();
            code.iastore();
            code.aload(KEYS_BY_GROUP);
            code.iload(GROUP);
            code.lload(KEY);
            code.lastore();
            emitRunCacheUpdate(code, runCache);
            code.goto_(accumulateNewRun);

            code.labelBinding(notEmpty);
            code.iload(ID);
            code.loadConstant(1);
            code.isub();
            code.istore(GROUP);
            emitRunCacheUpdate(code, runCache);
            code.goto_(accumulateNewRun);
        }
        else {
            // int slot = GroupingState.hashLong(key) & tableMask;
            code.lload(KEY);
            code.invokestatic(CD_GROUPING_STATE, "hashLong", MethodTypeDesc.of(CD_int, CD_long));
            if (idIndexedGrouping) {
                code.dup();
                code.istore(idIndexedHashLocal(specs));
            }
            code.iload(TABLE_MASK);
            code.iand();
            code.istore(SLOT);

            code.labelBinding(probeTop);
            // int id = tableIds[slot];
            code.aload(TABLE_IDS);
            code.iload(SLOT);
            code.iaload();
            code.istore(ID);
            // The ordinary table uses -1 as empty; packed id-indexed slots use zero and encode group+1.
            code.iload(ID);
            if (idIndexedGrouping) {
                code.ifne(notEmpty);
            }
            else {
                code.loadConstant(-1);
                code.if_icmpne(notEmpty);
            }

            // empty slot: group = (int) nextId; nextId++;
            code.lload(NEXT_ID);
            code.l2i();
            code.istore(GROUP);
            code.lload(NEXT_ID);
            code.loadConstant(1L);
            code.ladd();
            code.lstore(NEXT_ID);
            // The id-indexed layout stores the key only in the canonical reverse map below.
            if (!idIndexedGrouping) {
                code.aload(TABLE_KEYS);
                code.iload(SLOT);
                code.lload(KEY);
                code.lastore();
            }
            // tableIds[slot] = group;
            code.aload(TABLE_IDS);
            code.iload(SLOT);
            if (idIndexedGrouping) {
                code.iload(idIndexedHashLocal(specs));
                code.loadConstant(~ID_INDEXED_GROUP_MASK);
                code.iand();
                code.iload(GROUP);
                code.loadConstant(1);
                code.iadd();
                code.ior();
            }
            else {
                code.iload(GROUP);
            }
            code.iastore();
            // keysByGroup[group] = key;
            code.aload(KEYS_BY_GROUP);
            code.iload(GROUP);
            code.lload(KEY);
            code.lastore();
            emitRunCacheUpdate(code, runCache);
            code.goto_(accumulateNewRun);

            // occupied slot: if (tableKeys[slot] == key) { group = id; goto accumulate; }
            code.labelBinding(notEmpty);
            if (idIndexedGrouping) {
                code.iload(ID);
                code.loadConstant(~ID_INDEXED_GROUP_MASK);
                code.iand();
                code.iload(idIndexedHashLocal(specs));
                code.loadConstant(~ID_INDEXED_GROUP_MASK);
                code.iand();
                code.if_icmpne(advance);
                code.iload(ID);
                code.loadConstant(ID_INDEXED_GROUP_MASK);
                code.iand();
                code.loadConstant(1);
                code.isub();
                code.istore(GROUP);
                code.aload(KEYS_BY_GROUP);
                code.iload(GROUP);
            }
            else {
                code.aload(TABLE_KEYS);
                code.iload(SLOT);
            }
            code.laload();
            code.lload(KEY);
            code.lcmp();
            code.ifne(advance);
            if (!idIndexedGrouping) {
                code.iload(ID);
                code.istore(GROUP);
            }
            emitRunCacheUpdate(code, runCache);
            code.goto_(accumulateNewRun);

            // advance: slot = (slot + 1) & tableMask; goto probeTop;
            code.labelBinding(advance);
            code.iload(SLOT);
            code.loadConstant(1);
            code.iadd();
            code.iload(TABLE_MASK);
            code.iand();
            code.istore(SLOT);
            code.goto_(probeTop);
        }

        code.labelBinding(accumulateNewRun);
        if (batchConstantRuns) {
            int runCount = runCountLocal(specs);
            Label firstRun = code.newLabel();
            code.iload(runCount);
            code.ifeq(firstRun);
            emitConstantRunFlush(code, specs);
            code.labelBinding(firstRun);
            code.iload(GROUP);
            code.istore(runGroupLocal(specs));
            code.loadConstant(1);
            code.istore(runCount);
            code.goto_(accumulateCommon);

            code.labelBinding(accumulateSameRun);
            code.iinc(runCount, 1);
            code.labelBinding(accumulateCommon);
        }
        if (writeGroups) {
            code.aload(OUTPUT_GROUPS);
            code.iload(POSITION);
            code.iload(GROUP);
            code.i2l();
            code.lastore();
        }
        boolean[] emitted = new boolean[specs.size()];
        for (int accumulator = 0; accumulator < specs.size(); accumulator++) {
            if (emitted[accumulator]) {
                continue;
            }
            FusedAccumulatorSpec spec = specs.get(accumulator);
            if (!spec.readsInput()) {
                if (!batchConstantRuns) {
                    emitIncrement(code, spec, accumulator, intInputs, mappedInputs, inputUsesKeyIds);
                }
                emitted[accumulator] = true;
                continue;
            }

            // Accumulators over the same input share SQL null-elision semantics. Test that input's NULLS once,
            // then update every matching state in the same generated block (e.g. SUM(x), COUNT(x)).
            Label increment = code.newLabel();
            Label nextInput = code.newLabel();
            code.aload(INPUT_NULLS);
            code.loadConstant(accumulator);
            code.aaload();
            code.ifnull(increment);
            code.aload(INPUT_NULLS);
            code.loadConstant(accumulator);
            code.aaload();
            if (inputNullUsesKeyIds[accumulator]) {
                code.iload(KEY_POSITION);
            }
            else if (mappedInputNulls[accumulator]) {
                code.aload(INPUT_NULL_IDS);
                code.loadConstant(accumulator);
                code.aaload();
                code.iload(POSITION);
                code.iaload();
            }
            else {
                code.iload(POSITION);
            }
            code.baload();
            code.ifne(nextInput);
            code.labelBinding(increment);
            for (int candidate = accumulator; candidate < specs.size(); candidate++) {
                FusedAccumulatorSpec candidateSpec = specs.get(candidate);
                if (!emitted[candidate]
                        && candidateSpec.readsInput()
                        && candidateSpec.inputColumn() == spec.inputColumn()) {
                    emitIncrement(code, candidateSpec, candidate, intInputs, mappedInputs, inputUsesKeyIds);
                    emitted[candidate] = true;
                }
            }
            code.labelBinding(nextInput);
        }
    }

    private static boolean batchesConstantRuns(List<FusedAccumulatorSpec> specs, boolean constantRuns)
    {
        if (!constantRuns) {
            return false;
        }
        boolean hasInputIndependent = false;
        for (FusedAccumulatorSpec spec : specs) {
            if (!spec.readsInput()) {
                hasInputIndependent = true;
            }
            else {
                // Keep this representation coherent: a constant-only kernel can advance all state once per
                // physical key run. Mixed input kernels still walk every row and retain their ordinary updates.
                return false;
            }
        }
        return hasInputIndependent;
    }

    private static int runGroupLocal(List<FusedAccumulatorSpec> specs)
    {
        return INPUT_ARRAY_BASE + specs.size();
    }

    private static int runCountLocal(List<FusedAccumulatorSpec> specs)
    {
        return runGroupLocal(specs) + 1;
    }

    private static int idIndexedHashLocal(List<FusedAccumulatorSpec> specs)
    {
        return runCountLocal(specs) + 1;
    }

    /** Coalesces input-independent constant updates across a physically adjacent key run. */
    private static void emitConstantRunFlush(CodeBuilder code, List<FusedAccumulatorSpec> specs)
    {
        int runGroup = runGroupLocal(specs);
        int runCount = runCountLocal(specs);
        for (int accumulator = 0; accumulator < specs.size(); accumulator++) {
            FusedAccumulatorSpec spec = specs.get(accumulator);
            if (spec.readsInput()) {
                continue;
            }
            ClassDesc stateType = ClassDesc.of(spec.stateVectorType().getName());
            code.aload(STATES);
            code.loadConstant(accumulator);
            code.aaload();
            code.checkcast(stateType);
            code.iload(runGroup);
            code.iload(runCount);
            code.i2l();
            code.invokevirtual(stateType, "increment", INCREMENT_TYPE);
        }
    }

    private static void emitRunCacheUpdate(CodeBuilder code, boolean runCache)
    {
        if (!runCache) {
            return;
        }
        code.loadConstant(1);
        code.istore(CACHED_VALID);
        code.lload(KEY);
        code.lstore(CACHED_KEY);
        code.iload(GROUP);
        code.istore(CACHED_GROUP);
    }

    private static void emitIncrement(
            CodeBuilder code,
            FusedAccumulatorSpec spec,
            int accumulator,
            boolean[] intInputs,
            boolean[] mappedInputs,
            boolean[] inputUsesKeyIds)
    {
        ClassDesc stateType = ClassDesc.of(spec.stateVectorType().getName());
        code.aload(STATES);
        code.loadConstant(accumulator);
        code.aaload();
        code.checkcast(stateType);
        code.iload(GROUP);
        if (spec.readsValue()) {
            code.aload(INPUT_ARRAY_BASE + accumulator);
            if (inputUsesKeyIds[accumulator]) {
                code.iload(KEY_POSITION);
            }
            else if (mappedInputs[accumulator]) {
                code.aload(INPUT_IDS);
                code.loadConstant(accumulator);
                code.aaload();
                code.iload(POSITION);
                code.iaload();
            }
            else {
                code.iload(POSITION);
            }
            if (intInputs[accumulator]) {
                code.iaload();
                code.i2l();
            }
            else {
                code.laload();
            }
        }
        else {
            code.loadConstant(1L);
        }
        code.invokevirtual(stateType, "increment", INCREMENT_TYPE);
    }
}
