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

import org.weakref.nitro.core.function.aggregation.ContributionCarrier;
import org.weakref.nitro.core.function.aggregation.GroupedAggregationUpdate;

import java.lang.classfile.ClassFile;
import java.lang.classfile.CodeBuilder;
import java.lang.classfile.Label;
import java.lang.constant.ClassDesc;
import java.lang.constant.DirectMethodHandleDesc;
import java.lang.constant.DynamicCallSiteDesc;
import java.lang.constant.MethodTypeDesc;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.constant.ConstantDescs.CD_CallSite;
import static java.lang.constant.ConstantDescs.CD_Object;
import static java.lang.constant.ConstantDescs.CD_boolean;
import static java.lang.constant.ConstantDescs.CD_byte;
import static java.lang.constant.ConstantDescs.CD_double;
import static java.lang.constant.ConstantDescs.CD_int;
import static java.lang.constant.ConstantDescs.CD_long;
import static java.lang.constant.ConstantDescs.CD_void;
import static java.lang.constant.DirectMethodHandleDesc.Kind.STATIC;
import static java.lang.constant.MethodHandleDesc.ofMethod;

/**
 * Generates, once per accumulator-set shape and resource owner, a {@link FusedGroupingKernel} whose hot loop is emitted
 * as JVM bytecode with the {@code java.lang.classfile} API. The generated {@code accumulate} inlines
 * the single-long open-addressed probe and, per accumulator, a constant-linked provider state update — so there is
 * no group-id vector round-trip.
 * The generator hard-codes no aggregate function or provider state class: it emits exactly the contribution each
 * {@link GroupedAggregationUpdate} declares, and unsupported invocation conventions retain the ordinary
 * accumulator fallback.
 */
final class FusedGroupingAggregationKernelGenerator
        implements AutoCloseable
{
    private static final ClassDesc CD_KERNEL = ClassDesc.of("org.weakref.nitro.operator.FusedGroupingKernel");
    private static final ClassDesc CD_GROUPING_STATE = ClassDesc.of("org.weakref.nitro.operator.GroupingState");
    private static final ClassDesc CD_BOOTSTRAP = ClassDesc.of("org.weakref.nitro.operator.FusedAggregationUpdateBootstrap");
    private static final ClassDesc CD_INT_ARRAY = CD_int.arrayType();
    private static final ClassDesc CD_LONG_ARRAY = CD_long.arrayType();
    private static final ClassDesc CD_DOUBLE_ARRAY = CD_double.arrayType();
    private static final ClassDesc CD_BOOLEAN_ARRAY = CD_boolean.arrayType();
    private static final ClassDesc CD_BYTE_ARRAY = CD_byte.arrayType();
    private static final ClassDesc CD_INT_ARRAY_2D = ClassDesc.ofDescriptor("[[I");
    private static final ClassDesc CD_BOOLEAN_ARRAY_2D = ClassDesc.ofDescriptor("[[Z");
    private static final ClassDesc CD_OBJECT_ARRAY = CD_Object.arrayType();
    private static final DirectMethodHandleDesc BSM_UPDATE = ofMethod(
            STATIC,
            CD_BOOTSTRAP,
            "bootstrap",
            MethodTypeDesc.of(CD_CallSite, ClassDesc.of("java.lang.invoke.MethodHandles$Lookup"), ClassDesc.of("java.lang.String"), ClassDesc.of("java.lang.invoke.MethodType")));

    // Parameter slots of FusedGroupingKernel.accumulate.
    private static final int POSITIONS = 1;
    private static final int COUNT = 2;
    private static final int KEYS = 3;
    private static final int KEY_IDS = 4;
    private static final int KEY_OFFSET = 5;
    private static final int TABLE_KEYS = 6;
    private static final int TABLE_IDS = 7;
    private static final int TABLE_MASK = 8;
    private static final int KEYS_BY_GROUP = 9;
    private static final int START_NEXT_ID = 10;     // long, occupies 10-11
    private static final int OUTPUT_GROUPS = 12;
    private static final int INPUTS = 13;
    private static final int INPUT_VALUE_OFFSETS = 14;
    private static final int INPUT_IDS = 15;
    private static final int INPUT_OFFSETS = 16;
    private static final int INPUT_NULLS = 17;
    private static final int INPUT_NULL_IDS = 18;
    private static final int INPUT_NULL_OFFSETS = 19;
    private static final int STATES = 20;
    // Locals.
    private static final int NEXT_ID = 21;          // long, occupies 21-22
    private static final int INDEX = 23;
    private static final int POSITION = 24;
    private static final int KEY = 25;              // long, occupies 25-26
    private static final int SLOT = 27;
    private static final int GROUP = 28;
    private static final int ID = 29;
    private static final int KEY_POSITION = 30;
    private static final int CACHED_VALID = 31;
    private static final int CACHED_KEY = 32;       // long, occupies 32-33
    private static final int CACHED_GROUP = 34;
    private static final int VALUE_POSITION = 35;
    private static final int INPUT_ARRAY_BASE = 36;
    private static final int ID_INDEXED_GROUP_MASK = 0x03FF_FFFF;

    private final ConcurrentHashMap<KernelKey, FusedGroupingKernel> kernels = new ConcurrentHashMap<>();
    private final AtomicInteger nextClassId = new AtomicInteger();
    private boolean closed;

    FusedGroupingKernel create(
            List<GroupedAggregationUpdate> specs,
            boolean writeGroups,
            boolean intKey,
            boolean keyMapped,
            boolean preResolvedKeyDomain,
            boolean runCache,
            boolean constantRuns,
            boolean directGrouping,
            boolean idIndexedGrouping,
            boolean keyOffsetInput,
            boolean[] intInputs,
            ContributionCarrier[] inputCarriers,
            boolean[] mappedInputs,
            boolean[] mappedInputNulls,
            boolean[] inputUsesKeyIds,
            boolean[] inputNullUsesKeyIds,
            boolean[] offsetInputs,
            boolean[] offsetInputNulls)
    {
        checkOpen();
        String physicalShape = (intKey ? "i" : "l") + ":km=" + keyMapped + ":ko=" + keyOffsetInput + ":resolved=" + preResolvedKeyDomain + ":runs=" + runCache + ":constantRuns=" + constantRuns + ":direct=" + directGrouping + ":idIndexed=" + idIndexedGrouping + Arrays.toString(intInputs) + Arrays.toString(inputCarriers) + Arrays.toString(mappedInputs) + Arrays.toString(mappedInputNulls) + Arrays.toString(inputUsesKeyIds) + Arrays.toString(inputNullUsesKeyIds) + Arrays.toString(offsetInputs) + Arrays.toString(offsetInputNulls);
        return kernels.computeIfAbsent(
                new KernelKey(List.copyOf(specs), writeGroups, physicalShape),
                key -> generate(specs, writeGroups, intKey, keyMapped, preResolvedKeyDomain, runCache, constantRuns, directGrouping, idIndexedGrouping, keyOffsetInput, intInputs, inputCarriers, mappedInputs, mappedInputNulls, inputUsesKeyIds, inputNullUsesKeyIds, offsetInputs, offsetInputNulls));
    }

    private FusedGroupingKernel generate(
            List<GroupedAggregationUpdate> specs,
            boolean writeGroups,
            boolean intKey,
            boolean keyMapped,
            boolean preResolvedKeyDomain,
            boolean runCache,
            boolean constantRuns,
            boolean directGrouping,
            boolean idIndexedGrouping,
            boolean keyOffsetInput,
            boolean[] intInputs,
            ContributionCarrier[] inputCarriers,
            boolean[] mappedInputs,
            boolean[] mappedInputNulls,
            boolean[] inputUsesKeyIds,
            boolean[] inputNullUsesKeyIds,
            boolean[] offsetInputs,
            boolean[] offsetInputNulls)
    {
        ClassDesc thisClass = ClassDesc.of("org.weakref.nitro.operator.GeneratedFusedGroupingKernel" + nextClassId.incrementAndGet());
        MethodTypeDesc accumulateType = MethodTypeDesc.of(
                CD_long,
                CD_INT_ARRAY, CD_int, CD_Object, CD_INT_ARRAY, CD_int,
                CD_LONG_ARRAY, CD_INT_ARRAY, CD_int, CD_LONG_ARRAY,
                CD_long, CD_LONG_ARRAY, CD_OBJECT_ARRAY, CD_INT_ARRAY_2D, CD_INT_ARRAY_2D, CD_INT_ARRAY,
                CD_BOOLEAN_ARRAY_2D, CD_INT_ARRAY_2D, CD_INT_ARRAY, CD_OBJECT_ARRAY);

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
                    code -> emitAccumulate(code, specs, writeGroups, intKey, keyMapped, preResolvedKeyDomain, runCache, constantRuns, directGrouping, idIndexedGrouping, keyOffsetInput, intInputs, inputCarriers, mappedInputs, mappedInputNulls, inputUsesKeyIds, inputNullUsesKeyIds, offsetInputs, offsetInputNulls));
        });

        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup()
                    .defineHiddenClassWithClassData(
                            bytes,
                            specs.stream().map(GroupedAggregationUpdate::target).toList(),
                            true,
                            MethodHandles.Lookup.ClassOption.NESTMATE);
            return (FusedGroupingKernel) lookup.findConstructor(lookup.lookupClass(), java.lang.invoke.MethodType.methodType(void.class))
                    .invoke();
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to generate fused grouping kernel", e);
        }
    }

    @Override
    public void close()
    {
        closed = true;
        kernels.clear();
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("Fused grouping kernel generator is closed");
        }
    }

    private record KernelKey(List<GroupedAggregationUpdate> updates, boolean writeGroups, String physicalShape) {}

    private static void emitAccumulate(
            CodeBuilder code,
            List<GroupedAggregationUpdate> specs,
            boolean writeGroups,
            boolean intKey,
            boolean keyMapped,
            boolean preResolvedKeyDomain,
            boolean runCache,
            boolean constantRuns,
            boolean directGrouping,
            boolean idIndexedGrouping,
            boolean keyOffsetInput,
            boolean[] intInputs,
            ContributionCarrier[] inputCarriers,
            boolean[] mappedInputs,
            boolean[] mappedInputNulls,
            boolean[] inputUsesKeyIds,
            boolean[] inputNullUsesKeyIds,
            boolean[] offsetInputs,
            boolean[] offsetInputNulls)
    {
        // Resolve physical arrays once outside the row loop. The generated class has one concrete shape, so no
        // representation test or interface dispatch remains in the hot path.
        code.aload(KEYS);
        code.checkcast(intKey ? CD_INT_ARRAY : CD_LONG_ARRAY);
        code.astore(KEYS);
        int input = 0;
        for (GroupedAggregationUpdate spec : specs) {
            for (int contribution = 0; contribution < spec.contributions().size(); contribution++, input++) {
                if (!spec.readsValue(contribution)) {
                    continue;
                }
                code.aload(INPUTS);
                code.loadConstant(input);
                code.aaload();
                code.checkcast(switch (inputCarriers[input]) {
                    case DOUBLE -> CD_DOUBLE_ARRAY;
                    case BOOLEAN -> CD_BOOLEAN_ARRAY;
                    case LONG -> intInputs[input] ? CD_INT_ARRAY : CD_LONG_ARRAY;
                    case BINARY_REGION -> CD_BYTE_ARRAY;
                });
                code.astore(INPUT_ARRAY_BASE + input);
            }
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

        emitLoop(code, specs, false, writeGroups, intKey, keyMapped, preResolvedKeyDomain, runCache, constantRuns, directGrouping, idIndexedGrouping, keyOffsetInput, intInputs, inputCarriers, mappedInputs, mappedInputNulls, inputUsesKeyIds, inputNullUsesKeyIds, offsetInputs, offsetInputNulls);
        code.goto_(end);

        code.labelBinding(sparse);
        emitLoop(code, specs, true, writeGroups, intKey, keyMapped, preResolvedKeyDomain, runCache, constantRuns, directGrouping, idIndexedGrouping, keyOffsetInput, intInputs, inputCarriers, mappedInputs, mappedInputNulls, inputUsesKeyIds, inputNullUsesKeyIds, offsetInputs, offsetInputNulls);

        code.labelBinding(end);
        code.lload(NEXT_ID);
        code.lreturn();
    }

    // for (int index = 0; index < count; index++) { position = sparse ? positions[index] : index; <body> }
    private static void emitLoop(CodeBuilder code, List<GroupedAggregationUpdate> specs, boolean sparse, boolean writeGroups, boolean intKey, boolean keyMapped, boolean preResolvedKeyDomain, boolean runCache, boolean constantRuns, boolean directGrouping, boolean idIndexedGrouping, boolean keyOffsetInput, boolean[] intInputs, ContributionCarrier[] inputCarriers, boolean[] mappedInputs, boolean[] mappedInputNulls, boolean[] inputUsesKeyIds, boolean[] inputNullUsesKeyIds, boolean[] offsetInputs, boolean[] offsetInputNulls)
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
        emitProbeAndAccumulate(code, specs, writeGroups, intKey, keyMapped, preResolvedKeyDomain, runCache, batchConstantRuns, directGrouping, idIndexedGrouping, keyOffsetInput, intInputs, inputCarriers, mappedInputs, mappedInputNulls, inputUsesKeyIds, inputNullUsesKeyIds, offsetInputs, offsetInputNulls);

        code.iinc(INDEX, 1);
        code.goto_(top);
        code.labelBinding(exit);
        if (batchConstantRuns) {
            emitConstantRunFlush(code, specs);
        }
    }

    private static void emitProbeAndAccumulate(CodeBuilder code, List<GroupedAggregationUpdate> specs, boolean writeGroups, boolean intKey, boolean keyMapped, boolean preResolvedKeyDomain, boolean runCache, boolean batchConstantRuns, boolean directGrouping, boolean idIndexedGrouping, boolean keyOffsetInput, boolean[] intInputs, ContributionCarrier[] inputCarriers, boolean[] mappedInputs, boolean[] mappedInputNulls, boolean[] inputUsesKeyIds, boolean[] inputNullUsesKeyIds, boolean[] offsetInputs, boolean[] offsetInputNulls)
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
        if (keyOffsetInput) {
            code.iload(KEY_OFFSET);
            code.iadd();
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

        if (preResolvedKeyDomain) {
            // The batch boundary resolved each referenced physical dictionary key once. TABLE_IDS is the compact
            // domain-id -> group-id map for this shape, so the generated row loop retains its monomorphic state
            // updates while eliminating per-logical-row hashing and collision probes.
            code.aload(TABLE_IDS);
            code.iload(KEY_POSITION);
            code.iaload();
            code.istore(GROUP);
            code.goto_(accumulateNewRun);
        }
        else if (runCache) {
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
            GroupedAggregationUpdate spec = specs.get(accumulator);
            if (spec.contributions().size() > 1) {
                emitMultiInputIncrement(
                        code,
                        spec,
                        accumulator,
                        inputOffset(specs, accumulator),
                        intInputs,
                        inputCarriers,
                        mappedInputs,
                        mappedInputNulls,
                        inputUsesKeyIds,
                        inputNullUsesKeyIds,
                        offsetInputs,
                        offsetInputNulls);
                emitted[accumulator] = true;
                continue;
            }
            if (!spec.readsInput()) {
                if (!batchConstantRuns) {
                    emitIncrement(code, spec, accumulator, inputOffset(specs, accumulator), intInputs, inputCarriers, mappedInputs, inputUsesKeyIds, offsetInputs);
                }
                emitted[accumulator] = true;
                continue;
            }

            // Accumulators over the same input share SQL null-elision semantics. Test that input's NULLS once,
            // then update every matching state in the same generated block (e.g. SUM(x), COUNT(x)).
            Label increment = code.newLabel();
            Label nextInput = code.newLabel();
            int accumulatorInput = inputOffset(specs, accumulator);
            code.aload(INPUT_NULLS);
            code.loadConstant(accumulatorInput);
            code.aaload();
            code.ifnull(increment);
            code.aload(INPUT_NULLS);
            code.loadConstant(accumulatorInput);
            code.aaload();
            if (inputNullUsesKeyIds[accumulatorInput]) {
                code.iload(KEY_POSITION);
            }
            else if (mappedInputNulls[accumulatorInput]) {
                code.aload(INPUT_NULL_IDS);
                code.loadConstant(accumulatorInput);
                code.aaload();
                code.iload(POSITION);
                code.iaload();
            }
            else {
                code.iload(POSITION);
            }
            if (offsetInputNulls[accumulatorInput]) {
                code.aload(INPUT_NULL_OFFSETS);
                code.loadConstant(accumulatorInput);
                code.iaload();
                code.iadd();
            }
            code.baload();
            code.ifne(nextInput);
            code.labelBinding(increment);
            for (int candidate = accumulator; candidate < specs.size(); candidate++) {
                GroupedAggregationUpdate candidateSpec = specs.get(candidate);
                if (!emitted[candidate]
                        && candidateSpec.contributions().size() == 1
                        && candidateSpec.readsInput()
                        && candidateSpec.inputColumn() == spec.inputColumn()) {
                    emitIncrement(code, candidateSpec, candidate, inputOffset(specs, candidate), intInputs, inputCarriers, mappedInputs, inputUsesKeyIds, offsetInputs);
                    emitted[candidate] = true;
                }
            }
            code.labelBinding(nextInput);
        }
    }

    private static boolean batchesConstantRuns(List<GroupedAggregationUpdate> specs, boolean constantRuns)
    {
        if (!constantRuns) {
            return false;
        }
        boolean hasInputIndependent = false;
        for (GroupedAggregationUpdate spec : specs) {
            if (!spec.readsInput()) {
                hasInputIndependent = true;
                if (spec.target().repeatedUpdate().isEmpty()) {
                    // Coalescing changes the call sequence. Only a provider-declared repeated update can preserve
                    // general aggregation semantics over the run's logical multiplicity.
                    return false;
                }
            }
            else {
                // Keep this representation coherent: a constant-only kernel can advance all state once per
                // physical key run. Mixed input kernels still walk every row and retain their ordinary updates.
                return false;
            }
        }
        return hasInputIndependent;
    }

    private static int runGroupLocal(List<GroupedAggregationUpdate> specs)
    {
        return INPUT_ARRAY_BASE + inputCount(specs);
    }

    private static int inputCount(List<GroupedAggregationUpdate> specs)
    {
        return specs.stream().mapToInt(spec -> spec.contributions().size()).sum();
    }

    private static int inputOffset(List<GroupedAggregationUpdate> specs, int accumulator)
    {
        int offset = 0;
        for (int index = 0; index < accumulator; index++) {
            offset += specs.get(index).contributions().size();
        }
        return offset;
    }

    private static int runCountLocal(List<GroupedAggregationUpdate> specs)
    {
        return runGroupLocal(specs) + 1;
    }

    private static int idIndexedHashLocal(List<GroupedAggregationUpdate> specs)
    {
        return runCountLocal(specs) + 1;
    }

    /** Coalesces input-independent constant updates across a physically adjacent key run. */
    private static void emitConstantRunFlush(CodeBuilder code, List<GroupedAggregationUpdate> specs)
    {
        int runGroup = runGroupLocal(specs);
        int runCount = runCountLocal(specs);
        for (int accumulator = 0; accumulator < specs.size(); accumulator++) {
            GroupedAggregationUpdate spec = specs.get(accumulator);
            if (spec.readsInput()) {
                continue;
            }
            code.aload(STATES);
            code.loadConstant(accumulator);
            code.aaload();
            code.iload(runGroup);
            for (int contribution = 0; contribution < spec.contributions().size(); contribution++) {
                code.loadConstant(spec.constantValue(contribution));
            }
            code.iload(runCount);
            emitUpdateInvocation(code, specs.get(accumulator), accumulator, true);
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
            GroupedAggregationUpdate spec,
            int accumulator,
            int inputOffset,
            boolean[] intInputs,
            ContributionCarrier[] inputCarriers,
            boolean[] mappedInputs,
            boolean[] inputUsesKeyIds,
            boolean[] offsetInputs)
    {
        code.aload(STATES);
        code.loadConstant(accumulator);
        code.aaload();
        code.iload(GROUP);
        int input = inputOffset;
        for (int contribution = 0; contribution < spec.contributions().size(); contribution++, input++) {
            if (spec.readsValue(contribution)) {
                code.aload(INPUT_ARRAY_BASE + input);
                if (inputUsesKeyIds[input]) {
                    code.iload(KEY_POSITION);
                }
                else if (mappedInputs[input]) {
                    code.aload(INPUT_IDS);
                    code.loadConstant(input);
                    code.aaload();
                    code.iload(POSITION);
                    code.iaload();
                }
                else {
                    code.iload(POSITION);
                }
                if (offsetInputs[input]) {
                    code.aload(INPUT_OFFSETS);
                    code.loadConstant(input);
                    code.iaload();
                    code.iadd();
                }
                switch (inputCarriers[input]) {
                    case DOUBLE -> code.daload();
                    case BOOLEAN -> code.baload();
                    case LONG -> {
                        if (intInputs[input]) {
                            code.iaload();
                            code.i2l();
                        }
                        else {
                            code.laload();
                        }
                    }
                    case BINARY_REGION -> {
                        code.istore(VALUE_POSITION);
                        code.aload(INPUT_VALUE_OFFSETS);
                        code.loadConstant(input);
                        code.aaload();
                        code.iload(VALUE_POSITION);
                        code.iaload();
                        code.aload(INPUT_VALUE_OFFSETS);
                        code.loadConstant(input);
                        code.aaload();
                        code.iload(VALUE_POSITION);
                        code.loadConstant(1);
                        code.iadd();
                        code.iaload();
                        code.aload(INPUT_VALUE_OFFSETS);
                        code.loadConstant(input);
                        code.aaload();
                        code.iload(VALUE_POSITION);
                        code.iaload();
                        code.isub();
                    }
                }
            }
            else {
                code.loadConstant(spec.constantValue(contribution));
            }
        }
        emitUpdateInvocation(code, spec, accumulator, false);
    }

    private static void emitMultiInputIncrement(
            CodeBuilder code,
            GroupedAggregationUpdate spec,
            int accumulator,
            int inputOffset,
            boolean[] intInputs,
            ContributionCarrier[] inputCarriers,
            boolean[] mappedInputs,
            boolean[] mappedInputNulls,
            boolean[] inputUsesKeyIds,
            boolean[] inputNullUsesKeyIds,
            boolean[] offsetInputs,
            boolean[] offsetInputNulls)
    {
        Label next = code.newLabel();
        for (int contribution = 0; contribution < spec.contributions().size(); contribution++) {
            if (!spec.readsInput(contribution)) {
                continue;
            }
            int input = inputOffset + contribution;
            Label noNullVector = code.newLabel();
            code.aload(INPUT_NULLS);
            code.loadConstant(input);
            code.aaload();
            code.ifnull(noNullVector);
            code.aload(INPUT_NULLS);
            code.loadConstant(input);
            code.aaload();
            if (inputNullUsesKeyIds[input]) {
                code.iload(KEY_POSITION);
            }
            else if (mappedInputNulls[input]) {
                code.aload(INPUT_NULL_IDS);
                code.loadConstant(input);
                code.aaload();
                code.iload(POSITION);
                code.iaload();
            }
            else {
                code.iload(POSITION);
            }
            if (offsetInputNulls[input]) {
                code.aload(INPUT_NULL_OFFSETS);
                code.loadConstant(input);
                code.iaload();
                code.iadd();
            }
            code.baload();
            code.ifne(next);
            code.labelBinding(noNullVector);
        }
        emitIncrement(code, spec, accumulator, inputOffset, intInputs, inputCarriers, mappedInputs, inputUsesKeyIds, offsetInputs);
        code.labelBinding(next);
    }

    private static void emitUpdateInvocation(CodeBuilder code, GroupedAggregationUpdate spec, int accumulator, boolean repeated)
    {
        List<ClassDesc> parameters = new ArrayList<>();
        parameters.add(CD_Object);
        parameters.add(CD_int);
        for (int contribution = 0; contribution < spec.contributions().size(); contribution++) {
            for (Class<?> parameter : spec.carrier(contribution).parameterTypes()) {
                parameters.add(parameter.describeConstable()
                        .map(ClassDesc.class::cast)
                        .orElseThrow());
            }
        }
        if (repeated) {
            parameters.add(CD_int);
        }
        MethodTypeDesc type = MethodTypeDesc.of(CD_void, parameters.toArray(ClassDesc[]::new));
        code.invokedynamic(DynamicCallSiteDesc.of(
                BSM_UPDATE,
                (repeated ? "repeated" : "update") + accumulator,
                type));
    }
}
