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

/** Generates exact provider updates over group ids produced by any grouping layout. */
final class StagedAggregationKernelGenerator
        implements AutoCloseable
{
    private static final ClassDesc CD_KERNEL = ClassDesc.of("org.weakref.nitro.operator.StagedAggregationKernel");
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
            MethodTypeDesc.of(
                    CD_CallSite,
                    ClassDesc.of("java.lang.invoke.MethodHandles$Lookup"),
                    ClassDesc.of("java.lang.String"),
                    ClassDesc.of("java.lang.invoke.MethodType")));

    // Parameter slots of StagedAggregationKernel.accumulate.
    private static final int POSITIONS = 1;
    private static final int COUNT = 2;
    private static final int GROUPS = 3;
    private static final int INPUTS = 4;
    private static final int INPUT_VALUE_OFFSETS = 5;
    private static final int INPUT_MAPPINGS = 6;
    private static final int INPUT_MAPPING_OFFSETS = 7;
    private static final int INPUT_BASE_OFFSETS = 8;
    private static final int INPUT_NULLS = 9;
    private static final int INPUT_NULL_MAPPINGS = 10;
    private static final int INPUT_NULL_MAPPING_OFFSETS = 11;
    private static final int INPUT_NULL_BASE_OFFSETS = 12;
    private static final int STATES = 13;
    // Locals.
    private static final int INDEX = 14;
    private static final int POSITION = 15;
    private static final int GROUP = 16;
    private static final int VALUE_POSITION = 17;
    private static final int INPUT_ARRAY_BASE = 18;

    private final ConcurrentHashMap<KernelKey, StagedAggregationKernel> kernels = new ConcurrentHashMap<>();
    private final AtomicInteger nextClassId = new AtomicInteger();
    private boolean closed;

    StagedAggregationKernel create(
            List<GroupedAggregationUpdate> updates,
            boolean[] intInputs,
            ContributionCarrier[] inputCarriers,
            boolean[] allNullInputs,
            boolean[] mappedInputs,
            boolean[] inputMappingOffsets,
            boolean[] inputBaseOffsets,
            boolean[] nullableInputs,
            boolean[] mappedInputNulls,
            boolean[] inputNullMappingOffsets,
            boolean[] inputNullBaseOffsets)
    {
        checkOpen();
        String physicalShape = Arrays.toString(intInputs) +
                Arrays.toString(inputCarriers) +
                Arrays.toString(allNullInputs) +
                Arrays.toString(mappedInputs) +
                Arrays.toString(inputMappingOffsets) +
                Arrays.toString(inputBaseOffsets) +
                Arrays.toString(nullableInputs) +
                Arrays.toString(mappedInputNulls) +
                Arrays.toString(inputNullMappingOffsets) +
                Arrays.toString(inputNullBaseOffsets);
        return kernels.computeIfAbsent(
                new KernelKey(List.copyOf(updates), physicalShape),
                _ -> generate(
                        updates,
                        intInputs,
                        inputCarriers,
                        allNullInputs,
                        mappedInputs,
                        inputMappingOffsets,
                        inputBaseOffsets,
                        nullableInputs,
                        mappedInputNulls,
                        inputNullMappingOffsets,
                        inputNullBaseOffsets));
    }

    private StagedAggregationKernel generate(
            List<GroupedAggregationUpdate> updates,
            boolean[] intInputs,
            ContributionCarrier[] inputCarriers,
            boolean[] allNullInputs,
            boolean[] mappedInputs,
            boolean[] inputMappingOffsets,
            boolean[] inputBaseOffsets,
            boolean[] nullableInputs,
            boolean[] mappedInputNulls,
            boolean[] inputNullMappingOffsets,
            boolean[] inputNullBaseOffsets)
    {
        ClassDesc thisClass = ClassDesc.of(
                "org.weakref.nitro.operator.GeneratedStagedAggregationKernel" + nextClassId.incrementAndGet());
        MethodTypeDesc accumulateType = MethodTypeDesc.of(
                CD_void,
                CD_INT_ARRAY,
                CD_int,
                CD_LONG_ARRAY,
                CD_OBJECT_ARRAY,
                CD_INT_ARRAY_2D,
                CD_INT_ARRAY_2D,
                CD_INT_ARRAY,
                CD_INT_ARRAY,
                CD_BOOLEAN_ARRAY_2D,
                CD_INT_ARRAY_2D,
                CD_INT_ARRAY,
                CD_INT_ARRAY,
                CD_OBJECT_ARRAY);

        byte[] bytes = ClassFile.of().build(thisClass, builder -> {
            builder.withSuperclass(CD_Object);
            builder.withInterfaceSymbols(CD_KERNEL);
            builder.withFlags(ClassFile.ACC_FINAL | ClassFile.ACC_SYNTHETIC);
            builder.withMethodBody("<init>", MethodTypeDesc.of(CD_void), ClassFile.ACC_PUBLIC, code -> {
                code.aload(0);
                code.invokespecial(CD_Object, "<init>", MethodTypeDesc.of(CD_void));
                code.return_();
            });
            builder.withMethodBody(
                    "accumulate",
                    accumulateType,
                    ClassFile.ACC_PUBLIC,
                    code -> emitAccumulate(
                            code,
                            updates,
                            intInputs,
                            inputCarriers,
                            allNullInputs,
                            mappedInputs,
                            inputMappingOffsets,
                            inputBaseOffsets,
                            nullableInputs,
                            mappedInputNulls,
                            inputNullMappingOffsets,
                            inputNullBaseOffsets));
        });

        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup().defineHiddenClassWithClassData(
                    bytes,
                    updates.stream().map(GroupedAggregationUpdate::target).toList(),
                    true,
                    MethodHandles.Lookup.ClassOption.NESTMATE);
            return (StagedAggregationKernel) lookup
                    .findConstructor(lookup.lookupClass(), java.lang.invoke.MethodType.methodType(void.class))
                    .invoke();
        }
        catch (Throwable failure) {
            throw new RuntimeException("Failed to generate staged aggregation kernel", failure);
        }
    }

    private static void emitAccumulate(
            CodeBuilder code,
            List<GroupedAggregationUpdate> updates,
            boolean[] intInputs,
            ContributionCarrier[] inputCarriers,
            boolean[] allNullInputs,
            boolean[] mappedInputs,
            boolean[] inputMappingOffsets,
            boolean[] inputBaseOffsets,
            boolean[] nullableInputs,
            boolean[] mappedInputNulls,
            boolean[] inputNullMappingOffsets,
            boolean[] inputNullBaseOffsets)
    {
        int input = 0;
        for (GroupedAggregationUpdate update : updates) {
            for (int contribution = 0; contribution < update.contributions().size(); contribution++, input++) {
                if (!update.readsValue(contribution)) {
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

        Label sparse = code.newLabel();
        Label exit = code.newLabel();
        code.aload(POSITIONS);
        code.ifnonnull(sparse);
        emitLoop(
                code,
                updates,
                false,
                intInputs,
                inputCarriers,
                allNullInputs,
                mappedInputs,
                inputMappingOffsets,
                inputBaseOffsets,
                nullableInputs,
                mappedInputNulls,
                inputNullMappingOffsets,
                inputNullBaseOffsets);
        code.goto_(exit);
        code.labelBinding(sparse);
        emitLoop(
                code,
                updates,
                true,
                intInputs,
                inputCarriers,
                allNullInputs,
                mappedInputs,
                inputMappingOffsets,
                inputBaseOffsets,
                nullableInputs,
                mappedInputNulls,
                inputNullMappingOffsets,
                inputNullBaseOffsets);
        code.labelBinding(exit);
        code.return_();
    }

    private static void emitLoop(
            CodeBuilder code,
            List<GroupedAggregationUpdate> updates,
            boolean sparse,
            boolean[] intInputs,
            ContributionCarrier[] inputCarriers,
            boolean[] allNullInputs,
            boolean[] mappedInputs,
            boolean[] inputMappingOffsets,
            boolean[] inputBaseOffsets,
            boolean[] nullableInputs,
            boolean[] mappedInputNulls,
            boolean[] inputNullMappingOffsets,
            boolean[] inputNullBaseOffsets)
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
        }
        else {
            code.iload(INDEX);
        }
        code.istore(POSITION);
        code.aload(GROUPS);
        code.iload(POSITION);
        code.laload();
        code.l2i();
        code.istore(GROUP);

        int input = 0;
        for (int updateIndex = 0; updateIndex < updates.size(); updateIndex++) {
            GroupedAggregationUpdate update = updates.get(updateIndex);
            int updateInputOffset = input;
            boolean alwaysNull = false;
            for (int contribution = 0; contribution < update.contributions().size(); contribution++) {
                alwaysNull |= update.readsInput(contribution) && allNullInputs[updateInputOffset + contribution];
            }
            if (alwaysNull) {
                input += update.contributions().size();
                continue;
            }

            Label skipUpdate = code.newLabel();
            for (int contribution = 0; contribution < update.contributions().size(); contribution++, input++) {
                if (!update.readsInput(contribution) || !nullableInputs[input]) {
                    continue;
                }
                code.aload(INPUT_NULLS);
                code.loadConstant(input);
                code.aaload();
                emitPhysicalPosition(
                        code,
                        input,
                        INPUT_NULL_MAPPINGS,
                        INPUT_NULL_MAPPING_OFFSETS,
                        INPUT_NULL_BASE_OFFSETS,
                        mappedInputNulls[input],
                        inputNullMappingOffsets[input],
                        inputNullBaseOffsets[input]);
                code.baload();
                code.ifne(skipUpdate);
            }

            code.aload(STATES);
            code.loadConstant(updateIndex);
            code.aaload();
            code.iload(GROUP);
            int contributionInput = updateInputOffset;
            for (int contribution = 0; contribution < update.contributions().size(); contribution++, contributionInput++) {
                if (!update.readsValue(contribution)) {
                    code.loadConstant(update.constantValue(contribution));
                    continue;
                }
                emitValue(
                        code,
                        contributionInput,
                        inputCarriers[contributionInput],
                        intInputs[contributionInput],
                        mappedInputs[contributionInput],
                        inputMappingOffsets[contributionInput],
                        inputBaseOffsets[contributionInput]);
            }
            emitInvocation(code, update, updateIndex);
            code.labelBinding(skipUpdate);
        }

        code.iinc(INDEX, 1);
        code.goto_(top);
        code.labelBinding(exit);
    }

    private static void emitValue(
            CodeBuilder code,
            int input,
            ContributionCarrier carrier,
            boolean intInput,
            boolean mapped,
            boolean mappingOffset,
            boolean baseOffset)
    {
        code.aload(INPUT_ARRAY_BASE + input);
        emitPhysicalPosition(
                code,
                input,
                INPUT_MAPPINGS,
                INPUT_MAPPING_OFFSETS,
                INPUT_BASE_OFFSETS,
                mapped,
                mappingOffset,
                baseOffset);
        switch (carrier) {
            case DOUBLE -> code.daload();
            case BOOLEAN -> code.baload();
            case LONG -> {
                if (intInput) {
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

    private static void emitPhysicalPosition(
            CodeBuilder code,
            int input,
            int mappings,
            int mappingOffsets,
            int baseOffsets,
            boolean mapped,
            boolean mappingOffset,
            boolean baseOffset)
    {
        if (mapped) {
            code.aload(mappings);
            code.loadConstant(input);
            code.aaload();
        }
        code.iload(POSITION);
        if (mappingOffset) {
            code.aload(mappingOffsets);
            code.loadConstant(input);
            code.iaload();
            code.iadd();
        }
        if (mapped) {
            code.iaload();
        }
        if (baseOffset) {
            code.aload(baseOffsets);
            code.loadConstant(input);
            code.iaload();
            code.iadd();
        }
    }

    private static void emitInvocation(CodeBuilder code, GroupedAggregationUpdate update, int updateIndex)
    {
        List<ClassDesc> parameters = new ArrayList<>();
        parameters.add(CD_Object);
        parameters.add(CD_int);
        for (int contribution = 0; contribution < update.contributions().size(); contribution++) {
            for (Class<?> parameter : update.carrier(contribution).parameterTypes()) {
                parameters.add(parameter.describeConstable().map(ClassDesc.class::cast).orElseThrow());
            }
        }
        code.invokedynamic(DynamicCallSiteDesc.of(
                BSM_UPDATE,
                "update" + updateIndex,
                MethodTypeDesc.of(CD_void, parameters.toArray(ClassDesc[]::new))));
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
            throw new IllegalStateException("Staged aggregation kernel generator is closed");
        }
    }

    private record KernelKey(List<GroupedAggregationUpdate> updates, String physicalShape) {}
}
