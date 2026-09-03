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

/** Generates exact provider updates over a proven shared dictionary domain. */
final class DictionaryDomainGroupingKernelGenerator
        implements AutoCloseable
{
    private static final ClassDesc CD_KERNEL = ClassDesc.of("org.weakref.nitro.operator.DictionaryDomainGroupingKernel");
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

    // Parameters of DictionaryDomainGroupingKernel.accumulate.
    private static final int DOMAIN_SIZE = 1;
    private static final int FREQUENCIES = 2;
    private static final int GROUPS = 3;
    private static final int INPUTS = 4;
    private static final int INPUT_VALUE_OFFSETS = 5;
    private static final int INPUT_OFFSETS = 6;
    private static final int INPUT_NULLS = 7;
    private static final int INPUT_NULL_OFFSETS = 8;
    private static final int STATES = 9;
    // Locals.
    private static final int DOMAIN = 10;
    private static final int FREQUENCY = 11;
    private static final int GROUP = 12;
    private static final int VALUE_POSITION = 13;
    private static final int INPUT_ARRAY_BASE = 14;

    private final ConcurrentHashMap<KernelKey, DictionaryDomainGroupingKernel> kernels = new ConcurrentHashMap<>();
    private final AtomicInteger nextClassId = new AtomicInteger();
    private boolean closed;

    DictionaryDomainGroupingKernel create(
            List<GroupedAggregationUpdate> updates,
            boolean[] intInputs,
            ContributionCarrier[] inputCarriers,
            boolean[] allNullInputs,
            boolean[] offsetInputs,
            boolean[] offsetInputNulls)
    {
        checkOpen();
        for (GroupedAggregationUpdate update : updates) {
            if (update.target().repeatedUpdate().isEmpty()) {
                throw new IllegalArgumentException("dictionary-domain update is missing repeated semantics");
            }
        }
        String physicalShape = Arrays.toString(intInputs) + Arrays.toString(inputCarriers) + Arrays.toString(allNullInputs) + Arrays.toString(offsetInputs) + Arrays.toString(offsetInputNulls);
        return kernels.computeIfAbsent(
                new KernelKey(List.copyOf(updates), physicalShape),
                _ -> generate(updates, intInputs, inputCarriers, allNullInputs, offsetInputs, offsetInputNulls));
    }

    private DictionaryDomainGroupingKernel generate(
            List<GroupedAggregationUpdate> updates,
            boolean[] intInputs,
            ContributionCarrier[] inputCarriers,
            boolean[] allNullInputs,
            boolean[] offsetInputs,
            boolean[] offsetInputNulls)
    {
        ClassDesc thisClass = ClassDesc.of("org.weakref.nitro.operator.GeneratedDictionaryDomainGroupingKernel" + nextClassId.incrementAndGet());
        MethodTypeDesc accumulateType = MethodTypeDesc.of(
                CD_void,
                CD_int, CD_INT_ARRAY, CD_INT_ARRAY, CD_OBJECT_ARRAY, CD_INT_ARRAY_2D, CD_INT_ARRAY,
                CD_BOOLEAN_ARRAY_2D, CD_INT_ARRAY, CD_OBJECT_ARRAY);

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
                    code -> emitAccumulate(code, updates, intInputs, inputCarriers, allNullInputs, offsetInputs, offsetInputNulls));
        });

        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup().defineHiddenClassWithClassData(
                    bytes,
                    updates.stream().map(GroupedAggregationUpdate::target).toList(),
                    true,
                    MethodHandles.Lookup.ClassOption.NESTMATE);
            return (DictionaryDomainGroupingKernel) lookup.findConstructor(lookup.lookupClass(), java.lang.invoke.MethodType.methodType(void.class)).invoke();
        }
        catch (Throwable failure) {
            throw new RuntimeException("Failed to generate dictionary-domain grouping kernel", failure);
        }
    }

    private static void emitAccumulate(
            CodeBuilder code,
            List<GroupedAggregationUpdate> updates,
            boolean[] intInputs,
            ContributionCarrier[] inputCarriers,
            boolean[] allNullInputs,
            boolean[] offsetInputs,
            boolean[] offsetInputNulls)
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

        code.loadConstant(0);
        code.istore(DOMAIN);
        Label top = code.newLabel();
        Label next = code.newLabel();
        Label exit = code.newLabel();
        code.labelBinding(top);
        code.iload(DOMAIN);
        code.iload(DOMAIN_SIZE);
        code.if_icmpge(exit);

        code.aload(FREQUENCIES);
        code.iload(DOMAIN);
        code.iaload();
        code.dup();
        code.istore(FREQUENCY);
        code.ifeq(next);
        code.aload(GROUPS);
        code.iload(DOMAIN);
        code.iaload();
        code.istore(GROUP);

        input = 0;
        for (int accumulator = 0; accumulator < updates.size(); accumulator++) {
            GroupedAggregationUpdate update = updates.get(accumulator);
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
                if (!update.readsInput(contribution)) {
                    continue;
                }
                Label notNull = code.newLabel();
                code.aload(INPUT_NULLS);
                code.loadConstant(input);
                code.aaload();
                code.ifnull(notNull);
                code.aload(INPUT_NULLS);
                code.loadConstant(input);
                code.aaload();
                code.iload(DOMAIN);
                if (offsetInputNulls[input]) {
                    code.aload(INPUT_NULL_OFFSETS);
                    code.loadConstant(input);
                    code.iaload();
                    code.iadd();
                }
                code.baload();
                code.ifne(skipUpdate);
                code.labelBinding(notNull);
            }

            code.aload(STATES);
            code.loadConstant(accumulator);
            code.aaload();
            code.iload(GROUP);
            int contributionInput = input - update.contributions().size();
            for (int contribution = 0; contribution < update.contributions().size(); contribution++, contributionInput++) {
                if (!update.readsValue(contribution)) {
                    code.loadConstant(update.constantValue(contribution));
                    continue;
                }
                emitValue(code, contributionInput, inputCarriers[contributionInput], intInputs[contributionInput], offsetInputs[contributionInput]);
            }
            code.iload(FREQUENCY);
            emitInvocation(code, update, accumulator);
            code.labelBinding(skipUpdate);
        }

        code.labelBinding(next);
        code.iinc(DOMAIN, 1);
        code.goto_(top);
        code.labelBinding(exit);
        code.return_();
    }

    private static void emitValue(CodeBuilder code, int input, ContributionCarrier carrier, boolean intInput, boolean offsetInput)
    {
        code.aload(INPUT_ARRAY_BASE + input);
        code.iload(DOMAIN);
        if (offsetInput) {
            code.aload(INPUT_OFFSETS);
            code.loadConstant(input);
            code.iaload();
            code.iadd();
        }
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

    private static void emitInvocation(CodeBuilder code, GroupedAggregationUpdate update, int accumulator)
    {
        List<ClassDesc> parameters = new ArrayList<>();
        parameters.add(CD_Object);
        parameters.add(CD_int);
        for (int contribution = 0; contribution < update.contributions().size(); contribution++) {
            for (Class<?> parameter : update.carrier(contribution).parameterTypes()) {
                parameters.add(parameter.describeConstable().map(ClassDesc.class::cast).orElseThrow());
            }
        }
        parameters.add(CD_int);
        code.invokedynamic(DynamicCallSiteDesc.of(
                BSM_UPDATE,
                "repeated" + accumulator,
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
            throw new IllegalStateException("Dictionary-domain grouping kernel generator is closed");
        }
    }

    private record KernelKey(List<GroupedAggregationUpdate> updates, String physicalShape) {}
}
