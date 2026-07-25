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
package org.weakref.nitro.jit;

import java.lang.classfile.ClassFile;
import java.lang.classfile.CodeBuilder;
import java.lang.classfile.Label;
import java.lang.constant.ClassDesc;
import java.lang.constant.MethodTypeDesc;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import static java.lang.constant.ConstantDescs.CD_boolean;
import static java.lang.constant.ConstantDescs.CD_int;
import static java.lang.constant.ConstantDescs.CD_void;

final class Utf8DynamicMaskKernelGenerator
{
    private static final ClassDesc CD_OBJECT = ClassDesc.of("java.lang.Object");
    private static final ClassDesc CD_ARRAYS = ClassDesc.of("java.util.Arrays");
    private static final ClassDesc CD_KERNEL = ClassDesc.of("org.weakref.nitro.jit.Utf8DynamicMaskKernel");
    private static final ClassDesc CD_MASK = ClassDesc.of("org.weakref.nitro.data.Mask");
    private static final ClassDesc CD_DICTIONARY = ClassDesc.of("org.weakref.nitro.data.DictionaryVector");
    private static final ClassDesc CD_BOOLEAN_VALUES = ClassDesc.of("org.weakref.nitro.data.VectorAccess$BooleanValues");
    private static final ClassDesc CD_BYTE_ARRAY = ClassDesc.ofDescriptor("[B");
    private static final ClassDesc CD_BOOLEAN_ARRAY = ClassDesc.ofDescriptor("[Z");
    private static final ClassDesc CD_INT_ARRAY = ClassDesc.ofDescriptor("[I");
    private static final MethodTypeDesc RETAIN_TYPE = MethodTypeDesc.of(
            CD_void,
            CD_BYTE_ARRAY,
            CD_INT_ARRAY,
            CD_INT_ARRAY,
            CD_BOOLEAN_ARRAY,
            CD_BYTE_ARRAY,
            CD_INT_ARRAY,
            CD_INT_ARRAY,
            CD_BOOLEAN_ARRAY,
            CD_MASK,
            CD_boolean);
    private static final MethodTypeDesc RETAIN_NESTED_TYPE = MethodTypeDesc.of(
            CD_void,
            CD_BYTE_ARRAY,
            CD_INT_ARRAY,
            CD_DICTIONARY,
            CD_int,
            CD_BOOLEAN_VALUES,
            CD_BYTE_ARRAY,
            CD_INT_ARRAY,
            CD_DICTIONARY,
            CD_int,
            CD_BOOLEAN_VALUES,
            CD_MASK,
            CD_boolean);
    private static final MethodTypeDesc MISMATCH_TYPE = MethodTypeDesc.of(
            CD_int,
            CD_BYTE_ARRAY,
            CD_int,
            CD_int,
            CD_BYTE_ARRAY,
            CD_int,
            CD_int);
    private static final MethodTypeDesc MASK_BOOLEAN = MethodTypeDesc.of(CD_boolean);
    private static final MethodTypeDesc MASK_INT = MethodTypeDesc.of(CD_int);
    private static final MethodTypeDesc MASK_POSITIONS = MethodTypeDesc.of(CD_INT_ARRAY, CD_int);
    private static final MethodTypeDesc MASK_SELECTED_POSITIONS = MethodTypeDesc.of(CD_INT_ARRAY);
    private static final MethodTypeDesc MASK_FINISH = MethodTypeDesc.of(CD_void, CD_int);
    private static final MethodTypeDesc DICTIONARY_BASE_POSITION = MethodTypeDesc.of(CD_int, CD_int, CD_int);
    private static final MethodTypeDesc BOOLEAN_VALUE = MethodTypeDesc.of(CD_boolean, CD_int);

    private static final int LEFT_DATA = 1;
    private static final int LEFT_OFFSETS = 2;
    private static final int LEFT_IDS = 3;
    private static final int LEFT_NULLS = 4;
    private static final int RIGHT_DATA = 5;
    private static final int RIGHT_OFFSETS = 6;
    private static final int RIGHT_IDS = 7;
    private static final int RIGHT_NULLS = 8;
    private static final int MASK = 9;
    private static final int SELECT_MATCHES = 10;
    private static final int DENSE = 11;
    private static final int ITERATIONS = 12;
    private static final int POSITIONS = 13;
    private static final int RETAINED = 14;
    private static final int INDEX = 15;
    private static final int POSITION = 16;
    private static final int LEFT_POSITION = 17;
    private static final int RIGHT_POSITION = 18;
    private static final int LEFT_START = 19;
    private static final int LEFT_END = 20;
    private static final int RIGHT_START = 21;
    private static final int RIGHT_END = 22;

    private Utf8DynamicMaskKernelGenerator() {}

    static Utf8DynamicMaskKernel generate()
    {
        ClassDesc generatedClass = ClassDesc.of("org.weakref.nitro.jit.GeneratedUtf8DynamicMaskKernel");
        byte[] classBytes = ClassFile.of().build(generatedClass, builder -> {
            builder.withSuperclass(CD_OBJECT);
            builder.withInterfaceSymbols(CD_KERNEL);
            builder.withFlags(ClassFile.ACC_FINAL | ClassFile.ACC_SYNTHETIC);
            builder.withMethodBody("<init>", MethodTypeDesc.of(CD_void), ClassFile.ACC_PUBLIC, code -> {
                code.aload(0);
                code.invokespecial(CD_OBJECT, "<init>", MethodTypeDesc.of(CD_void));
                code.return_();
            });
            builder.withMethodBody("retainFlatFlat", RETAIN_TYPE, ClassFile.ACC_PUBLIC, code -> emitRetain(code, false, false));
            builder.withMethodBody("retainDictionaryDictionary", RETAIN_TYPE, ClassFile.ACC_PUBLIC, code -> emitRetain(code, true, true));
            builder.withMethodBody("retainDictionaryFlat", RETAIN_TYPE, ClassFile.ACC_PUBLIC, code -> emitRetain(code, true, false));
            builder.withMethodBody("retainFlatDictionary", RETAIN_TYPE, ClassFile.ACC_PUBLIC, code -> emitRetain(code, false, true));
            builder.withMethodBody("retainNestedDictionaryDictionary", RETAIN_NESTED_TYPE, ClassFile.ACC_PUBLIC, Utf8DynamicMaskKernelGenerator::emitRetainNested);
        });

        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup()
                    .defineHiddenClass(classBytes, true, MethodHandles.Lookup.ClassOption.NESTMATE);
            return (Utf8DynamicMaskKernel) lookup.findConstructor(
                    lookup.lookupClass(),
                    MethodType.methodType(void.class)).invoke();
        }
        catch (Throwable throwable) {
            throw new IllegalStateException("Failed to generate UTF-8 dynamic mask kernel", throwable);
        }
    }

    private static void emitRetain(CodeBuilder code, boolean leftMapped, boolean rightMapped)
    {
        Label nonEmpty = code.newLabel();
        Label sparse = code.newLabel();
        Label positionsReady = code.newLabel();
        Label loop = code.newLabel();
        Label done = code.newLabel();
        Label sparsePosition = code.newLabel();
        Label positionReady = code.newLabel();
        Label lengthsDiffer = code.newLabel();
        Label comparisonReady = code.newLabel();
        Label skip = code.newLabel();

        code.aload(MASK);
        code.invokevirtual(CD_MASK, "none", MASK_BOOLEAN);
        code.ifeq(nonEmpty);
        code.return_();
        code.labelBinding(nonEmpty);

        code.aload(MASK);
        code.invokevirtual(CD_MASK, "all", MASK_BOOLEAN);
        code.istore(DENSE);
        code.aload(MASK);
        code.invokevirtual(CD_MASK, "selectedCount", MASK_INT);
        code.istore(ITERATIONS);
        code.iload(DENSE);
        code.ifeq(sparse);
        code.aload(MASK);
        code.iload(ITERATIONS);
        code.invokevirtual(CD_MASK, "positionsArrayForOverwrite", MASK_POSITIONS);
        code.goto_(positionsReady);
        code.labelBinding(sparse);
        code.aload(MASK);
        code.invokevirtual(CD_MASK, "selectedPositions", MASK_SELECTED_POSITIONS);
        code.labelBinding(positionsReady);
        code.astore(POSITIONS);
        code.loadConstant(0);
        code.istore(RETAINED);
        code.loadConstant(0);
        code.istore(INDEX);

        code.labelBinding(loop);
        code.iload(INDEX);
        code.iload(ITERATIONS);
        code.if_icmpge(done);
        code.iload(DENSE);
        code.ifeq(sparsePosition);
        code.iload(INDEX);
        code.goto_(positionReady);
        code.labelBinding(sparsePosition);
        code.aload(POSITIONS);
        code.iload(INDEX);
        code.iaload();
        code.labelBinding(positionReady);
        code.istore(POSITION);

        emitNullSkip(code, LEFT_NULLS, skip);
        emitNullSkip(code, RIGHT_NULLS, skip);
        emitMappedPosition(code, leftMapped, LEFT_IDS, LEFT_POSITION);
        emitMappedPosition(code, rightMapped, RIGHT_IDS, RIGHT_POSITION);
        emitOffset(code, LEFT_OFFSETS, LEFT_POSITION, 0, LEFT_START);
        emitOffset(code, LEFT_OFFSETS, LEFT_POSITION, 1, LEFT_END);
        emitOffset(code, RIGHT_OFFSETS, RIGHT_POSITION, 0, RIGHT_START);
        emitOffset(code, RIGHT_OFFSETS, RIGHT_POSITION, 1, RIGHT_END);

        code.iload(LEFT_END);
        code.iload(LEFT_START);
        code.isub();
        code.iload(RIGHT_END);
        code.iload(RIGHT_START);
        code.isub();
        code.if_icmpne(lengthsDiffer);
        code.aload(LEFT_DATA);
        code.iload(LEFT_START);
        code.iload(LEFT_END);
        code.aload(RIGHT_DATA);
        code.iload(RIGHT_START);
        code.iload(RIGHT_END);
        code.invokestatic(CD_ARRAYS, "mismatch", MISMATCH_TYPE);
        code.loadConstant(-1);
        Label mismatch = code.newLabel();
        code.if_icmpne(mismatch);
        code.loadConstant(1);
        code.goto_(comparisonReady);
        code.labelBinding(mismatch);
        code.loadConstant(0);
        code.goto_(comparisonReady);
        code.labelBinding(lengthsDiffer);
        code.loadConstant(0);
        code.labelBinding(comparisonReady);
        code.iload(SELECT_MATCHES);
        code.if_icmpne(skip);
        code.aload(POSITIONS);
        code.iload(RETAINED);
        code.iload(POSITION);
        code.iastore();
        code.iinc(RETAINED, 1);
        code.labelBinding(skip);
        code.iinc(INDEX, 1);
        code.goto_(loop);

        code.labelBinding(done);
        code.aload(MASK);
        code.iload(RETAINED);
        code.invokevirtual(CD_MASK, "finishRetain", MASK_FINISH);
        code.return_();
    }

    private static void emitNullSkip(CodeBuilder code, int nullsSlot, Label skip)
    {
        Label notNull = code.newLabel();
        code.aload(nullsSlot);
        code.ifnull(notNull);
        code.aload(nullsSlot);
        code.iload(POSITION);
        code.baload();
        code.ifne(skip);
        code.labelBinding(notNull);
    }

    private static void emitRetainNested(CodeBuilder code)
    {
        final int leftData = 1;
        final int leftOffsets = 2;
        final int leftDictionary = 3;
        final int leftDepth = 4;
        final int leftNulls = 5;
        final int rightData = 6;
        final int rightOffsets = 7;
        final int rightDictionary = 8;
        final int rightDepth = 9;
        final int rightNulls = 10;
        final int mask = 11;
        final int selectMatches = 12;
        final int dense = 13;
        final int iterations = 14;
        final int positions = 15;
        final int retained = 16;
        final int index = 17;
        final int position = 18;
        final int leftPosition = 19;
        final int rightPosition = 20;
        final int leftStart = 21;
        final int leftEnd = 22;
        final int rightStart = 23;
        final int rightEnd = 24;

        Label nonEmpty = code.newLabel();
        Label sparse = code.newLabel();
        Label positionsReady = code.newLabel();
        Label loop = code.newLabel();
        Label done = code.newLabel();
        Label sparsePosition = code.newLabel();
        Label positionReady = code.newLabel();
        Label leftNotNull = code.newLabel();
        Label rightNotNull = code.newLabel();
        Label lengthsDiffer = code.newLabel();
        Label comparisonReady = code.newLabel();
        Label mismatch = code.newLabel();
        Label skip = code.newLabel();

        code.aload(mask);
        code.invokevirtual(CD_MASK, "none", MASK_BOOLEAN);
        code.ifeq(nonEmpty);
        code.return_();
        code.labelBinding(nonEmpty);
        code.aload(mask);
        code.invokevirtual(CD_MASK, "all", MASK_BOOLEAN);
        code.istore(dense);
        code.aload(mask);
        code.invokevirtual(CD_MASK, "selectedCount", MASK_INT);
        code.istore(iterations);
        code.iload(dense);
        code.ifeq(sparse);
        code.aload(mask);
        code.iload(iterations);
        code.invokevirtual(CD_MASK, "positionsArrayForOverwrite", MASK_POSITIONS);
        code.goto_(positionsReady);
        code.labelBinding(sparse);
        code.aload(mask);
        code.invokevirtual(CD_MASK, "selectedPositions", MASK_SELECTED_POSITIONS);
        code.labelBinding(positionsReady);
        code.astore(positions);
        code.loadConstant(0);
        code.istore(retained);
        code.loadConstant(0);
        code.istore(index);

        code.labelBinding(loop);
        code.iload(index);
        code.iload(iterations);
        code.if_icmpge(done);
        code.iload(dense);
        code.ifeq(sparsePosition);
        code.iload(index);
        code.goto_(positionReady);
        code.labelBinding(sparsePosition);
        code.aload(positions);
        code.iload(index);
        code.iaload();
        code.labelBinding(positionReady);
        code.istore(position);

        code.aload(leftNulls);
        code.ifnull(leftNotNull);
        code.aload(leftNulls);
        code.iload(position);
        code.invokeinterface(CD_BOOLEAN_VALUES, "value", BOOLEAN_VALUE);
        code.ifne(skip);
        code.labelBinding(leftNotNull);
        code.aload(rightNulls);
        code.ifnull(rightNotNull);
        code.aload(rightNulls);
        code.iload(position);
        code.invokeinterface(CD_BOOLEAN_VALUES, "value", BOOLEAN_VALUE);
        code.ifne(skip);
        code.labelBinding(rightNotNull);

        code.aload(leftDictionary);
        code.iload(position);
        code.iload(leftDepth);
        code.invokevirtual(CD_DICTIONARY, "basePosition", DICTIONARY_BASE_POSITION);
        code.istore(leftPosition);
        code.aload(rightDictionary);
        code.iload(position);
        code.iload(rightDepth);
        code.invokevirtual(CD_DICTIONARY, "basePosition", DICTIONARY_BASE_POSITION);
        code.istore(rightPosition);
        emitNestedOffset(code, leftOffsets, leftPosition, 0, leftStart);
        emitNestedOffset(code, leftOffsets, leftPosition, 1, leftEnd);
        emitNestedOffset(code, rightOffsets, rightPosition, 0, rightStart);
        emitNestedOffset(code, rightOffsets, rightPosition, 1, rightEnd);

        code.iload(leftEnd);
        code.iload(leftStart);
        code.isub();
        code.iload(rightEnd);
        code.iload(rightStart);
        code.isub();
        code.if_icmpne(lengthsDiffer);
        code.aload(leftData);
        code.iload(leftStart);
        code.iload(leftEnd);
        code.aload(rightData);
        code.iload(rightStart);
        code.iload(rightEnd);
        code.invokestatic(CD_ARRAYS, "mismatch", MISMATCH_TYPE);
        code.loadConstant(-1);
        code.if_icmpne(mismatch);
        code.loadConstant(1);
        code.goto_(comparisonReady);
        code.labelBinding(mismatch);
        code.loadConstant(0);
        code.goto_(comparisonReady);
        code.labelBinding(lengthsDiffer);
        code.loadConstant(0);
        code.labelBinding(comparisonReady);
        code.iload(selectMatches);
        code.if_icmpne(skip);
        code.aload(positions);
        code.iload(retained);
        code.iload(position);
        code.iastore();
        code.iinc(retained, 1);
        code.labelBinding(skip);
        code.iinc(index, 1);
        code.goto_(loop);

        code.labelBinding(done);
        code.aload(mask);
        code.iload(retained);
        code.invokevirtual(CD_MASK, "finishRetain", MASK_FINISH);
        code.return_();
    }

    private static void emitNestedOffset(CodeBuilder code, int offsetsSlot, int positionSlot, int delta, int outputSlot)
    {
        code.aload(offsetsSlot);
        code.iload(positionSlot);
        if (delta != 0) {
            code.loadConstant(delta);
            code.iadd();
        }
        code.iaload();
        code.istore(outputSlot);
    }

    private static void emitMappedPosition(CodeBuilder code, boolean mapped, int idsSlot, int outputSlot)
    {
        if (mapped) {
            code.aload(idsSlot);
            code.iload(POSITION);
            code.iaload();
        }
        else {
            code.iload(POSITION);
        }
        code.istore(outputSlot);
    }

    private static void emitOffset(CodeBuilder code, int offsetsSlot, int positionSlot, int delta, int outputSlot)
    {
        code.aload(offsetsSlot);
        code.iload(positionSlot);
        if (delta != 0) {
            code.loadConstant(delta);
            code.iadd();
        }
        code.iaload();
        code.istore(outputSlot);
    }
}
