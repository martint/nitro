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

import java.lang.classfile.ClassFile;
import java.lang.classfile.CodeBuilder;
import java.lang.classfile.Label;
import java.lang.constant.ClassDesc;
import java.lang.constant.MethodTypeDesc;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.constant.ConstantDescs.CD_boolean;
import static java.lang.constant.ConstantDescs.CD_int;
import static java.lang.constant.ConstantDescs.CD_long;
import static java.lang.constant.ConstantDescs.CD_void;

/** Emits an unrolled normalized-key grouping loop for a physical field/null shape. */
final class NormalizedIntGroupingKernelGenerator
        implements AutoCloseable
{
    private static final int NULL_FREE = 0;
    private static final int ALL_NULL = 1;
    private static final int MIXED = 2;

    private static final ClassDesc CD_KERNEL = ClassDesc.of("org.weakref.nitro.operator.NormalizedIntGroupingKernel");
    private static final ClassDesc CD_TABLE = ClassDesc.of("org.weakref.nitro.operator.FlatGroupingTable");
    private static final ClassDesc CD_VECTOR = ClassDesc.of("org.weakref.nitro.data.Vector");
    private static final ClassDesc CD_LONG_VALUES = ClassDesc.of("org.weakref.nitro.data.VectorAccess$LongValues");
    private static final ClassDesc CD_BOOLEAN_VALUES = ClassDesc.of("org.weakref.nitro.data.VectorAccess$BooleanValues");
    private static final ClassDesc CD_INT_ARRAY = CD_int.arrayType();
    private static final ClassDesc CD_INT_ARRAY_ARRAY = CD_INT_ARRAY.arrayType();
    private static final ClassDesc CD_LONG_ARRAY = CD_long.arrayType();
    private static final ClassDesc CD_LONG_VALUES_ARRAY = CD_LONG_VALUES.arrayType();
    private static final ClassDesc CD_BOOLEAN_VALUES_ARRAY = CD_BOOLEAN_VALUES.arrayType();
    private static final ClassDesc CD_VECTOR_ARRAY = CD_VECTOR.arrayType();
    private static final MethodTypeDesc ASSIGN_TYPE = MethodTypeDesc.of(
            CD_long,
            CD_INT_ARRAY, CD_int, CD_LONG_VALUES_ARRAY, CD_INT_ARRAY_ARRAY, CD_BOOLEAN_VALUES_ARRAY,
            CD_TABLE, CD_VECTOR_ARRAY, CD_VECTOR_ARRAY, CD_long, CD_LONG_ARRAY);
    private static final MethodTypeDesc LONG_VALUE_TYPE = MethodTypeDesc.of(CD_long, CD_int);
    private static final MethodTypeDesc BOOLEAN_VALUE_TYPE = MethodTypeDesc.of(CD_boolean, CD_int);
    private static final MethodTypeDesc ASSIGN_NORMALIZED_TYPE = MethodTypeDesc.of(
            CD_long, CD_VECTOR_ARRAY, CD_VECTOR_ARRAY, CD_int, CD_long, CD_long, CD_long);
    private static final MethodTypeDesc ASSIGN_FALLBACK_TYPE = MethodTypeDesc.of(
            CD_long, CD_VECTOR_ARRAY, CD_VECTOR_ARRAY, CD_int, CD_long);

    private static final int POSITIONS = 1;
    private static final int COUNT = 2;
    private static final int LONG_VALUES = 3;
    private static final int BINARY_IDS = 4;
    private static final int NULLS = 5;
    private static final int TABLE = 6;
    private static final int VALUES = 7;
    private static final int NULL_VECTORS = 8;
    private static final int NEXT_ID = 9;
    private static final int OUTPUT = 11;
    private static final int INDEX = 12;
    private static final int POSITION = 13;
    private static final int FIRST = 14;
    private static final int SECOND = 16;
    private static final int ENCODED = 18;
    private static final int GROUP = 19;
    private static final int RAW_LONG = 21;

    private final ConcurrentHashMap<Long, NormalizedIntGroupingKernel> kernels = new ConcurrentHashMap<>();
    private boolean closed;

    NormalizedIntGroupingKernel create(long shape)
    {
        if (closed) {
            throw new IllegalStateException("Normalized grouping kernel generator is closed");
        }
        return kernels.computeIfAbsent(shape, NormalizedIntGroupingKernelGenerator::generate);
    }

    static long shape(FlatTypeHandler.Kind[] kinds, boolean[] nullFree, boolean[] allNull)
    {
        long shape = kinds.length;
        for (int field = 0; field < kinds.length; field++) {
            if (kinds[field] == FlatTypeHandler.Kind.BINARY) {
                shape |= 1L << (4 + field);
            }
            int nullShape = allNull[field] ? ALL_NULL : nullFree[field] ? NULL_FREE : MIXED;
            shape |= (long) nullShape << (8 + field * 2);
        }
        return shape;
    }

    @Override
    public void close()
    {
        closed = true;
        kernels.clear();
    }

    private static NormalizedIntGroupingKernel generate(long shape)
    {
        int fieldCount = (int) (shape & 0xF);
        ClassDesc thisClass = ClassDesc.of("org.weakref.nitro.operator.GeneratedNormalizedIntGroupingKernel" + Long.toUnsignedString(shape));
        byte[] bytes = ClassFile.of().build(thisClass, builder -> {
            builder.withSuperclass(ClassDesc.of("java.lang.Object"));
            builder.withInterfaceSymbols(CD_KERNEL);
            builder.withFlags(ClassFile.ACC_FINAL | ClassFile.ACC_SYNTHETIC);
            builder.withMethodBody("<init>", MethodTypeDesc.of(CD_void), ClassFile.ACC_PUBLIC, code -> {
                code.aload(0);
                code.invokespecial(ClassDesc.of("java.lang.Object"), "<init>", MethodTypeDesc.of(CD_void));
                code.return_();
            });
            builder.withMethodBody("assign", ASSIGN_TYPE, ClassFile.ACC_PUBLIC, code -> emitAssign(code, shape, fieldCount));
        });
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup().defineHiddenClass(bytes, true, MethodHandles.Lookup.ClassOption.NESTMATE);
            return (NormalizedIntGroupingKernel) lookup.findConstructor(lookup.lookupClass(), java.lang.invoke.MethodType.methodType(void.class)).invoke();
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to generate normalized grouping kernel for shape " + shape, e);
        }
    }

    private static void emitAssign(CodeBuilder code, long shape, int fieldCount)
    {
        code.loadConstant(0);
        code.istore(INDEX);
        Label top = code.newLabel();
        Label exit = code.newLabel();
        code.labelBinding(top);
        code.iload(INDEX);
        code.iload(COUNT);
        code.if_icmpge(exit);
        Label dense = code.newLabel();
        Label positionReady = code.newLabel();
        code.aload(POSITIONS);
        code.ifnull(dense);
        code.aload(POSITIONS);
        code.iload(INDEX);
        code.iaload();
        code.goto_(positionReady);
        code.labelBinding(dense);
        code.iload(INDEX);
        code.labelBinding(positionReady);
        code.istore(POSITION);

        code.loadConstant(0L);
        code.lstore(FIRST);
        code.loadConstant(0L);
        code.lstore(SECOND);
        Label fallback = code.newLabel();
        for (int field = 0; field < fieldCount; field++) {
            int nullShape = (int) ((shape >>> (8 + field * 2)) & 3);
            boolean binary = (shape & (1L << (4 + field))) != 0;
            emitEncodedValue(code, field, nullShape, binary, fallback);
            int target = field < 2 ? FIRST : SECOND;
            int shift = (field & 1) * Integer.SIZE;
            code.lload(target);
            code.iload(ENCODED);
            code.i2l();
            if (shift != 0) {
                code.loadConstant(shift);
                code.lshl();
            }
            code.lor();
            code.lstore(target);
        }

        code.aload(TABLE);
        code.aload(VALUES);
        code.aload(NULL_VECTORS);
        code.iload(POSITION);
        code.lload(NEXT_ID);
        code.lload(FIRST);
        code.lload(SECOND);
        code.invokevirtual(CD_TABLE, "assignNormalizedGroup", ASSIGN_NORMALIZED_TYPE);
        code.lstore(GROUP);
        Label assigned = code.newLabel();
        code.goto_(assigned);

        code.labelBinding(fallback);
        code.aload(TABLE);
        code.aload(VALUES);
        code.aload(NULL_VECTORS);
        code.iload(POSITION);
        code.lload(NEXT_ID);
        code.invokevirtual(CD_TABLE, "assignGroupWithoutNormalization", ASSIGN_FALLBACK_TYPE);
        code.lstore(GROUP);

        code.labelBinding(assigned);
        Label existing = code.newLabel();
        code.lload(GROUP);
        code.lload(NEXT_ID);
        code.lcmp();
        code.ifne(existing);
        code.lload(NEXT_ID);
        code.loadConstant(1L);
        code.ladd();
        code.lstore(NEXT_ID);
        code.labelBinding(existing);
        code.aload(OUTPUT);
        code.iload(POSITION);
        code.lload(GROUP);
        code.lastore();
        code.iinc(INDEX, 1);
        code.goto_(top);
        code.labelBinding(exit);
        code.lload(NEXT_ID);
        code.lreturn();
    }

    private static void emitEncodedValue(CodeBuilder code, int field, int nullShape, boolean binary, Label fallback)
    {
        if (nullShape == ALL_NULL) {
            code.loadConstant(0);
            code.istore(ENCODED);
            return;
        }
        if (nullShape == MIXED) {
            Label nonNull = code.newLabel();
            Label done = code.newLabel();
            code.aload(NULLS);
            code.loadConstant(field);
            code.aaload();
            code.iload(POSITION);
            code.invokeinterface(CD_BOOLEAN_VALUES, "value", BOOLEAN_VALUE_TYPE);
            code.ifeq(nonNull);
            code.loadConstant(0);
            code.istore(ENCODED);
            code.goto_(done);
            code.labelBinding(nonNull);
            emitNonNullEncodedValue(code, field, binary, fallback);
            code.labelBinding(done);
            return;
        }
        if (nullShape != NULL_FREE) {
            code.goto_(fallback);
            return;
        }
        emitNonNullEncodedValue(code, field, binary, fallback);
    }

    private static void emitNonNullEncodedValue(CodeBuilder code, int field, boolean binary, Label fallback)
    {
        if (binary) {
            code.aload(BINARY_IDS);
            code.loadConstant(field);
            code.aaload();
            code.iload(POSITION);
            code.iaload();
            code.istore(ENCODED);
            code.iload(ENCODED);
            code.iflt(fallback);
            code.iload(ENCODED);
            code.loadConstant(1);
            code.iadd();
            code.istore(ENCODED);
            return;
        }
        code.aload(LONG_VALUES);
        code.loadConstant(field);
        code.aaload();
        code.iload(POSITION);
        code.invokeinterface(CD_LONG_VALUES, "value", LONG_VALUE_TYPE);
        code.lstore(RAW_LONG);
        code.lload(RAW_LONG);
        code.loadConstant(0L);
        code.lcmp();
        code.iflt(fallback);
        code.lload(RAW_LONG);
        code.loadConstant((long) Integer.MAX_VALUE);
        code.lcmp();
        code.ifge(fallback);
        code.lload(RAW_LONG);
        code.l2i();
        code.loadConstant(1);
        code.iadd();
        code.istore(ENCODED);
    }
}
