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

/** Emits the concrete mixed-composite grouping loop for one three-field physical null shape. */
final class MixedComposite3GroupingKernelGenerator
        implements AutoCloseable
{
    static final int NULL_FREE = 0;
    static final int ALL_NULL = 1;
    static final int MIXED = 2;

    private static final ClassDesc CD_KERNEL = ClassDesc.of("org.weakref.nitro.operator.MixedComposite3GroupingKernel");
    private static final ClassDesc CD_TABLE = ClassDesc.of("org.weakref.nitro.operator.FlatGroupingTable");
    private static final ClassDesc CD_VECTOR = ClassDesc.of("org.weakref.nitro.data.Vector");
    private static final ClassDesc CD_LONG_VALUES = ClassDesc.of("org.weakref.nitro.function.scalar.builtin.VectorAccess$LongValues");
    private static final ClassDesc CD_BOOLEAN_VALUES = ClassDesc.of("org.weakref.nitro.function.scalar.builtin.VectorAccess$BooleanValues");
    private static final ClassDesc CD_INT_ARRAY = CD_int.arrayType();
    private static final ClassDesc CD_LONG_ARRAY = CD_long.arrayType();
    private static final ClassDesc CD_VECTOR_ARRAY = CD_VECTOR.arrayType();

    private static final MethodTypeDesc ASSIGN_TYPE = MethodTypeDesc.of(
            CD_long,
            CD_INT_ARRAY, CD_int, CD_LONG_VALUES, CD_BOOLEAN_VALUES,
            CD_INT_ARRAY, CD_INT_ARRAY, CD_BOOLEAN_VALUES,
            CD_INT_ARRAY, CD_INT_ARRAY, CD_BOOLEAN_VALUES,
            CD_int, CD_int, CD_TABLE, CD_VECTOR_ARRAY, CD_VECTOR_ARRAY, CD_long, CD_LONG_ARRAY, CD_INT_ARRAY);
    private static final MethodTypeDesc LONG_VALUE_TYPE = MethodTypeDesc.of(CD_long, CD_int);
    private static final MethodTypeDesc BOOLEAN_VALUE_TYPE = MethodTypeDesc.of(CD_boolean, CD_int);
    private static final MethodTypeDesc ASSIGN_HASHED_TYPE = MethodTypeDesc.of(CD_long, CD_VECTOR_ARRAY, CD_VECTOR_ARRAY, CD_int, CD_long);

    private static final int POSITIONS = 1;
    private static final int COUNT = 2;
    private static final int LONG_VALUES = 3;
    private static final int LONG_NULLS = 4;
    private static final int FIRST_IDS = 5;
    private static final int FIRST_GLOBAL_IDS = 6;
    private static final int FIRST_NULLS = 7;
    private static final int SECOND_IDS = 8;
    private static final int SECOND_GLOBAL_IDS = 9;
    private static final int SECOND_NULLS = 10;
    private static final int FIRST_RADIX = 11;
    private static final int SECOND_RADIX = 12;
    private static final int TABLE = 13;
    private static final int VALUES = 14;
    private static final int NULLS = 15;
    private static final int START_NEXT_ID = 16;
    private static final int OUTPUT = 18;
    private static final int COMPOSITE_CACHE = 19;
    private static final int NEXT_ID = 20;
    private static final int INDEX = 22;
    private static final int POSITION = 23;
    private static final int LONG_DIGIT = 24;
    private static final int FIRST_DIGIT = 26;
    private static final int SECOND_DIGIT = 28;
    private static final int COMPOSITE = 30;
    private static final int GROUP = 31;

    private final ConcurrentHashMap<Integer, MixedComposite3GroupingKernel> kernels = new ConcurrentHashMap<>();
    private boolean closed;

    MixedComposite3GroupingKernel create(int shape)
    {
        checkOpen();
        return kernels.computeIfAbsent(shape, MixedComposite3GroupingKernelGenerator::generate);
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
            throw new IllegalStateException("Mixed-composite grouping kernel generator is closed");
        }
    }

    static int shape(int longNullShape, int firstNullShape, int secondNullShape)
    {
        return longNullShape | firstNullShape << 2 | secondNullShape << 4;
    }

    private static MixedComposite3GroupingKernel generate(int shape)
    {
        int longNullShape = shape & 3;
        int firstNullShape = (shape >>> 2) & 3;
        int secondNullShape = (shape >>> 4) & 3;
        ClassDesc thisClass = ClassDesc.of("org.weakref.nitro.operator.GeneratedMixedComposite3GroupingKernel" + shape);
        byte[] bytes = ClassFile.of().build(thisClass, builder -> {
            builder.withSuperclass(ClassDesc.of("java.lang.Object"));
            builder.withInterfaceSymbols(CD_KERNEL);
            builder.withFlags(ClassFile.ACC_FINAL | ClassFile.ACC_SYNTHETIC);
            builder.withMethodBody("<init>", MethodTypeDesc.of(CD_void), ClassFile.ACC_PUBLIC, code -> {
                code.aload(0);
                code.invokespecial(ClassDesc.of("java.lang.Object"), "<init>", MethodTypeDesc.of(CD_void));
                code.return_();
            });
            builder.withMethodBody("assign", ASSIGN_TYPE, ClassFile.ACC_PUBLIC,
                    code -> emitAssign(code, longNullShape, firstNullShape, secondNullShape));
        });
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup().defineHiddenClass(bytes, true, MethodHandles.Lookup.ClassOption.NESTMATE);
            return (MixedComposite3GroupingKernel) lookup.findConstructor(lookup.lookupClass(), java.lang.invoke.MethodType.methodType(void.class)).invoke();
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to generate mixed composite grouping kernel for shape " + shape, e);
        }
    }

    private static void emitAssign(CodeBuilder code, int longNullShape, int firstNullShape, int secondNullShape)
    {
        code.lload(START_NEXT_ID);
        code.lstore(NEXT_ID);
        Label sparse = code.newLabel();
        Label end = code.newLabel();
        code.aload(POSITIONS);
        code.ifnonnull(sparse);
        emitLoop(code, false, longNullShape, firstNullShape, secondNullShape);
        code.goto_(end);
        code.labelBinding(sparse);
        emitLoop(code, true, longNullShape, firstNullShape, secondNullShape);
        code.labelBinding(end);
        code.lload(NEXT_ID);
        code.lreturn();
    }

    private static void emitLoop(CodeBuilder code, boolean sparse, int longNullShape, int firstNullShape, int secondNullShape)
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

        Label fallback = code.newLabel();
        Label assigned = code.newLabel();
        emitLongDigit(code, longNullShape);
        emitBinaryDigit(code, firstNullShape, FIRST_NULLS, FIRST_IDS, FIRST_GLOBAL_IDS, FIRST_RADIX, FIRST_DIGIT);
        emitBinaryDigit(code, secondNullShape, SECOND_NULLS, SECOND_IDS, SECOND_GLOBAL_IDS, SECOND_RADIX, SECOND_DIGIT);
        emitBoundsCheck(code, longNullShape, LONG_DIGIT, 16, -1, fallback);
        emitBoundsCheck(code, firstNullShape, FIRST_DIGIT, -1, FIRST_RADIX, fallback);
        emitBoundsCheck(code, secondNullShape, SECOND_DIGIT, -1, SECOND_RADIX, fallback);

        // composite = long + 17 * (first + firstRadix * second)
        code.lload(FIRST_DIGIT);
        code.iload(FIRST_RADIX);
        code.i2l();
        code.lload(SECOND_DIGIT);
        code.lmul();
        code.ladd();
        code.loadConstant(17L);
        code.lmul();
        code.lload(LONG_DIGIT);
        code.ladd();
        code.l2i();
        code.istore(COMPOSITE);

        code.aload(COMPOSITE_CACHE);
        code.iload(COMPOSITE);
        code.iaload();
        code.i2l();
        code.lstore(GROUP);
        code.lload(GROUP);
        code.loadConstant(0L);
        code.lcmp();
        code.ifge(assigned);
        emitHashedAssign(code);
        code.aload(COMPOSITE_CACHE);
        code.iload(COMPOSITE);
        code.lload(GROUP);
        code.l2i();
        code.iastore();
        code.goto_(assigned);

        code.labelBinding(fallback);
        emitHashedAssign(code);

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
    }

    private static void emitLongDigit(CodeBuilder code, int nullShape)
    {
        if (nullShape == ALL_NULL) {
            code.loadConstant(16L);
            code.lstore(LONG_DIGIT);
            return;
        }
        if (nullShape == MIXED) {
            Label notNull = code.newLabel();
            Label done = code.newLabel();
            code.aload(LONG_NULLS);
            code.iload(POSITION);
            code.invokeinterface(CD_BOOLEAN_VALUES, "value", BOOLEAN_VALUE_TYPE);
            code.ifeq(notNull);
            code.loadConstant(16L);
            code.lstore(LONG_DIGIT);
            code.goto_(done);
            code.labelBinding(notNull);
            emitLongValue(code);
            code.labelBinding(done);
            return;
        }
        emitLongValue(code);
    }

    private static void emitLongValue(CodeBuilder code)
    {
        code.aload(LONG_VALUES);
        code.iload(POSITION);
        code.invokeinterface(CD_LONG_VALUES, "value", LONG_VALUE_TYPE);
        code.lstore(LONG_DIGIT);
    }

    private static void emitBinaryDigit(CodeBuilder code, int nullShape, int nullAccessor, int ids, int globals, int radix, int target)
    {
        if (nullShape == ALL_NULL) {
            code.iload(radix);
            code.loadConstant(1);
            code.isub();
            code.i2l();
            code.lstore(target);
            return;
        }
        if (nullShape == MIXED) {
            Label notNull = code.newLabel();
            Label done = code.newLabel();
            code.aload(nullAccessor);
            code.iload(POSITION);
            code.invokeinterface(CD_BOOLEAN_VALUES, "value", BOOLEAN_VALUE_TYPE);
            code.ifeq(notNull);
            code.iload(radix);
            code.loadConstant(1);
            code.isub();
            code.i2l();
            code.lstore(target);
            code.goto_(done);
            code.labelBinding(notNull);
            emitBinaryValue(code, ids, globals, target);
            code.labelBinding(done);
            return;
        }
        emitBinaryValue(code, ids, globals, target);
    }

    private static void emitBinaryValue(CodeBuilder code, int ids, int globals, int target)
    {
        code.aload(globals);
        code.aload(ids);
        code.iload(POSITION);
        code.iaload();
        code.iaload();
        code.i2l();
        code.lstore(target);
    }

    private static void emitBoundsCheck(CodeBuilder code, int nullShape, int digit, int fixedLimit, int radix, Label fallback)
    {
        code.lload(digit);
        code.loadConstant(0L);
        code.lcmp();
        code.iflt(fallback);
        code.lload(digit);
        if (fixedLimit >= 0) {
            code.loadConstant((long) (nullShape == ALL_NULL ? fixedLimit + 1 : fixedLimit));
        }
        else {
            code.iload(radix);
            if (nullShape != ALL_NULL) {
                code.loadConstant(1);
                code.isub();
            }
            code.i2l();
        }
        code.lcmp();
        code.ifge(fallback);
    }

    private static void emitHashedAssign(CodeBuilder code)
    {
        code.aload(TABLE);
        code.aload(VALUES);
        code.aload(NULLS);
        code.iload(POSITION);
        code.lload(NEXT_ID);
        code.invokevirtual(CD_TABLE, "assignGroupHashed", ASSIGN_HASHED_TYPE);
        code.lstore(GROUP);
    }
}
