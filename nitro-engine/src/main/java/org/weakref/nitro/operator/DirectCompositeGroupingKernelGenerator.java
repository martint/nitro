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

import static java.lang.constant.ConstantDescs.CD_int;
import static java.lang.constant.ConstantDescs.CD_long;
import static java.lang.constant.ConstantDescs.CD_void;

/** Emits a null-free direct-composite grouping loop for an arbitrary physical field shape. */
final class DirectCompositeGroupingKernelGenerator
        implements AutoCloseable
{
    private record Shape(long bits, int fieldCount) {}

    private record FieldLocals(boolean binary, boolean subtractBase, int values, int ids, int radix, int base) {}

    private static final ClassDesc CD_KERNEL = ClassDesc.of("org.weakref.nitro.operator.DirectCompositeGroupingKernel");
    private static final ClassDesc CD_TABLE = ClassDesc.of("org.weakref.nitro.operator.FlatGroupingTable");
    private static final ClassDesc CD_VECTOR = ClassDesc.of("org.weakref.nitro.data.Vector");
    private static final ClassDesc CD_LONG_VALUES = ClassDesc.of("org.weakref.nitro.data.VectorAccess$LongValues");
    private static final ClassDesc CD_INT_ARRAY = CD_int.arrayType();
    private static final ClassDesc CD_LONG_ARRAY = CD_long.arrayType();
    private static final ClassDesc CD_INT_ARRAY_ARRAY = CD_INT_ARRAY.arrayType();
    private static final ClassDesc CD_LONG_VALUES_ARRAY = CD_LONG_VALUES.arrayType();
    private static final ClassDesc CD_VECTOR_ARRAY = CD_VECTOR.arrayType();

    private static final MethodTypeDesc ASSIGN_TYPE = MethodTypeDesc.of(
            CD_long,
            CD_INT_ARRAY, CD_int, CD_LONG_VALUES_ARRAY, CD_INT_ARRAY_ARRAY, CD_INT_ARRAY_ARRAY,
            CD_INT_ARRAY, CD_LONG_ARRAY, CD_TABLE, CD_VECTOR_ARRAY, CD_VECTOR_ARRAY, CD_long,
            CD_LONG_ARRAY, CD_INT_ARRAY);
    private static final MethodTypeDesc LONG_VALUE_TYPE = MethodTypeDesc.of(CD_long, CD_int);
    private static final MethodTypeDesc ASSIGN_HASHED_TYPE = MethodTypeDesc.of(CD_long, CD_VECTOR_ARRAY, CD_VECTOR_ARRAY, CD_int, CD_long);

    private static final int POSITIONS = 1;
    private static final int COUNT = 2;
    private static final int LONG_VALUES = 3;
    private static final int IDS = 4;
    private static final int GLOBAL_IDS = 5;
    private static final int RADICES = 6;
    private static final int LONG_BASES = 7;
    private static final int TABLE = 8;
    private static final int VALUES = 9;
    private static final int NULLS = 10;
    private static final int START_NEXT_ID = 11;
    private static final int OUTPUT = 13;
    private static final int COMPOSITE_CACHE = 14;
    private static final int NEXT_ID = 15;
    private static final int INDEX = 17;
    private static final int POSITION = 18;
    private static final int COMPOSITE = 19;
    private static final int MULTIPLIER = 21;
    private static final int DIGIT = 23;
    private static final int GROUP = 25;

    private final ConcurrentHashMap<Shape, DirectCompositeGroupingKernel> kernels = new ConcurrentHashMap<>();
    private boolean closed;

    DirectCompositeGroupingKernel create(long shape, int fieldCount)
    {
        checkOpen();
        return kernels.computeIfAbsent(new Shape(shape, fieldCount), DirectCompositeGroupingKernelGenerator::generate);
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
            throw new IllegalStateException("Direct-composite grouping kernel generator is closed");
        }
    }

    static long fieldShape(long shape, int field, boolean binary, boolean subtractBase)
    {
        long bits = (binary ? 1L : 0L) | (subtractBase ? 2L : 0L);
        return shape | bits << (field * 2);
    }

    private static DirectCompositeGroupingKernel generate(Shape shape)
    {
        ClassDesc thisClass = ClassDesc.of("org.weakref.nitro.operator.GeneratedDirectCompositeGroupingKernel" +
                shape.fieldCount() + "_" + Long.toUnsignedString(shape.bits(), 16));
        byte[] bytes = ClassFile.of().build(thisClass, builder -> {
            builder.withSuperclass(ClassDesc.of("java.lang.Object"));
            builder.withInterfaceSymbols(CD_KERNEL);
            builder.withFlags(ClassFile.ACC_FINAL | ClassFile.ACC_SYNTHETIC);
            builder.withMethodBody("<init>", MethodTypeDesc.of(CD_void), ClassFile.ACC_PUBLIC, code -> {
                code.aload(0);
                code.invokespecial(ClassDesc.of("java.lang.Object"), "<init>", MethodTypeDesc.of(CD_void));
                code.return_();
            });
            builder.withMethodBody("assign", ASSIGN_TYPE, ClassFile.ACC_PUBLIC, code -> emitAssign(code, shape));
        });
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup().defineHiddenClass(bytes, true, MethodHandles.Lookup.ClassOption.NESTMATE);
            return (DirectCompositeGroupingKernel) lookup.findConstructor(lookup.lookupClass(), java.lang.invoke.MethodType.methodType(void.class)).invoke();
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to generate direct-composite grouping kernel for " + shape, e);
        }
    }

    private static void emitAssign(CodeBuilder code, Shape shape)
    {
        FieldLocals[] fields = new FieldLocals[shape.fieldCount()];
        int nextLocal = 27;
        for (int field = 0; field < shape.fieldCount(); field++) {
            boolean binary = ((shape.bits() >>> (field * 2)) & 1) != 0;
            boolean subtractBase = ((shape.bits() >>> (field * 2)) & 2) != 0;
            if (binary) {
                int globalIds = nextLocal++;
                int ids = nextLocal++;
                int radix = nextLocal++;
                code.aload(GLOBAL_IDS);
                code.loadConstant(field);
                code.aaload();
                code.astore(globalIds);
                code.aload(IDS);
                code.loadConstant(field);
                code.aaload();
                code.astore(ids);
                code.aload(RADICES);
                code.loadConstant(field);
                code.iaload();
                code.istore(radix);
                fields[field] = new FieldLocals(true, false, globalIds, ids, radix, -1);
            }
            else {
                int values = nextLocal++;
                code.aload(LONG_VALUES);
                code.loadConstant(field);
                code.aaload();
                code.astore(values);
                int base = -1;
                if (subtractBase) {
                    base = nextLocal;
                    nextLocal += 2;
                    code.aload(LONG_BASES);
                    code.loadConstant(field);
                    code.laload();
                    code.lstore(base);
                }
                fields[field] = new FieldLocals(false, subtractBase, values, -1, -1, base);
            }
        }
        code.lload(START_NEXT_ID);
        code.lstore(NEXT_ID);
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
        code.lstore(COMPOSITE);
        code.loadConstant(1L);
        code.lstore(MULTIPLIER);
        Label fallback = code.newLabel();
        Label assigned = code.newLabel();
        for (int field = 0; field < shape.fieldCount(); field++) {
            FieldLocals locals = fields[field];
            if (locals.binary()) {
                code.aload(locals.values());
                code.aload(locals.ids());
                code.iload(POSITION);
                code.iaload();
                code.iaload();
                code.i2l();
                code.lstore(DIGIT);
            }
            else {
                code.aload(locals.values());
                code.iload(POSITION);
                code.invokeinterface(CD_LONG_VALUES, "value", LONG_VALUE_TYPE);
                if (locals.subtractBase()) {
                    code.lload(locals.base());
                    code.lsub();
                }
                code.lstore(DIGIT);
            }
            code.lload(DIGIT);
            code.loadConstant(0L);
            code.lcmp();
            code.iflt(fallback);
            code.lload(DIGIT);
            if (locals.binary()) {
                code.iload(locals.radix());
                code.loadConstant(1);
                code.isub();
                code.i2l();
            }
            else {
                code.loadConstant(16L);
            }
            code.lcmp();
            code.ifge(fallback);
            code.lload(COMPOSITE);
            code.lload(DIGIT);
            code.lload(MULTIPLIER);
            code.lmul();
            code.ladd();
            code.lstore(COMPOSITE);
            if (field + 1 < shape.fieldCount()) {
                code.lload(MULTIPLIER);
                if (locals.binary()) {
                    code.iload(locals.radix());
                    code.i2l();
                }
                else {
                    code.loadConstant(17L);
                }
                code.lmul();
                code.lstore(MULTIPLIER);
            }
        }

        code.aload(COMPOSITE_CACHE);
        code.lload(COMPOSITE);
        code.l2i();
        code.iaload();
        code.i2l();
        code.lstore(GROUP);
        code.lload(GROUP);
        code.loadConstant(0L);
        code.lcmp();
        code.ifge(assigned);
        emitHashedAssign(code);
        code.aload(COMPOSITE_CACHE);
        code.lload(COMPOSITE);
        code.l2i();
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
        code.lload(NEXT_ID);
        code.lreturn();
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
