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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.constant.ConstantDescs.CD_boolean;
import static java.lang.constant.ConstantDescs.CD_int;
import static java.lang.constant.ConstantDescs.CD_long;
import static java.lang.constant.ConstantDescs.CD_void;

/** Emits an unrolled exact record comparator for a resolved physical key shape. */
final class DictionaryRecordEqualityKernelGenerator
        implements AutoCloseable
{
    static final int NULL_FREE = 0;
    static final int ALL_NULL = 1;
    static final int MIXED = 2;

    static final int LONG = 0;
    static final int EMBEDDED_BINARY_ID = 1;
    static final int COMPACT_BINARY_ID = 2;
    static final int EXACT_BINARY = 3;

    private static final ClassDesc CD_KERNEL = ClassDesc.of("org.weakref.nitro.operator.DictionaryRecordEqualityKernel");
    private static final ClassDesc CD_LAYOUT = ClassDesc.of("org.weakref.nitro.operator.FlatKeyLayout");
    private static final ClassDesc CD_BYTE_ARRAY = ClassDesc.ofDescriptor("[B");
    private static final ClassDesc CD_ARENA = ClassDesc.of("org.weakref.nitro.operator.FlatGroupingTable$FlatVariableWidthArena");
    private static final ClassDesc CD_VECTOR_ARRAY = ClassDesc.ofDescriptor("[Lorg/weakref/nitro/data/Vector;");
    private static final MethodTypeDesc IDENTICAL_TYPE = MethodTypeDesc.of(
            CD_int, CD_LAYOUT, CD_BYTE_ARRAY, CD_int, CD_ARENA, CD_VECTOR_ARRAY, CD_int, CD_int);
    private static final MethodTypeDesc INPUT_NULL_TYPE = MethodTypeDesc.of(CD_boolean, CD_int, CD_int);
    private static final MethodTypeDesc INPUT_LONG_TYPE = MethodTypeDesc.of(CD_long, CD_int, CD_int);
    private static final MethodTypeDesc INPUT_GLOBAL_ID_TYPE = MethodTypeDesc.of(CD_int, CD_int, CD_int);
    private static final MethodTypeDesc RECORD_INT_TYPE = MethodTypeDesc.of(CD_int, CD_BYTE_ARRAY, CD_int);
    private static final MethodTypeDesc RECORD_LONG_TYPE = MethodTypeDesc.of(CD_long, CD_BYTE_ARRAY, CD_int);
    private static final MethodTypeDesc LAYOUT_RECORD_LONG_TYPE = MethodTypeDesc.of(CD_long, CD_int, CD_BYTE_ARRAY, CD_int);
    private static final MethodTypeDesc EXACT_BINARY_TYPE = MethodTypeDesc.of(
            CD_boolean, CD_int, CD_BYTE_ARRAY, CD_int, CD_ARENA, CD_VECTOR_ARRAY, CD_int, CD_int);

    private static final int LAYOUT = 1;
    private static final int FIXED_CHUNK = 2;
    private static final int FIXED_OFFSET = 3;
    private static final int VARIABLE_WIDTH_ARENA = 4;
    private static final int VALUES = 5;
    private static final int POSITION = 6;
    private static final int RECORD_INDEX = 7;
    private static final int INPUT_NULL = 8;
    private static final int INPUT_GLOBAL_ID = 9;

    private final ConcurrentHashMap<Shape, DictionaryRecordEqualityKernel> kernels = new ConcurrentHashMap<>();
    private final AtomicInteger nextClassId = new AtomicInteger();
    private boolean closed;

    DictionaryRecordEqualityKernel create(Shape shape)
    {
        checkOpen();
        return kernels.computeIfAbsent(shape, this::generate);
    }

    private DictionaryRecordEqualityKernel generate(Shape shape)
    {
        ClassDesc thisClass = ClassDesc.of("org.weakref.nitro.operator.GeneratedDictionaryRecordEqualityKernel" + nextClassId.incrementAndGet());
        byte[] bytes = ClassFile.of().build(thisClass, builder -> {
            builder.withSuperclass(ClassDesc.of("java.lang.Object"));
            builder.withInterfaceSymbols(CD_KERNEL);
            builder.withFlags(ClassFile.ACC_FINAL | ClassFile.ACC_SYNTHETIC);
            builder.withMethodBody("<init>", MethodTypeDesc.of(CD_void), ClassFile.ACC_PUBLIC, code -> {
                code.aload(0);
                code.invokespecial(ClassDesc.of("java.lang.Object"), "<init>", MethodTypeDesc.of(CD_void));
                code.return_();
            });
            builder.withMethodBody("identical", IDENTICAL_TYPE, ClassFile.ACC_PUBLIC, code -> emitIdentical(code, shape));
        });
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup().defineHiddenClass(bytes, true, MethodHandles.Lookup.ClassOption.NESTMATE);
            return (DictionaryRecordEqualityKernel) lookup.findConstructor(lookup.lookupClass(), java.lang.invoke.MethodType.methodType(void.class)).invoke();
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to generate dictionary record equality kernel for " + shape, e);
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
            throw new IllegalStateException("Dictionary record equality kernel generator is closed");
        }
    }

    private static void emitIdentical(CodeBuilder code, Shape shape)
    {
        Label different = code.newLabel();
        Label fallback = code.newLabel();
        for (FieldShape fieldShape : shape.fields()) {
            int field = fieldShape.field();
            int nullShape = fieldShape.nullShape();
            Label fieldDone = code.newLabel();
            if (!shape.nullableRecord()) {
                // A non-nullable flat record has no leading null byte. Do not interpret the first key byte as a
                // null bitmap merely because this batch's logical null shape is NULL_FREE.
            }
            else if (nullShape == NULL_FREE) {
                emitRecordNull(code, field);
                code.ifne(different);
            }
            else if (nullShape == ALL_NULL) {
                emitRecordNull(code, field);
                code.ifeq(different);
                continue;
            }
            else {
                code.aload(LAYOUT);
                code.loadConstant(field);
                code.iload(POSITION);
                code.invokevirtual(CD_LAYOUT, "generatedEqualityInputNull", INPUT_NULL_TYPE);
                code.istore(INPUT_NULL);
                Label inputNotNull = code.newLabel();
                code.iload(INPUT_NULL);
                code.ifeq(inputNotNull);
                emitRecordNull(code, field);
                code.ifeq(different);
                code.goto_(fieldDone);
                code.labelBinding(inputNotNull);
                emitRecordNull(code, field);
                code.ifne(different);
            }

            if (fieldShape.kind() == EMBEDDED_BINARY_ID) {
                emitEmbeddedBinaryComparison(code, fieldShape.fixedOffset(), field, different, fallback);
            }
            else if (fieldShape.kind() == COMPACT_BINARY_ID) {
                emitCompactBinaryComparison(code, fieldShape.fixedOffset(), field, different);
            }
            else if (fieldShape.kind() == EXACT_BINARY) {
                emitExactBinaryComparison(code, fieldShape.fixedOffset(), field, different);
            }
            else {
                emitLongComparison(code, fieldShape, different);
            }
            code.labelBinding(fieldDone);
        }
        code.loadConstant(DictionaryRecordEqualityKernel.IDENTICAL);
        code.ireturn();
        code.labelBinding(different);
        code.loadConstant(DictionaryRecordEqualityKernel.DIFFERENT);
        code.ireturn();
        code.labelBinding(fallback);
        code.loadConstant(DictionaryRecordEqualityKernel.FALLBACK);
        code.ireturn();
    }

    private static void emitRecordNull(CodeBuilder code, int field)
    {
        code.aload(FIXED_CHUNK);
        code.iload(FIXED_OFFSET);
        code.loadConstant(field >>> 3);
        code.iadd();
        code.baload();
        code.loadConstant(1 << (field & 7));
        code.iand();
    }

    private static void emitLongComparison(CodeBuilder code, FieldShape fieldShape, Label different)
    {
        if (fieldShape.fixedSize() == Long.BYTES) {
            emitRecordOffset(code, fieldShape.fixedOffset());
            code.aload(FIXED_CHUNK);
            code.swap();
            code.invokestatic(CD_LAYOUT, "generatedEqualityRecordLong", RECORD_LONG_TYPE);
        }
        else {
            code.aload(LAYOUT);
            code.loadConstant(fieldShape.field());
            code.aload(FIXED_CHUNK);
            emitRecordOffset(code, fieldShape.fixedOffset());
            code.invokevirtual(CD_LAYOUT, "generatedEqualityRecordLong", LAYOUT_RECORD_LONG_TYPE);
        }
        code.aload(LAYOUT);
        code.loadConstant(fieldShape.field());
        code.iload(POSITION);
        code.invokevirtual(CD_LAYOUT, "generatedEqualityInputLong", INPUT_LONG_TYPE);
        code.lcmp();
        code.ifne(different);
    }

    private static void emitEmbeddedBinaryComparison(CodeBuilder code, int fieldOffset, int field, Label different, Label fallback)
    {
        code.aload(FIXED_CHUNK);
        emitRecordOffset(code, fieldOffset + Integer.BYTES * 2);
        code.invokestatic(CD_LAYOUT, "generatedEqualityRecordInt", RECORD_INT_TYPE);
        code.ifge(fallback);

        code.aload(LAYOUT);
        code.loadConstant(field);
        code.iload(POSITION);
        code.invokevirtual(CD_LAYOUT, "generatedEqualityInputGlobalId", INPUT_GLOBAL_ID_TYPE);
        code.istore(INPUT_GLOBAL_ID);
        code.iload(INPUT_GLOBAL_ID);
        code.iflt(fallback);

        code.aload(FIXED_CHUNK);
        emitRecordOffset(code, fieldOffset);
        code.invokestatic(CD_LAYOUT, "generatedEqualityRecordInt", RECORD_INT_TYPE);
        code.iload(INPUT_GLOBAL_ID);
        code.if_icmpne(different);
    }

    private static void emitCompactBinaryComparison(CodeBuilder code, int fieldOffset, int field, Label different)
    {
        Label exact = code.newLabel();
        Label done = code.newLabel();
        code.aload(FIXED_CHUNK);
        emitRecordOffset(code, fieldOffset);
        code.invokestatic(CD_LAYOUT, "generatedEqualityRecordInt", RECORD_INT_TYPE);
        code.istore(INPUT_NULL);
        code.iload(INPUT_NULL);
        code.iflt(exact);

        code.aload(LAYOUT);
        code.loadConstant(field);
        code.iload(POSITION);
        code.invokevirtual(CD_LAYOUT, "generatedEqualityInputGlobalId", INPUT_GLOBAL_ID_TYPE);
        code.istore(INPUT_GLOBAL_ID);
        code.iload(INPUT_GLOBAL_ID);
        code.iflt(exact);
        code.iload(INPUT_GLOBAL_ID);
        code.iload(INPUT_NULL);
        code.if_icmpne(different);
        code.goto_(done);

        code.labelBinding(exact);
        code.aload(LAYOUT);
        code.loadConstant(field);
        code.aload(FIXED_CHUNK);
        emitRecordOffset(code, fieldOffset);
        code.aload(VARIABLE_WIDTH_ARENA);
        code.aload(VALUES);
        code.iload(POSITION);
        code.iload(RECORD_INDEX);
        code.invokevirtual(CD_LAYOUT, "generatedEqualityExactBinary", EXACT_BINARY_TYPE);
        code.ifeq(different);
        code.labelBinding(done);
    }

    private static void emitExactBinaryComparison(CodeBuilder code, int fieldOffset, int field, Label different)
    {
        code.aload(LAYOUT);
        code.loadConstant(field);
        code.aload(FIXED_CHUNK);
        emitRecordOffset(code, fieldOffset);
        code.aload(VARIABLE_WIDTH_ARENA);
        code.aload(VALUES);
        code.iload(POSITION);
        code.iload(RECORD_INDEX);
        code.invokevirtual(CD_LAYOUT, "generatedEqualityExactBinary", EXACT_BINARY_TYPE);
        code.ifeq(different);
    }

    private static void emitRecordOffset(CodeBuilder code, int fieldOffset)
    {
        code.iload(FIXED_OFFSET);
        code.loadConstant(fieldOffset);
        code.iadd();
    }

    record Shape(boolean nullableRecord, List<FieldShape> fields)
    {
        Shape
        {
            fields = List.copyOf(fields);
        }
    }

    record FieldShape(int field, int nullShape, int kind, int fixedOffset, int fixedSize) {}
}
