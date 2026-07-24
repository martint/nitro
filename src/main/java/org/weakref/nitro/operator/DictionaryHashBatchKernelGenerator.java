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

/** Emits an unrolled logical hash pass for a concrete dictionary-assisted physical key and null shape. */
final class DictionaryHashBatchKernelGenerator
        implements AutoCloseable
{
    static final int NULL_FREE = 0;
    static final int ALL_NULL = 1;
    static final int MIXED = 2;
    static final int LONG_ACCESSOR_HASH = 1;

    static final int HASH_MODE_SHIFT = 18;

    private static final int ASSIGN_TILE_ROWS =
            Math.max(1, Integer.getInteger("nitro.group.generatedDictionaryHashProbeTileRows", 72));

    private static final ClassDesc CD_KERNEL = ClassDesc.of("org.weakref.nitro.operator.DictionaryHashBatchKernel");
    private static final ClassDesc CD_BOOLEAN_VALUES = ClassDesc.of("org.weakref.nitro.data.VectorAccess$BooleanValues");
    private static final ClassDesc CD_LONG_VALUES = ClassDesc.of("org.weakref.nitro.data.VectorAccess$LongValues");
    private static final ClassDesc CD_TABLE = ClassDesc.of("org.weakref.nitro.operator.FlatGroupingTable");
    private static final ClassDesc CD_VECTOR = ClassDesc.of("org.weakref.nitro.data.Vector");
    private static final ClassDesc CD_LONG_BOX = ClassDesc.of("java.lang.Long");
    private static final ClassDesc CD_INT_ARRAY = CD_int.arrayType();
    private static final ClassDesc CD_LONG_ARRAY = CD_long.arrayType();
    private static final ClassDesc CD_INT_ARRAY_ARRAY = CD_INT_ARRAY.arrayType();
    private static final ClassDesc CD_LONG_ARRAY_ARRAY = CD_LONG_ARRAY.arrayType();
    private static final ClassDesc CD_BOOLEAN_VALUES_ARRAY = CD_BOOLEAN_VALUES.arrayType();
    private static final ClassDesc CD_LONG_VALUES_ARRAY = CD_LONG_VALUES.arrayType();
    private static final ClassDesc CD_VECTOR_ARRAY = CD_VECTOR.arrayType();
    private static final MethodTypeDesc HASH_TYPE = MethodTypeDesc.of(
            CD_void, CD_int, CD_INT_ARRAY_ARRAY, CD_LONG_ARRAY_ARRAY, CD_LONG_VALUES_ARRAY, CD_BOOLEAN_VALUES_ARRAY, CD_LONG_ARRAY);
    private static final MethodTypeDesc ASSIGN_TYPE = MethodTypeDesc.of(
            CD_long,
            CD_int, CD_INT_ARRAY_ARRAY, CD_LONG_ARRAY_ARRAY, CD_LONG_VALUES_ARRAY, CD_BOOLEAN_VALUES_ARRAY,
            CD_TABLE, CD_VECTOR_ARRAY, CD_VECTOR_ARRAY, CD_long, CD_LONG_ARRAY);
    private static final MethodTypeDesc BOOLEAN_VALUE_TYPE = MethodTypeDesc.of(CD_boolean, CD_int);
    private static final MethodTypeDesc LONG_VALUE_TYPE = MethodTypeDesc.of(CD_long, CD_int);
    private static final MethodTypeDesc LONG_HASH_CODE_TYPE = MethodTypeDesc.of(CD_int, CD_long);
    private static final MethodTypeDesc ASSIGN_HASH_TYPE = MethodTypeDesc.of(
            CD_long, CD_VECTOR_ARRAY, CD_VECTOR_ARRAY, CD_int, CD_long, CD_long);

    private static final int COUNT = 1;
    private static final int DICTIONARY_IDS = 2;
    private static final int ENTRY_HASHES = 3;
    private static final int LONG_VALUES = 4;
    private static final int NULLS = 5;
    private static final int OUTPUT = 6;
    private static final int POSITION = 7;
    private static final int RESULT = 8;

    private static final int ASSIGN_TABLE = 6;
    private static final int ASSIGN_VALUES = 7;
    private static final int ASSIGN_NULLS = 8;
    private static final int ASSIGN_NEXT_GROUP_ID = 9;
    private static final int ASSIGN_OUTPUT = 11;
    private static final int ASSIGN_POSITION = 12;
    private static final int ASSIGN_HASH = 13;
    private static final int ASSIGN_GROUP_ID = 15;
    private static final int ASSIGN_TILE_END = 17;
    private static final int ASSIGN_TILE_START = 18;

    private final ConcurrentHashMap<Integer, DictionaryHashBatchKernel> kernels = new ConcurrentHashMap<>();
    private boolean closed;

    DictionaryHashBatchKernel create(int shape)
    {
        checkOpen();
        return kernels.computeIfAbsent(shape, DictionaryHashBatchKernelGenerator::generate);
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
            throw new IllegalStateException("Dictionary hash batch kernel generator is closed");
        }
    }

    private static DictionaryHashBatchKernel generate(int shape)
    {
        int fieldCount = shape & 0xF;
        ClassDesc thisClass = ClassDesc.of("org.weakref.nitro.operator.GeneratedDictionaryHashBatchKernel" + Integer.toUnsignedString(shape));
        byte[] bytes = ClassFile.of().build(thisClass, builder -> {
            builder.withSuperclass(ClassDesc.of("java.lang.Object"));
            builder.withInterfaceSymbols(CD_KERNEL);
            builder.withFlags(ClassFile.ACC_FINAL | ClassFile.ACC_SYNTHETIC);
            builder.withMethodBody("<init>", MethodTypeDesc.of(CD_void), ClassFile.ACC_PUBLIC, code -> {
                code.aload(0);
                code.invokespecial(ClassDesc.of("java.lang.Object"), "<init>", MethodTypeDesc.of(CD_void));
                code.return_();
            });
            builder.withMethodBody("hash", HASH_TYPE, ClassFile.ACC_PUBLIC, code -> emitHash(code, shape, fieldCount));
            builder.withMethodBody("assign", ASSIGN_TYPE, ClassFile.ACC_PUBLIC, code -> emitAssign(code, shape, fieldCount));
        });
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup().defineHiddenClass(bytes, true, MethodHandles.Lookup.ClassOption.NESTMATE);
            return (DictionaryHashBatchKernel) lookup.findConstructor(lookup.lookupClass(), java.lang.invoke.MethodType.methodType(void.class)).invoke();
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to generate dictionary hash batch kernel for shape " + shape, e);
        }
    }

    private static void emitHash(CodeBuilder code, int shape, int fieldCount)
    {
        code.loadConstant(0);
        code.istore(POSITION);
        Label top = code.newLabel();
        Label exit = code.newLabel();
        code.labelBinding(top);
        code.iload(POSITION);
        code.iload(COUNT);
        code.if_icmpge(exit);
        code.loadConstant(1L);
        code.lstore(RESULT);

        for (int field = 0; field < fieldCount; field++) {
            int nullShape = (shape >>> (4 + field * 2)) & 3;
            int hashMode = (shape >>> (HASH_MODE_SHIFT + field * 2)) & 3;
            code.lload(RESULT);
            code.loadConstant(31L);
            code.lmul();
            emitFieldHash(code, field, nullShape, hashMode, POSITION);
            code.ladd();
            code.lstore(RESULT);
        }

        code.aload(OUTPUT);
        code.iload(POSITION);
        code.lload(RESULT);
        code.lastore();
        code.iinc(POSITION, 1);
        code.goto_(top);
        code.labelBinding(exit);
        code.return_();
    }

    private static void emitFieldHash(CodeBuilder code, int field, int nullShape, int hashMode, int position)
    {
        if (nullShape == ALL_NULL) {
            code.loadConstant(1L);
            return;
        }
        if (nullShape == MIXED) {
            Label notNull = code.newLabel();
            Label done = code.newLabel();
            code.aload(NULLS);
            code.loadConstant(field);
            code.aaload();
            code.iload(position);
            code.invokeinterface(CD_BOOLEAN_VALUES, "value", BOOLEAN_VALUE_TYPE);
            code.ifeq(notNull);
            code.loadConstant(1L);
            code.goto_(done);
            code.labelBinding(notNull);
            emitValueHash(code, field, hashMode, position);
            code.labelBinding(done);
            return;
        }
        emitValueHash(code, field, hashMode, position);
    }

    private static void emitValueHash(CodeBuilder code, int field, int hashMode, int position)
    {
        if (hashMode == LONG_ACCESSOR_HASH) {
            code.aload(LONG_VALUES);
            code.loadConstant(field);
            code.aaload();
            code.iload(position);
            code.invokeinterface(CD_LONG_VALUES, "value", LONG_VALUE_TYPE);
            code.invokestatic(CD_LONG_BOX, "hashCode", LONG_HASH_CODE_TYPE);
            code.i2l();
            return;
        }
        emitDictionaryHash(code, field, position);
    }

    private static void emitDictionaryHash(CodeBuilder code, int field, int position)
    {
        code.aload(ENTRY_HASHES);
        code.loadConstant(field);
        code.aaload();
        code.aload(DICTIONARY_IDS);
        code.loadConstant(field);
        code.aaload();
        code.iload(position);
        code.iaload();
        code.laload();
    }

    private static void emitAssign(CodeBuilder code, int shape, int fieldCount)
    {
        code.loadConstant(0);
        code.istore(ASSIGN_TILE_START);
        Label tileTop = code.newLabel();
        Label exit = code.newLabel();
        code.labelBinding(tileTop);
        code.iload(ASSIGN_TILE_START);
        code.iload(COUNT);
        code.if_icmpge(exit);

        code.iload(ASSIGN_TILE_START);
        code.loadConstant(ASSIGN_TILE_ROWS);
        code.iadd();
        code.istore(ASSIGN_TILE_END);
        Label boundedTile = code.newLabel();
        code.iload(ASSIGN_TILE_END);
        code.iload(COUNT);
        code.if_icmple(boundedTile);
        code.iload(COUNT);
        code.istore(ASSIGN_TILE_END);
        code.labelBinding(boundedTile);

        // Hash a bounded tile first so independent dictionary and accessor loads can overlap. The caller-owned
        // result vector is safe scratch until each position is replaced by its final group id in the probe pass.
        code.iload(ASSIGN_TILE_START);
        code.istore(ASSIGN_POSITION);
        Label hashTop = code.newLabel();
        Label probeStart = code.newLabel();
        code.labelBinding(hashTop);
        code.iload(ASSIGN_POSITION);
        code.iload(ASSIGN_TILE_END);
        code.if_icmpge(probeStart);
        code.loadConstant(1L);
        code.lstore(ASSIGN_HASH);
        for (int field = 0; field < fieldCount; field++) {
            int nullShape = (shape >>> (4 + field * 2)) & 3;
            int hashMode = (shape >>> (HASH_MODE_SHIFT + field * 2)) & 3;
            code.lload(ASSIGN_HASH);
            code.loadConstant(31L);
            code.lmul();
            emitFieldHash(code, field, nullShape, hashMode, ASSIGN_POSITION);
            code.ladd();
            code.lstore(ASSIGN_HASH);
        }
        code.aload(ASSIGN_OUTPUT);
        code.iload(ASSIGN_POSITION);
        code.lload(ASSIGN_HASH);
        code.lastore();
        code.iinc(ASSIGN_POSITION, 1);
        code.goto_(hashTop);

        code.labelBinding(probeStart);
        code.iload(ASSIGN_TILE_START);
        code.istore(ASSIGN_POSITION);
        Label probeTop = code.newLabel();
        Label tileDone = code.newLabel();
        code.labelBinding(probeTop);
        code.iload(ASSIGN_POSITION);
        code.iload(ASSIGN_TILE_END);
        code.if_icmpge(tileDone);
        code.aload(ASSIGN_OUTPUT);
        code.iload(ASSIGN_POSITION);
        code.laload();
        code.lstore(ASSIGN_HASH);
        code.aload(ASSIGN_TABLE);
        code.aload(ASSIGN_VALUES);
        code.aload(ASSIGN_NULLS);
        code.iload(ASSIGN_POSITION);
        code.lload(ASSIGN_NEXT_GROUP_ID);
        code.lload(ASSIGN_HASH);
        code.invokevirtual(CD_TABLE, "assignGroupWithHash", ASSIGN_HASH_TYPE);
        code.lstore(ASSIGN_GROUP_ID);
        Label existing = code.newLabel();
        code.lload(ASSIGN_GROUP_ID);
        code.lload(ASSIGN_NEXT_GROUP_ID);
        code.lcmp();
        code.ifne(existing);
        code.lload(ASSIGN_NEXT_GROUP_ID);
        code.loadConstant(1L);
        code.ladd();
        code.lstore(ASSIGN_NEXT_GROUP_ID);
        code.labelBinding(existing);
        code.aload(ASSIGN_OUTPUT);
        code.iload(ASSIGN_POSITION);
        code.lload(ASSIGN_GROUP_ID);
        code.lastore();

        code.iinc(ASSIGN_POSITION, 1);
        code.goto_(probeTop);
        code.labelBinding(tileDone);
        code.iload(ASSIGN_TILE_END);
        code.istore(ASSIGN_TILE_START);
        code.goto_(tileTop);
        code.labelBinding(exit);
        code.lload(ASSIGN_NEXT_GROUP_ID);
        code.lreturn();
    }
}
