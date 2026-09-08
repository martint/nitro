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

import org.weakref.nitro.core.type.FixedWidthKeyLayout;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Vector;

import java.lang.classfile.ClassFile;
import java.lang.classfile.CodeBuilder;
import java.lang.classfile.Label;
import java.lang.constant.ClassDesc;
import java.lang.constant.DirectMethodHandleDesc;
import java.lang.constant.DynamicCallSiteDesc;
import java.lang.constant.MethodTypeDesc;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.constant.ConstantDescs.CD_CallSite;
import static java.lang.constant.ConstantDescs.CD_Object;
import static java.lang.constant.ConstantDescs.CD_boolean;
import static java.lang.constant.ConstantDescs.CD_double;
import static java.lang.constant.ConstantDescs.CD_int;
import static java.lang.constant.ConstantDescs.CD_long;
import static java.lang.constant.ConstantDescs.CD_void;
import static java.lang.constant.DirectMethodHandleDesc.Kind.STATIC;
import static java.lang.constant.MethodHandleDesc.ofMethod;

/** Generates exact flat-table hot methods for canonical lanes composed with direct physical fields. */
final class ProjectedFlatKeyLayoutGenerator
        implements AutoCloseable
{
    private static final ClassDesc CD_BASE = ClassDesc.of("org.weakref.nitro.operator.ProjectedFlatKeyLayout");
    private static final ClassDesc CD_FLAT_LAYOUT = ClassDesc.of("org.weakref.nitro.operator.FlatKeyLayout");
    private static final ClassDesc CD_CONSTRUCTION = ClassDesc.of("org.weakref.nitro.operator.FlatKeyLayout$Construction");
    private static final ClassDesc CD_RESOLVED_LAYOUT = ClassDesc.of("org.weakref.nitro.operator.ResolvedFixedWidthKeyLayout");
    private static final ClassDesc CD_PRIMITIVE_ARRAY_POOL = ClassDesc.of("org.weakref.nitro.data.PrimitiveArrayPool");
    private static final ClassDesc CD_VECTOR = ClassDesc.of("org.weakref.nitro.data.Vector");
    private static final ClassDesc CD_VECTOR_ARRAY = CD_VECTOR.arrayType();
    private static final ClassDesc CD_ARENA = ClassDesc.of("org.weakref.nitro.operator.FlatGroupingTable$FlatVariableWidthArena");
    private static final ClassDesc CD_BYTE_ARRAY = java.lang.constant.ConstantDescs.CD_byte.arrayType();
    private static final ClassDesc CD_INT_ARRAY = CD_int.arrayType();
    private static final ClassDesc CD_OBJECT_ARRAY = CD_Object.arrayType();
    private static final ClassDesc CD_INT_ARRAY_2D = CD_INT_ARRAY.arrayType();
    private static final ClassDesc CD_LONG_ARRAY = CD_long.arrayType();
    private static final ClassDesc CD_DOUBLE_ARRAY = CD_double.arrayType();
    private static final ClassDesc CD_BOOLEAN_ARRAY = CD_boolean.arrayType();
    private static final ClassDesc CD_PROJECTION_BOOTSTRAP = ClassDesc.of("org.weakref.nitro.operator.FixedWidthKeyProjectionBootstrap");
    private static final DirectMethodHandleDesc BSM_PROJECTION = ofMethod(
            STATIC,
            CD_PROJECTION_BOOTSTRAP,
            "bootstrap",
            MethodTypeDesc.of(CD_CallSite, ClassDesc.of("java.lang.invoke.MethodHandles$Lookup"), ClassDesc.of("java.lang.String"), ClassDesc.of("java.lang.invoke.MethodType")));

    private static final MethodTypeDesc CONSTRUCTOR_TYPE = MethodTypeDesc.of(
            CD_void,
            CD_CONSTRUCTION,
            CD_RESOLVED_LAYOUT,
            CD_PRIMITIVE_ARRAY_POOL);
    private static final MethodTypeDesc BATCH_TYPE = MethodTypeDesc.of(CD_void, CD_VECTOR_ARRAY, CD_VECTOR_ARRAY);
    private static final MethodTypeDesc FIELD_HASH_TYPE = MethodTypeDesc.of(CD_long, CD_int, CD_int, CD_VECTOR, CD_int);
    private static final MethodTypeDesc WRITE_FIELD_TYPE = MethodTypeDesc.of(
            CD_void, CD_int, CD_VECTOR, CD_int, CD_BYTE_ARRAY, CD_int, CD_ARENA, CD_int);
    private static final MethodTypeDesc IDENTICAL_FIELD_TYPE = MethodTypeDesc.of(
            CD_boolean, CD_int, CD_BYTE_ARRAY, CD_int, CD_ARENA, CD_VECTOR, CD_int, CD_int);

    private final ConcurrentHashMap<GenerationShape, MethodHandle> constructors = new ConcurrentHashMap<>();
    private final AtomicInteger nextClassId = new AtomicInteger();
    private boolean closed;

    ProjectedFlatKeyLayout create(
            ResolvedPersistentKeyLayout layout,
            Vector[] values,
            List<TypeBinding> types,
            PrimitiveArrayPool arrayPool,
            OperatorCodeGenerationResources codeGeneration,
            FlatKeyTablePolicy policy)
    {
        if (closed) {
            throw new IllegalStateException("Projected flat-key layout generator is closed");
        }
        Vector[] fieldValues = new Vector[layout.fields().length];
        int[] inputChannels = new int[layout.fields().length];
        FlatTypeHandler[] handlers = new FlatTypeHandler[layout.fields().length];
        for (int field = 0; field < layout.fields().length; field++) {
            ResolvedPersistentKeyLayout.Field descriptor = layout.fields()[field];
            int logicalKey = descriptor.logicalKey();
            fieldValues[field] = values[logicalKey];
            inputChannels[field] = logicalKey;
            handlers[field] = descriptor.canonical()
                    ? FlatTypeHandlers.CANONICAL
                    : FlatTypeHandlers.forVector(values[logicalKey], types.get(logicalKey), policy.layout(), false);
            if (handlers[field] == null) {
                throw new IllegalArgumentException("No direct flat-key handler for logical field " + logicalKey);
            }
        }

        FlatKeyLayout.Construction construction = FlatKeyLayout.construction(
                fieldValues,
                inputChannels,
                handlers,
                true,
                arrayPool,
                codeGeneration,
                policy);
        GenerationShape shape = new GenerationShape(
                FixedWidthKeyTableLayout.from(layout.canonicalLayout()),
                Arrays.stream(layout.canonicalFieldIndexes()).boxed().toList());
        MethodHandle constructor = constructors.computeIfAbsent(shape, this::generate);
        try {
            return (ProjectedFlatKeyLayout) constructor.invoke(construction, layout.canonicalLayout(), arrayPool);
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to instantiate generated projected flat-key layout", e);
        }
    }

    @Override
    public void close()
    {
        closed = true;
        constructors.clear();
    }

    private MethodHandle generate(GenerationShape shape)
    {
        ClassDesc thisClass = ClassDesc.of("org.weakref.nitro.operator.GeneratedProjectedFlatKeyLayout" + nextClassId.incrementAndGet());
        byte[] bytes = ClassFile.of().build(thisClass, builder -> {
            builder.withSuperclass(CD_BASE);
            builder.withFlags(ClassFile.ACC_FINAL | ClassFile.ACC_SYNTHETIC);
            for (int source = 0; source < shape.layout().sourceCarriers().size(); source++) {
                builder.withField(valuesField(source), arrayDescriptor(shape.layout().sourceCarriers().get(source)), ClassFile.ACC_PRIVATE);
                builder.withField(mappingField(source), CD_INT_ARRAY, ClassFile.ACC_PRIVATE);
                builder.withField(mappingOffsetField(source), CD_int, ClassFile.ACC_PRIVATE);
                builder.withField(baseOffsetField(source), CD_int, ClassFile.ACC_PRIVATE);
            }
            builder.withMethodBody("<init>", CONSTRUCTOR_TYPE, ClassFile.ACC_PUBLIC, code -> {
                code.aload(0);
                code.aload(1);
                code.aload(2);
                code.aload(3);
                code.invokespecial(CD_BASE, "<init>", CONSTRUCTOR_TYPE);
                code.return_();
            });
            builder.withMethodBody("beginBatch", BATCH_TYPE, ClassFile.ACC_PUBLIC, code -> emitBeginBatch(code, shape, thisClass));
            builder.withMethodBody("endBatch", MethodTypeDesc.of(CD_void), ClassFile.ACC_PUBLIC, code -> emitEndBatch(code, shape, thisClass));
            builder.withMethodBody("fieldHash", FIELD_HASH_TYPE, ClassFile.ACC_PUBLIC, code -> emitFieldHash(code, shape, thisClass));
            builder.withMethodBody("writeFieldFlat", WRITE_FIELD_TYPE, ClassFile.ACC_PUBLIC, code -> emitWriteField(code, shape, thisClass));
            builder.withMethodBody("identicalField", IDENTICAL_FIELD_TYPE, ClassFile.ACC_PUBLIC, code -> emitIdenticalField(code, shape, thisClass));
        });

        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup().defineHiddenClassWithClassData(
                    bytes,
                    shape.layout().projections(),
                    true,
                    MethodHandles.Lookup.ClassOption.NESTMATE);
            return lookup.findConstructor(
                    lookup.lookupClass(),
                    MethodType.methodType(
                            void.class,
                            FlatKeyLayout.Construction.class,
                            ResolvedFixedWidthKeyLayout.class,
                            PrimitiveArrayPool.class));
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to generate projected flat-key layout", e);
        }
    }

    private static void emitBeginBatch(CodeBuilder code, GenerationShape shape, ClassDesc thisClass)
    {
        code.aload(0);
        code.aload(1);
        code.aload(2);
        code.invokespecial(CD_BASE, "beginBatch", BATCH_TYPE);
        for (int source = 0; source < shape.layout().sourceCarriers().size(); source++) {
            code.aload(0);
            code.aload(0);
            code.invokevirtual(CD_BASE, "projectedKeyArrays", MethodTypeDesc.of(CD_OBJECT_ARRAY));
            code.loadConstant(source);
            code.aaload();
            code.checkcast(arrayDescriptor(shape.layout().sourceCarriers().get(source)));
            code.putfield(thisClass, valuesField(source), arrayDescriptor(shape.layout().sourceCarriers().get(source)));

            code.aload(0);
            code.aload(0);
            code.invokevirtual(CD_BASE, "projectedKeyMappings", MethodTypeDesc.of(CD_INT_ARRAY_2D));
            code.loadConstant(source);
            code.aaload();
            code.putfield(thisClass, mappingField(source), CD_INT_ARRAY);

            code.aload(0);
            code.aload(0);
            code.invokevirtual(CD_BASE, "projectedKeyMappingOffsets", MethodTypeDesc.of(CD_INT_ARRAY));
            code.loadConstant(source);
            code.iaload();
            code.putfield(thisClass, mappingOffsetField(source), CD_int);

            code.aload(0);
            code.aload(0);
            code.invokevirtual(CD_BASE, "projectedKeyBaseOffsets", MethodTypeDesc.of(CD_INT_ARRAY));
            code.loadConstant(source);
            code.iaload();
            code.putfield(thisClass, baseOffsetField(source), CD_int);
        }
        code.return_();
    }

    private static void emitEndBatch(CodeBuilder code, GenerationShape shape, ClassDesc thisClass)
    {
        for (int source = 0; source < shape.layout().sourceCarriers().size(); source++) {
            code.aload(0);
            code.aconst_null();
            code.putfield(thisClass, valuesField(source), arrayDescriptor(shape.layout().sourceCarriers().get(source)));
            code.aload(0);
            code.aconst_null();
            code.putfield(thisClass, mappingField(source), CD_INT_ARRAY);
        }
        code.aload(0);
        code.invokespecial(CD_BASE, "endBatch", MethodTypeDesc.of(CD_void));
        code.return_();
    }

    private static void emitFieldHash(CodeBuilder code, GenerationShape shape, ClassDesc thisClass)
    {
        for (int lane = 0; lane < shape.canonicalFieldIndexes().size(); lane++) {
            Label next = code.newLabel();
            code.iload(1);
            code.loadConstant(shape.canonicalFieldIndexes().get(lane));
            code.if_icmpne(next);
            emitProjectedLane(code, shape, thisClass, lane, 4, 5);
            code.invokestatic(ClassDesc.of("java.lang.Long"), "hashCode", MethodTypeDesc.of(CD_int, CD_long));
            code.i2l();
            code.lreturn();
            code.labelBinding(next);
        }
        code.aload(0);
        code.iload(1);
        code.iload(2);
        code.aload(3);
        code.iload(4);
        code.invokespecial(CD_BASE, "fieldHash", FIELD_HASH_TYPE);
        code.lreturn();
    }

    private static void emitWriteField(CodeBuilder code, GenerationShape shape, ClassDesc thisClass)
    {
        for (int lane = 0; lane < shape.canonicalFieldIndexes().size(); lane++) {
            Label next = code.newLabel();
            code.iload(1);
            code.loadConstant(shape.canonicalFieldIndexes().get(lane));
            code.if_icmpne(next);
            code.aload(4);
            code.iload(5);
            emitProjectedLane(code, shape, thisClass, lane, 3, 8);
            code.invokestatic(CD_FLAT_LAYOUT, "writeCanonicalLong", MethodTypeDesc.of(CD_void, CD_BYTE_ARRAY, CD_int, CD_long));
            code.return_();
            code.labelBinding(next);
        }
        code.aload(0);
        code.iload(1);
        code.aload(2);
        code.iload(3);
        code.aload(4);
        code.iload(5);
        code.aload(6);
        code.iload(7);
        code.invokespecial(CD_BASE, "writeFieldFlat", WRITE_FIELD_TYPE);
        code.return_();
    }

    private static void emitIdenticalField(CodeBuilder code, GenerationShape shape, ClassDesc thisClass)
    {
        for (int lane = 0; lane < shape.canonicalFieldIndexes().size(); lane++) {
            Label next = code.newLabel();
            code.iload(1);
            code.loadConstant(shape.canonicalFieldIndexes().get(lane));
            code.if_icmpne(next);
            code.aload(2);
            code.iload(3);
            code.invokestatic(CD_FLAT_LAYOUT, "readCanonicalLong", MethodTypeDesc.of(CD_long, CD_BYTE_ARRAY, CD_int));
            emitProjectedLane(code, shape, thisClass, lane, 6, 8);
            code.lcmp();
            Label different = code.newLabel();
            code.ifne(different);
            code.loadConstant(1);
            code.ireturn();
            code.labelBinding(different);
            code.loadConstant(0);
            code.ireturn();
            code.labelBinding(next);
        }
        code.aload(0);
        code.iload(1);
        code.aload(2);
        code.iload(3);
        code.aload(4);
        code.aload(5);
        code.iload(6);
        code.iload(7);
        code.invokespecial(CD_BASE, "identicalField", IDENTICAL_FIELD_TYPE);
        code.ireturn();
    }

    private static void emitProjectedLane(
            CodeBuilder code,
            GenerationShape shape,
            ClassDesc thisClass,
            int lane,
            int logicalPosition,
            int physicalPosition)
    {
        int sourceOffset = 0;
        for (int previous = 0; previous < lane; previous++) {
            sourceOffset += shape.layout().laneSourceCounts().get(previous);
        }
        int sourceCount = shape.layout().laneSourceCounts().get(lane);
        List<ClassDesc> parameters = new ArrayList<>(sourceCount);
        for (int sourceInLane = 0; sourceInLane < sourceCount; sourceInLane++) {
            int source = sourceOffset + sourceInLane;
            FixedWidthKeyLayout.Carrier carrier = shape.layout().sourceCarriers().get(source);
            emitPhysicalPosition(code, thisClass, source, logicalPosition, physicalPosition);
            code.aload(0);
            code.getfield(thisClass, valuesField(source), arrayDescriptor(carrier));
            code.iload(physicalPosition);
            switch (carrier) {
                case I32 -> code.iaload();
                case I64 -> code.laload();
                case F64 -> code.daload();
                case BOOLEAN -> code.baload();
            }
            parameters.add(carrierDescriptor(carrier));
        }
        if (shape.layout().projections().get(lane).isPresent()) {
            code.invokedynamic(DynamicCallSiteDesc.of(
                    BSM_PROJECTION,
                    "projection" + lane,
                    MethodTypeDesc.of(CD_long, parameters.toArray(ClassDesc[]::new))));
            return;
        }
        switch (shape.layout().sourceCarriers().get(sourceOffset)) {
            case I32, BOOLEAN -> code.i2l();
            case I64 -> {}
            case F64 -> code.invokestatic(
                    ClassDesc.of("java.lang.Double"),
                    "doubleToRawLongBits",
                    MethodTypeDesc.of(CD_long, CD_double));
        }
    }

    private static void emitPhysicalPosition(
            CodeBuilder code,
            ClassDesc thisClass,
            int source,
            int logicalPosition,
            int physicalPosition)
    {
        Label direct = code.newLabel();
        Label mapped = code.newLabel();
        code.aload(0);
        code.getfield(thisClass, mappingField(source), CD_INT_ARRAY);
        code.ifnull(direct);
        code.aload(0);
        code.getfield(thisClass, mappingField(source), CD_INT_ARRAY);
        code.iload(logicalPosition);
        code.aload(0);
        code.getfield(thisClass, mappingOffsetField(source), CD_int);
        code.iadd();
        code.iaload();
        code.istore(physicalPosition);
        code.goto_(mapped);
        code.labelBinding(direct);
        code.iload(logicalPosition);
        code.istore(physicalPosition);
        code.labelBinding(mapped);
        code.iload(physicalPosition);
        code.aload(0);
        code.getfield(thisClass, baseOffsetField(source), CD_int);
        code.iadd();
        code.istore(physicalPosition);
    }

    private static ClassDesc arrayDescriptor(FixedWidthKeyLayout.Carrier carrier)
    {
        return switch (carrier) {
            case I32 -> CD_INT_ARRAY;
            case I64 -> CD_LONG_ARRAY;
            case F64 -> CD_DOUBLE_ARRAY;
            case BOOLEAN -> CD_BOOLEAN_ARRAY;
        };
    }

    private static ClassDesc carrierDescriptor(FixedWidthKeyLayout.Carrier carrier)
    {
        return switch (carrier) {
            case I32 -> CD_int;
            case I64 -> CD_long;
            case F64 -> CD_double;
            case BOOLEAN -> CD_boolean;
        };
    }

    private static String valuesField(int source)
    {
        return "source" + source + "Values";
    }

    private static String mappingField(int source)
    {
        return "source" + source + "Mapping";
    }

    private static String mappingOffsetField(int source)
    {
        return "source" + source + "MappingOffset";
    }

    private static String baseOffsetField(int source)
    {
        return "source" + source + "BaseOffset";
    }

    private record GenerationShape(FixedWidthKeyTableLayout layout, List<Integer> canonicalFieldIndexes)
    {
        private GenerationShape
        {
            canonicalFieldIndexes = List.copyOf(canonicalFieldIndexes);
        }
    }
}
