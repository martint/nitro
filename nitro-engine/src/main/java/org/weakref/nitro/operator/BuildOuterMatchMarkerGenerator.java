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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.constant.ConstantDescs.CD_Object;
import static java.lang.constant.ConstantDescs.CD_boolean;
import static java.lang.constant.ConstantDescs.CD_int;
import static java.lang.constant.ConstantDescs.CD_long;
import static java.lang.constant.ConstantDescs.CD_void;

/** Emits one monomorphic build-identity marker for each complete physical position layout. */
final class BuildOuterMatchMarkerGenerator
        implements AutoCloseable
{
    enum StepKind
    {
        REGION,
        DICTIONARY,
        RLE
    }

    record Shape(
            boolean intValues,
            boolean dense,
            List<StepKind> valueSteps,
            boolean nullable,
            List<StepKind> nullSteps)
    {
        Shape
        {
            valueSteps = List.copyOf(valueSteps);
            nullSteps = List.copyOf(nullSteps);
        }

        static Shape of(boolean intValues, boolean dense, StepKind[] valueSteps, StepKind[] nullSteps)
        {
            return new Shape(
                    intValues,
                    dense,
                    Arrays.asList(valueSteps),
                    nullSteps != null,
                    nullSteps == null ? List.of() : Arrays.asList(nullSteps));
        }
    }

    private static final ClassDesc CD_KERNEL = ClassDesc.of("org.weakref.nitro.operator.BuildOuterMatchMarkerKernel");
    private static final ClassDesc CD_ATOMIC_LONG_ARRAY = ClassDesc.of("java.util.concurrent.atomic.AtomicLongArray");
    private static final ClassDesc CD_MATH = ClassDesc.of("java.lang.Math");
    private static final ClassDesc CD_INT_ARRAY = CD_int.arrayType();
    private static final ClassDesc CD_INT_ARRAY_ARRAY = CD_INT_ARRAY.arrayType();
    private static final ClassDesc CD_LONG_ARRAY = CD_long.arrayType();
    private static final ClassDesc CD_BOOLEAN_ARRAY = CD_boolean.arrayType();
    private static final MethodTypeDesc MARK_TYPE = MethodTypeDesc.of(
            CD_void,
            CD_ATOMIC_LONG_ARRAY,
            CD_Object,
            CD_INT_ARRAY_ARRAY,
            CD_INT_ARRAY,
            CD_BOOLEAN_ARRAY,
            CD_INT_ARRAY_ARRAY,
            CD_INT_ARRAY,
            CD_INT_ARRAY,
            CD_int);
    private static final MethodTypeDesc ATOMIC_GET_TYPE = MethodTypeDesc.of(CD_long, CD_int);
    private static final MethodTypeDesc ATOMIC_COMPARE_AND_SET_TYPE = MethodTypeDesc.of(CD_boolean, CD_int, CD_long, CD_long);
    private static final MethodTypeDesc TO_INT_EXACT_TYPE = MethodTypeDesc.of(CD_int, CD_long);

    private static final int MATCHED = 1;
    private static final int RAW_VALUES = 2;
    private static final int VALUE_STEP_ARRAYS = 3;
    private static final int VALUE_STEP_OFFSETS = 4;
    private static final int NULLS = 5;
    private static final int NULL_STEP_ARRAYS = 6;
    private static final int NULL_STEP_OFFSETS = 7;
    private static final int POSITIONS = 8;
    private static final int COUNT = 9;
    private static final int FIRST_LOCAL = 10;

    private final ConcurrentHashMap<Shape, BuildOuterMatchMarkerKernel> kernels = new ConcurrentHashMap<>();
    private boolean closed;

    BuildOuterMatchMarkerKernel create(Shape shape)
    {
        if (closed) {
            throw new IllegalStateException("Build-outer match marker generator is closed");
        }
        return kernels.computeIfAbsent(shape, BuildOuterMatchMarkerGenerator::generate);
    }

    @Override
    public void close()
    {
        closed = true;
        kernels.clear();
    }

    private static BuildOuterMatchMarkerKernel generate(Shape shape)
    {
        ClassDesc thisClass = ClassDesc.of(
                "org.weakref.nitro.operator.GeneratedBuildOuterMatchMarker" + Integer.toUnsignedString(shape.hashCode()));
        byte[] bytes = ClassFile.of().build(thisClass, builder -> {
            builder.withSuperclass(ClassDesc.of("java.lang.Object"));
            builder.withInterfaceSymbols(CD_KERNEL);
            builder.withFlags(ClassFile.ACC_FINAL | ClassFile.ACC_SYNTHETIC);
            builder.withMethodBody("<init>", MethodTypeDesc.of(CD_void), ClassFile.ACC_PUBLIC, code -> {
                code.aload(0);
                code.invokespecial(ClassDesc.of("java.lang.Object"), "<init>", MethodTypeDesc.of(CD_void));
                code.return_();
            });
            builder.withMethodBody("mark", MARK_TYPE, ClassFile.ACC_PUBLIC, code -> emitMark(code, shape));
        });
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup().defineHiddenClass(bytes, true, MethodHandles.Lookup.ClassOption.NESTMATE);
            return (BuildOuterMatchMarkerKernel) lookup.findConstructor(
                            lookup.lookupClass(),
                            java.lang.invoke.MethodType.methodType(void.class))
                    .invoke();
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to generate build-outer match marker for shape " + shape, e);
        }
    }

    private static void emitMark(CodeBuilder code, Shape shape)
    {
        int nextLocal = FIRST_LOCAL;
        int values = nextLocal++;
        int[] valueStepLocals = new int[shape.valueSteps().size()];
        int[] nullStepLocals = new int[shape.nullSteps().size()];
        int[] valueRleSizeLocals = new int[shape.valueSteps().size()];
        int[] nullRleSizeLocals = new int[shape.nullSteps().size()];
        Arrays.fill(valueRleSizeLocals, -1);
        Arrays.fill(nullRleSizeLocals, -1);
        nextLocal = bindSteps(
                code,
                shape.valueSteps(),
                VALUE_STEP_ARRAYS,
                VALUE_STEP_OFFSETS,
                valueStepLocals,
                valueRleSizeLocals,
                nextLocal);
        nextLocal = bindSteps(
                code,
                shape.nullSteps(),
                NULL_STEP_ARRAYS,
                NULL_STEP_OFFSETS,
                nullStepLocals,
                nullRleSizeLocals,
                nextLocal);
        int index = nextLocal++;
        int position = nextLocal++;
        int physical = nextLocal++;
        int identity = nextLocal++;
        int word = nextLocal++;
        int bit = nextLocal;
        nextLocal += 2;
        int current = nextLocal;
        nextLocal += 2;
        int low = nextLocal++;
        int high = nextLocal++;
        int middle = nextLocal;

        code.aload(RAW_VALUES);
        code.checkcast(shape.intValues() ? CD_INT_ARRAY : CD_LONG_ARRAY);
        code.astore(values);
        code.loadConstant(0);
        code.istore(index);

        Label loop = code.newLabel();
        Label exit = code.newLabel();
        Label next = code.newLabel();
        code.labelBinding(loop);
        code.iload(index);
        code.iload(COUNT);
        code.if_icmpge(exit);

        if (shape.dense()) {
            code.iload(index);
        }
        else {
            code.aload(POSITIONS);
            code.iload(index);
            code.iaload();
        }
        code.istore(position);

        if (shape.nullable()) {
            emitPhysicalPosition(code, shape.nullSteps(), nullStepLocals, nullRleSizeLocals, position, physical, low, high, middle);
            code.aload(NULLS);
            code.iload(physical);
            code.baload();
            code.ifne(next);
        }

        emitPhysicalPosition(code, shape.valueSteps(), valueStepLocals, valueRleSizeLocals, position, physical, low, high, middle);
        code.aload(values);
        code.iload(physical);
        if (shape.intValues()) {
            code.iaload();
        }
        else {
            code.laload();
            code.invokestatic(CD_MATH, "toIntExact", TO_INT_EXACT_TYPE);
        }
        code.istore(identity);

        code.iload(identity);
        code.loadConstant(Long.SIZE);
        code.idiv();
        code.istore(word);
        code.loadConstant(1L);
        code.iload(identity);
        code.loadConstant(Long.SIZE - 1);
        code.iand();
        code.lshl();
        code.lstore(bit);

        Label compareAndSet = code.newLabel();
        code.labelBinding(compareAndSet);
        code.aload(MATCHED);
        code.iload(word);
        code.invokevirtual(CD_ATOMIC_LONG_ARRAY, "get", ATOMIC_GET_TYPE);
        code.lstore(current);
        code.lload(current);
        code.lload(bit);
        code.land();
        code.loadConstant(0L);
        code.lcmp();
        code.ifne(next);
        code.aload(MATCHED);
        code.iload(word);
        code.lload(current);
        code.lload(current);
        code.lload(bit);
        code.lor();
        code.invokevirtual(CD_ATOMIC_LONG_ARRAY, "compareAndSet", ATOMIC_COMPARE_AND_SET_TYPE);
        code.ifeq(compareAndSet);

        code.labelBinding(next);
        code.iinc(index, 1);
        code.goto_(loop);
        code.labelBinding(exit);
        code.return_();
    }

    private static int bindSteps(
            CodeBuilder code,
            List<StepKind> steps,
            int arrays,
            int offsets,
            int[] locals,
            int[] rleSizeLocals,
            int nextLocal)
    {
        for (int step = 0; step < steps.size(); step++) {
            code.aload(steps.get(step) == StepKind.REGION ? offsets : arrays);
            code.loadConstant(step);
            if (steps.get(step) == StepKind.REGION) {
                code.iaload();
            }
            else {
                code.aaload();
                code.checkcast(CD_INT_ARRAY);
            }
            locals[step] = nextLocal;
            if (steps.get(step) == StepKind.REGION) {
                code.istore(nextLocal++);
            }
            else {
                code.astore(nextLocal++);
            }
            if (steps.get(step) == StepKind.RLE) {
                code.aload(offsets);
                code.loadConstant(step);
                code.iaload();
                rleSizeLocals[step] = nextLocal;
                code.istore(nextLocal++);
            }
        }
        return nextLocal;
    }

    private static void emitPhysicalPosition(
            CodeBuilder code,
            List<StepKind> steps,
            int[] stepLocals,
            int[] rleSizeLocals,
            int position,
            int physical,
            int low,
            int high,
            int middle)
    {
        code.iload(position);
        code.istore(physical);
        for (int step = 0; step < steps.size(); step++) {
            switch (steps.get(step)) {
                case REGION -> {
                    code.iload(physical);
                    code.iload(stepLocals[step]);
                    code.iadd();
                    code.istore(physical);
                }
                case DICTIONARY -> {
                    code.aload(stepLocals[step]);
                    code.iload(physical);
                    code.iaload();
                    code.istore(physical);
                }
                case RLE -> emitRlePosition(code, stepLocals[step], rleSizeLocals[step], physical, low, high, middle);
            }
        }
    }

    private static void emitRlePosition(CodeBuilder code, int runEnds, int runCount, int physical, int low, int high, int middle)
    {
        code.loadConstant(0);
        code.istore(low);
        code.iload(runCount);
        code.loadConstant(1);
        code.isub();
        code.istore(high);

        Label search = code.newLabel();
        Label lower = code.newLabel();
        Label done = code.newLabel();
        code.labelBinding(search);
        code.iload(low);
        code.iload(high);
        code.if_icmpgt(done);
        code.iload(low);
        code.iload(high);
        code.iadd();
        code.loadConstant(1);
        code.iushr();
        code.istore(middle);
        code.iload(physical);
        code.aload(runEnds);
        code.iload(middle);
        code.iaload();
        code.if_icmplt(lower);
        code.iload(middle);
        code.loadConstant(1);
        code.iadd();
        code.istore(low);
        code.goto_(search);
        code.labelBinding(lower);
        code.iload(middle);
        code.loadConstant(1);
        code.isub();
        code.istore(high);
        code.goto_(search);
        code.labelBinding(done);
        code.iload(low);
        code.istore(physical);
    }
}
