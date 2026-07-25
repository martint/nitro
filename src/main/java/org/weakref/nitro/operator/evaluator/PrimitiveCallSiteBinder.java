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
package org.weakref.nitro.operator.evaluator;

import java.lang.classfile.ClassFile;
import java.lang.constant.ClassDesc;
import java.lang.constant.MethodTypeDesc;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.constant.ConstantDescs.CD_boolean;
import static java.lang.constant.ConstantDescs.CD_int;
import static java.lang.constant.ConstantDescs.CD_void;
import static java.util.Objects.requireNonNull;

/// Binds a provider implementation behind an engine-visible generated call site.
///
/// The generated class names only JDK and Nitro invocation-protocol classes. Its constant pool and
/// descriptors never name the provider's concrete class, so the target may live in an isolated
/// plugin class loader.
public final class PrimitiveCallSiteBinder
{
    private static final ClassDesc CD_OBJECT = ClassDesc.of("java.lang.Object");
    private static final ClassDesc CD_LIST = ClassDesc.of("java.util.List");
    private static final ClassDesc CD_SET = ClassDesc.of("java.util.Set");
    private static final ClassDesc CD_MASK = ClassDesc.of("org.weakref.nitro.data.Mask");
    private static final ClassDesc CD_STREAMS = ClassDesc.of("org.weakref.nitro.data.Streams");
    private static final ClassDesc CD_EXECUTION_CONTEXT = ClassDesc.of("org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext");
    private static final ClassDesc CD_PRIMITIVE = ClassDesc.of("org.weakref.nitro.operator.evaluator.PrimitiveFunction");

    private static final MethodTypeDesc APPLY_TYPE = MethodTypeDesc.of(CD_STREAMS, CD_LIST, CD_MASK, CD_SET, CD_STREAMS, CD_EXECUTION_CONTEXT);
    private static final MethodTypeDesc REQUIRED_INPUT_STREAMS_TYPE = MethodTypeDesc.of(CD_SET, CD_int, CD_SET);
    private static final MethodTypeDesc REQUIRED_MASK_INPUT_STREAMS_TYPE = MethodTypeDesc.of(CD_SET, CD_int);
    private static final MethodTypeDesc BOOLEAN_TYPE = MethodTypeDesc.of(CD_boolean);
    private static final MethodTypeDesc SET_TYPE = MethodTypeDesc.of(CD_SET);

    private final AtomicInteger nextClassId = new AtomicInteger();

    public PrimitiveFunction bind(PrimitiveFunction target)
    {
        requireNonNull(target, "target is null");
        ClassDesc thisClass = ClassDesc.of("org.weakref.nitro.operator.evaluator.GeneratedPrimitiveCallSite" + nextClassId.incrementAndGet());
        byte[] bytes = ClassFile.of().build(thisClass, builder -> {
            builder.withSuperclass(CD_OBJECT);
            builder.withInterfaceSymbols(CD_PRIMITIVE);
            builder.withFlags(ClassFile.ACC_FINAL | ClassFile.ACC_SYNTHETIC);
            builder.withField("target", CD_PRIMITIVE, ClassFile.ACC_PRIVATE | ClassFile.ACC_FINAL);
            builder.withMethodBody("<init>", MethodTypeDesc.of(CD_void, CD_PRIMITIVE), ClassFile.ACC_PUBLIC, code -> {
                code.aload(0);
                code.invokespecial(CD_OBJECT, "<init>", MethodTypeDesc.of(CD_void));
                code.aload(0);
                code.aload(1);
                code.putfield(thisClass, "target", CD_PRIMITIVE);
                code.return_();
            });
            builder.withMethodBody("apply", APPLY_TYPE, ClassFile.ACC_PUBLIC, code -> {
                loadTarget(code, thisClass);
                code.aload(1);
                code.aload(2);
                code.aload(3);
                code.aload(4);
                code.aload(5);
                code.invokeinterface(CD_PRIMITIVE, "apply", APPLY_TYPE);
                code.areturn();
            });
            builder.withMethodBody("requiredInputStreams", REQUIRED_INPUT_STREAMS_TYPE, ClassFile.ACC_PUBLIC, code -> {
                loadTarget(code, thisClass);
                code.iload(1);
                code.aload(2);
                code.invokeinterface(CD_PRIMITIVE, "requiredInputStreams", REQUIRED_INPUT_STREAMS_TYPE);
                code.areturn();
            });
            builder.withMethodBody("requiredMaskInputStreams", REQUIRED_MASK_INPUT_STREAMS_TYPE, ClassFile.ACC_PUBLIC, code -> {
                loadTarget(code, thisClass);
                code.iload(1);
                code.invokeinterface(CD_PRIMITIVE, "requiredMaskInputStreams", REQUIRED_MASK_INPUT_STREAMS_TYPE);
                code.areturn();
            });
            delegateBoolean(builder, thisClass, "deterministic");
            delegateBoolean(builder, thisClass, "propagatesNulls");
            builder.withMethodBody("allocationContexts", SET_TYPE, ClassFile.ACC_PUBLIC, code -> {
                loadTarget(code, thisClass);
                code.invokeinterface(CD_PRIMITIVE, "allocationContexts", SET_TYPE);
                code.areturn();
            });
        });

        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup().defineHiddenClass(bytes, true, MethodHandles.Lookup.ClassOption.NESTMATE);
            return (PrimitiveFunction) lookup.findConstructor(lookup.lookupClass(), MethodType.methodType(void.class, PrimitiveFunction.class)).invoke(target);
        }
        catch (Throwable throwable) {
            throw new IllegalStateException("Failed to bind primitive call site", throwable);
        }
    }

    private static void delegateBoolean(java.lang.classfile.ClassBuilder builder, ClassDesc thisClass, String method)
    {
        builder.withMethodBody(method, BOOLEAN_TYPE, ClassFile.ACC_PUBLIC, code -> {
            loadTarget(code, thisClass);
            code.invokeinterface(CD_PRIMITIVE, method, BOOLEAN_TYPE);
            code.ireturn();
        });
    }

    private static void loadTarget(java.lang.classfile.CodeBuilder code, ClassDesc thisClass)
    {
        code.aload(0);
        code.getfield(thisClass, "target", CD_PRIMITIVE);
    }
}
