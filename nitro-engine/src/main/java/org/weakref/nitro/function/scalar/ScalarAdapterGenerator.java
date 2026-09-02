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
package org.weakref.nitro.function.scalar;

import org.weakref.nitro.core.function.BoundSignature;
import org.weakref.nitro.core.function.FunctionCapability;
import org.weakref.nitro.core.function.FunctionSemantics;
import org.weakref.nitro.core.function.NullPropagatingScalarInvocationProvider;

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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.constant.ConstantDescs.CD_CallSite;
import static java.lang.constant.ConstantDescs.CD_boolean;
import static java.lang.constant.ConstantDescs.CD_double;
import static java.lang.constant.ConstantDescs.CD_int;
import static java.lang.constant.ConstantDescs.CD_long;
import static java.lang.constant.ConstantDescs.CD_void;
import static java.lang.constant.ConstantDescs.ofCallsiteBootstrap;
import static java.util.Objects.requireNonNull;
import static org.weakref.nitro.core.function.FunctionSemantics.ArgumentNullConvention.RETURN_NULL_ON_NULL;
import static org.weakref.nitro.core.function.FunctionSemantics.FailureConvention.NEVER_FAILS;

/// Classfile API adapter from an exact scalar MethodHandle to Nitro's masked batch convention.
///
/// The first implementation deliberately admits only strict, non-nullable, non-failing functions
/// over primitive stack carriers. Carrier combinations and arity are generator inputs rather than
/// Java interface types, so expanding the admitted calling convention does not expand the API.
public final class ScalarAdapterGenerator
{
    private static final ClassDesc CD_OBJECT = ClassDesc.of("java.lang.Object");
    private static final ClassDesc CD_OBJECT_ARRAY = ClassDesc.ofDescriptor("[Ljava/lang/Object;");
    private static final ClassDesc CD_INT_ARRAY = ClassDesc.ofDescriptor("[I");
    private static final ClassDesc CD_INT_ARRAY_ARRAY = ClassDesc.ofDescriptor("[[I");
    private static final ClassDesc CD_LONG_ARRAY = ClassDesc.ofDescriptor("[J");
    private static final ClassDesc CD_DOUBLE_ARRAY = ClassDesc.ofDescriptor("[D");
    private static final ClassDesc CD_BOOLEAN_ARRAY = ClassDesc.ofDescriptor("[Z");
    private static final ClassDesc CD_BOOTSTRAP = ClassDesc.of("org.weakref.nitro.function.scalar.ScalarAdapterBootstrap");
    private static final ClassDesc CD_KERNEL = ClassDesc.of("org.weakref.nitro.function.scalar.GeneratedScalarKernel");
    private static final ClassDesc CD_LONG_VALUES = ClassDesc.of("org.weakref.nitro.data.VectorAccess$LongValues");
    private static final ClassDesc CD_DOUBLE_VALUES = ClassDesc.of("org.weakref.nitro.data.VectorAccess$DoubleValues");
    private static final ClassDesc CD_BOOLEAN_VALUES = ClassDesc.of("org.weakref.nitro.data.VectorAccess$BooleanValues");

    private static final MethodTypeDesc BOOLEAN_VALUE = MethodTypeDesc.of(CD_boolean, CD_int);
    private static final MethodTypeDesc DENSE_FLAT_NULL_FREE = MethodTypeDesc.of(CD_void, CD_OBJECT_ARRAY, CD_OBJECT, CD_int);
    private static final MethodTypeDesc SPARSE_FLAT_NULL_FREE = MethodTypeDesc.of(CD_void, CD_OBJECT_ARRAY, CD_OBJECT, CD_INT_ARRAY, CD_int);
    private static final MethodTypeDesc DENSE_DICTIONARY_NULL_FREE = MethodTypeDesc.of(CD_void, CD_OBJECT_ARRAY, CD_INT_ARRAY_ARRAY, CD_OBJECT, CD_int);
    private static final MethodTypeDesc SPARSE_DICTIONARY_NULL_FREE = MethodTypeDesc.of(CD_void, CD_OBJECT_ARRAY, CD_INT_ARRAY_ARRAY, CD_OBJECT, CD_INT_ARRAY, CD_int);
    private static final MethodTypeDesc DENSE = MethodTypeDesc.of(CD_void, CD_OBJECT_ARRAY, CD_OBJECT_ARRAY, CD_OBJECT, CD_int);
    private static final MethodTypeDesc SPARSE = MethodTypeDesc.of(CD_void, CD_OBJECT_ARRAY, CD_OBJECT_ARRAY, CD_OBJECT, CD_INT_ARRAY, CD_int);
    private static final DirectMethodHandleDesc BSM_SCALAR_TARGET = ofCallsiteBootstrap(CD_BOOTSTRAP, "bootstrap", CD_CallSite);

    private final AtomicInteger nextClassId = new AtomicInteger();

    public ScalarDescriptor adapt(
            String name,
            BoundSignature signature,
            FunctionSemantics semantics,
            ScalarMethodTarget target)
    {
        requireNonNull(name, "name is null");
        requireNonNull(signature, "signature is null");
        requireNonNull(semantics, "semantics is null");
        requireNonNull(target, "target is null");
        validate(signature, semantics, target.target());
        return descriptor(name, signature, semantics, target, List.of(target));
    }

    /**
     * Adapts a non-null primitive target when the registry asserts that null propagation belongs to the framework.
     *
     * <p>The supplied target is invoked only when every argument is non-null. This entry point is intentionally
     * separate from {@link #adapt}: nullable host calling conventions do not by themselves prove null-propagating
     * semantics. The registry adapter making this call supplies that proof for this exact logical signature.
     */
    public ScalarDescriptor adaptNullPropagating(
            String name,
            BoundSignature signature,
            FunctionSemantics semantics,
            ScalarMethodTarget target)
    {
        requireNonNull(name, "name is null");
        requireNonNull(signature, "signature is null");
        requireNonNull(semantics, "semantics is null");
        requireNonNull(target, "target is null");
        checkArgument(semantics.argumentNullConventions().size() == signature.argumentTypes().size(),
                "Scalar semantics argument count does not match bound signature");
        checkArgument(semantics.failureConvention() == NEVER_FAILS,
                "Framework-managed null-propagating scalar currently requires a non-failing target");
        validateTarget(signature, target.target());
        return descriptor(
                name,
                signature,
                semantics,
                target,
                List.of(new BoundNullPropagatingTarget(signature, target.target())));
    }

    private ScalarDescriptor descriptor(
            String name,
            BoundSignature signature,
            FunctionSemantics semantics,
            ScalarMethodTarget target,
            List<FunctionCapability> capabilities)
    {
        return new ScalarDescriptor(
                name,
                semantics.deterministic(),
                new FrameworkManagedScalarFunction(name, signature, generate(signature, target)),
                capabilities);
    }

    private GeneratedScalarKernel generate(BoundSignature signature, ScalarMethodTarget target)
    {
        List<Class<?>> arguments = signature.argumentTypes().stream()
                .<Class<?>>map(type -> type.carrierType())
                .toList();
        Class<?> result = signature.resultType().carrierType();
        MethodHandles.Lookup definitionLookup = MethodHandles.lookup();
        String packageName = definitionLookup.lookupClass().getPackageName();
        String className = (packageName.isEmpty() ? "" : packageName + ".") + "GeneratedScalarKernel" + nextClassId.incrementAndGet();
        ClassDesc thisClass = ClassDesc.of(className);
        MethodTypeDesc invocationType = MethodTypeDesc.of(
                descriptor(result),
                arguments.stream().map(ScalarAdapterGenerator::descriptor).toArray(ClassDesc[]::new));
        byte[] bytes = ClassFile.of().build(thisClass, builder -> {
            builder.withSuperclass(CD_OBJECT);
            builder.withInterfaceSymbols(CD_KERNEL);
            builder.withFlags(ClassFile.ACC_FINAL | ClassFile.ACC_SYNTHETIC);
            builder.withMethodBody("<init>", MethodTypeDesc.of(CD_void), ClassFile.ACC_PUBLIC, code -> {
                code.aload(0);
                code.invokespecial(CD_OBJECT, "<init>", MethodTypeDesc.of(CD_void));
                code.return_();
            });
            builder.withMethodBody("applyDenseFlatNullFree", DENSE_FLAT_NULL_FREE, ClassFile.ACC_PUBLIC,
                    code -> emitLoop(code, arguments, result, invocationType, false, InputForm.FLAT));
            builder.withMethodBody("applySparseFlatNullFree", SPARSE_FLAT_NULL_FREE, ClassFile.ACC_PUBLIC,
                    code -> emitLoop(code, arguments, result, invocationType, true, InputForm.FLAT));
            builder.withMethodBody("applyDenseDictionaryNullFree", DENSE_DICTIONARY_NULL_FREE, ClassFile.ACC_PUBLIC,
                    code -> emitLoop(code, arguments, result, invocationType, false, InputForm.DICTIONARY));
            builder.withMethodBody("applySparseDictionaryNullFree", SPARSE_DICTIONARY_NULL_FREE, ClassFile.ACC_PUBLIC,
                    code -> emitLoop(code, arguments, result, invocationType, true, InputForm.DICTIONARY));
            builder.withMethodBody("applyDense", DENSE, ClassFile.ACC_PUBLIC,
                    code -> emitLoop(code, arguments, result, invocationType, false, InputForm.ACCESSOR));
            builder.withMethodBody("applySparse", SPARSE, ClassFile.ACC_PUBLIC,
                    code -> emitLoop(code, arguments, result, invocationType, true, InputForm.ACCESSOR));
        });

        try {
            MethodHandles.Lookup lookup = definitionLookup.defineHiddenClassWithClassData(
                    bytes,
                    target.target(),
                    true,
                    MethodHandles.Lookup.ClassOption.NESTMATE);
            return (GeneratedScalarKernel) lookup.findConstructor(lookup.lookupClass(), MethodType.methodType(void.class)).invoke();
        }
        catch (Throwable throwable) {
            throw new IllegalStateException("Failed to generate scalar adapter for " + target.target().type(), throwable);
        }
    }

    private static void emitLoop(
            CodeBuilder code,
            List<Class<?>> arguments,
            Class<?> result,
            MethodTypeDesc invocationType,
            boolean sparse,
            InputForm inputForm)
    {
        int values = 1;
        boolean checkNulls = inputForm == InputForm.ACCESSOR;
        int nulls = checkNulls ? 2 : -1;
        int ids = inputForm == InputForm.DICTIONARY ? 2 : -1;
        int output = inputForm == InputForm.FLAT ? 2 : 3;
        int positions = sparse ? output + 1 : -1;
        int count = sparse ? output + 2 : output + 1;
        int index = count + 1;
        int position = index + 1;
        int valueAccessors = position + 1;
        int secondaryAccessors = valueAccessors + arguments.size();

        for (int argument = 0; argument < arguments.size(); argument++) {
            Class<?> carrier = arguments.get(argument);
            code.aload(values);
            code.loadConstant(argument);
            code.aaload();
            code.checkcast(inputForm == InputForm.ACCESSOR ? accessorDescriptor(carrier) : arrayDescriptor(carrier));
            code.astore(valueAccessors + argument);
            if (inputForm == InputForm.ACCESSOR) {
                code.aload(nulls);
                code.loadConstant(argument);
                code.aaload();
                code.checkcast(CD_BOOLEAN_VALUES);
                code.astore(secondaryAccessors + argument);
            }
            else if (inputForm == InputForm.DICTIONARY) {
                code.aload(ids);
                code.loadConstant(argument);
                code.aaload();
                code.checkcast(CD_INT_ARRAY);
                code.astore(secondaryAccessors + argument);
            }
        }

        Label loop = code.newLabel();
        Label next = code.newLabel();
        Label done = code.newLabel();
        code.loadConstant(0);
        code.istore(index);
        code.labelBinding(loop);
        code.iload(index);
        code.iload(count);
        code.if_icmpge(done);
        if (sparse) {
            code.aload(positions);
            code.iload(index);
            code.iaload();
        }
        else {
            code.iload(index);
        }
        code.istore(position);

        if (checkNulls) {
            for (int argument = 0; argument < arguments.size(); argument++) {
                Label notNull = code.newLabel();
                code.aload(secondaryAccessors + argument);
                code.ifnull(notNull);
                code.aload(secondaryAccessors + argument);
                code.iload(position);
                code.invokeinterface(CD_BOOLEAN_VALUES, "value", BOOLEAN_VALUE);
                code.ifne(next);
                code.labelBinding(notNull);
            }
        }

        code.aload(output);
        code.checkcast(arrayDescriptor(result));
        code.iload(position);
        for (int argument = 0; argument < arguments.size(); argument++) {
            Class<?> carrier = arguments.get(argument);
            code.aload(valueAccessors + argument);
            if (inputForm != InputForm.ACCESSOR) {
                if (inputForm == InputForm.DICTIONARY) {
                    code.aload(secondaryAccessors + argument);
                    code.iload(position);
                    code.iaload();
                }
                else {
                    code.iload(position);
                }
                load(code, carrier);
            }
            else {
                code.iload(position);
                code.invokeinterface(accessorDescriptor(carrier), "value", MethodTypeDesc.of(descriptor(carrier), CD_int));
            }
        }
        code.invokedynamic(DynamicCallSiteDesc.of(BSM_SCALAR_TARGET, "apply", invocationType));
        store(code, result);

        code.labelBinding(next);
        code.iinc(index, 1);
        code.goto_(loop);
        code.labelBinding(done);
        code.return_();
    }

    private static void validate(BoundSignature signature, FunctionSemantics semantics, MethodHandle target)
    {
        checkArgument(semantics.argumentNullConventions().size() == signature.argumentTypes().size(),
                "Scalar semantics argument count does not match bound signature");
        checkArgument(semantics.argumentNullConventions().stream().allMatch(RETURN_NULL_ON_NULL::equals),
                "Framework-managed scalar currently requires strict arguments");
        checkArgument(!semantics.nullableResult(), "Framework-managed scalar currently requires a non-null result");
        checkArgument(semantics.failureConvention() == NEVER_FAILS, "Framework-managed scalar currently requires a non-failing target");
        validateTarget(signature, target);
    }

    private static void validateTarget(BoundSignature signature, MethodHandle target)
    {
        signature.argumentTypes().forEach(type -> checkCarrier(type.carrierType()));
        checkCarrier(signature.resultType().carrierType());
        MethodType expected = MethodType.methodType(
                signature.resultType().carrierType(),
                signature.argumentTypes().stream().map(type -> type.carrierType()).toArray(Class<?>[]::new));
        checkArgument(target.type().equals(expected), "Scalar target type %s does not match bound signature %s", target.type(), expected);
    }

    private record BoundNullPropagatingTarget(BoundSignature signature, MethodHandle target)
            implements NullPropagatingScalarInvocationProvider
    {
        private BoundNullPropagatingTarget
        {
            requireNonNull(signature, "signature is null");
            requireNonNull(target, "target is null");
        }

        @Override
        public Optional<MethodHandle> target(BoundSignature requestedSignature)
        {
            return signature.equals(requestedSignature) ? Optional.of(target) : Optional.empty();
        }
    }

    private static void checkCarrier(Class<?> carrier)
    {
        checkArgument(carrier == long.class || carrier == double.class || carrier == boolean.class,
                "Unsupported framework-managed carrier: %s", carrier.getTypeName());
    }

    private static ClassDesc descriptor(Class<?> carrier)
    {
        if (carrier == long.class) {
            return CD_long;
        }
        if (carrier == double.class) {
            return CD_double;
        }
        if (carrier == boolean.class) {
            return CD_boolean;
        }
        throw new IllegalArgumentException("Unsupported framework-managed carrier: " + carrier.getTypeName());
    }

    private static ClassDesc accessorDescriptor(Class<?> carrier)
    {
        if (carrier == long.class) {
            return CD_LONG_VALUES;
        }
        if (carrier == double.class) {
            return CD_DOUBLE_VALUES;
        }
        if (carrier == boolean.class) {
            return CD_BOOLEAN_VALUES;
        }
        throw new IllegalArgumentException("Unsupported framework-managed carrier: " + carrier.getTypeName());
    }

    private static ClassDesc arrayDescriptor(Class<?> carrier)
    {
        if (carrier == long.class) {
            return CD_LONG_ARRAY;
        }
        if (carrier == double.class) {
            return CD_DOUBLE_ARRAY;
        }
        if (carrier == boolean.class) {
            return CD_BOOLEAN_ARRAY;
        }
        throw new IllegalArgumentException("Unsupported framework-managed carrier: " + carrier.getTypeName());
    }

    private static void store(CodeBuilder code, Class<?> carrier)
    {
        if (carrier == long.class) {
            code.lastore();
            return;
        }
        if (carrier == double.class) {
            code.dastore();
            return;
        }
        if (carrier == boolean.class) {
            code.bastore();
            return;
        }
        throw new IllegalArgumentException("Unsupported framework-managed carrier: " + carrier.getTypeName());
    }

    private static void load(CodeBuilder code, Class<?> carrier)
    {
        if (carrier == long.class) {
            code.laload();
            return;
        }
        if (carrier == double.class) {
            code.daload();
            return;
        }
        if (carrier == boolean.class) {
            code.baload();
            return;
        }
        throw new IllegalArgumentException("Unsupported framework-managed carrier: " + carrier.getTypeName());
    }

    private enum InputForm
    {
        ACCESSOR,
        FLAT,
        DICTIONARY,
    }
}
