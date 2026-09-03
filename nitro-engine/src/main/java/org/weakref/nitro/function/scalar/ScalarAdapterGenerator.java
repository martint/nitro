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
import org.weakref.nitro.core.function.ScalarResultWriter;
import org.weakref.nitro.core.function.ScalarResultWriterFactory;
import org.weakref.nitro.core.type.TypeBinding;

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

/// Classfile API adapter from an exact scalar MethodHandle to Nitro's masked batch convention.
///
/// The generated convention admits strict functions whose arguments and results use primitive stack carriers
/// or registry-owned reference carriers. Reference arguments provide an exact
/// [org.weakref.nitro.core.type.TypeBinding#scalarValueReader()] handle; reference results provide a
/// [org.weakref.nitro.core.function.ScalarResultWriterFactory] that copies each result immediately into
/// allocator-owned vector storage. Carrier combinations and arity are generator inputs rather than Java
/// interface types, so expanding the convention does not expand the API or teach Nitro about host carrier
/// classes.
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
    private static final ClassDesc CD_VECTOR = ClassDesc.of("org.weakref.nitro.data.Vector");
    private static final ClassDesc CD_RESULT_WRITER = ClassDesc.of("org.weakref.nitro.core.function.ScalarResultWriter");

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
        ScalarResultWriterFactory resultWriterFactory = resultWriterFactory(signature.resultType());
        validate(signature, semantics, target.target(), resultWriterFactory);
        return descriptor(name, signature, semantics, target, resultWriterFactory, List.of(target));
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
        ScalarResultWriterFactory resultWriterFactory = resultWriterFactory(signature.resultType());
        validateTarget(signature, target.target(), resultWriterFactory);
        return descriptor(
                name,
                signature,
                semantics,
                target,
                resultWriterFactory,
                List.of(new BoundNullPropagatingTarget(signature, target.target())));
    }

    private ScalarDescriptor descriptor(
            String name,
            BoundSignature signature,
            FunctionSemantics semantics,
            ScalarMethodTarget target,
            ScalarResultWriterFactory resultWriterFactory,
            List<FunctionCapability> capabilities)
    {
        return new ScalarDescriptor(
                name,
                semantics.deterministic(),
                new FrameworkManagedScalarFunction(
                        name,
                        signature,
                        generate(signature, target, resultWriterFactory),
                        resultWriterFactory),
                capabilities);
    }

    private GeneratedScalarKernel generate(
            BoundSignature signature,
            ScalarMethodTarget target,
            ScalarResultWriterFactory resultWriterFactory)
    {
        List<TypeBinding> argumentTypes = signature.argumentTypes();
        List<Class<?>> arguments = argumentTypes.stream()
                .<Class<?>>map(TypeBinding::carrierType)
                .toList();
        List<MethodHandle> readers = argumentTypes.stream()
                // Primitive arguments are read directly by generated bytecode. Retain a non-null placeholder so
                // the class-data table remains position aligned without an API-level carrier hierarchy.
                .map(type -> type.carrierType().isPrimitive()
                        ? target.target()
                        : type.scalarValueReader().orElseThrow().asType(MethodType.methodType(Object.class, org.weakref.nitro.data.Vector.class, int.class)))
                .toList();
        Class<?> result = signature.resultType().carrierType();
        boolean referenceResult = !result.isPrimitive();
        MethodHandles.Lookup definitionLookup = MethodHandles.lookup();
        String packageName = definitionLookup.lookupClass().getPackageName();
        String className = (packageName.isEmpty() ? "" : packageName + ".") + "GeneratedScalarKernel" + nextClassId.incrementAndGet();
        ClassDesc thisClass = ClassDesc.of(className);
        MethodTypeDesc invocationType = MethodTypeDesc.of(
                descriptor(result),
                arguments.stream().map(ScalarAdapterGenerator::descriptor).toArray(ClassDesc[]::new));
        MethodHandle invocationTarget = target.target().asType(MethodType.methodType(
                referenceResult ? Object.class : result,
                arguments.stream()
                        .map(carrier -> carrier.isPrimitive() ? carrier : Object.class)
                        .toArray(Class<?>[]::new)));
        MethodHandle resultWriter = referenceResult
                ? resultWriterFactory.appendTarget().asType(
                        MethodType.methodType(void.class, ScalarResultWriter.class, int.class, Object.class))
                : target.target();
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
                    new ScalarAdapterLinkage(invocationTarget, readers, resultWriter),
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
            code.checkcast(carrier.isPrimitive()
                    ? inputForm == InputForm.ACCESSOR ? accessorDescriptor(carrier) : arrayDescriptor(carrier)
                    : CD_VECTOR);
            code.astore(valueAccessors + argument);
            if (checkNulls) {
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
        code.checkcast(result.isPrimitive() ? arrayDescriptor(result) : CD_RESULT_WRITER);
        code.iload(position);
        for (int argument = 0; argument < arguments.size(); argument++) {
            Class<?> carrier = arguments.get(argument);
            code.aload(valueAccessors + argument);
            if (!carrier.isPrimitive()) {
                if (inputForm == InputForm.DICTIONARY) {
                    code.aload(secondaryAccessors + argument);
                    code.iload(position);
                    code.iaload();
                }
                else {
                    code.iload(position);
                }
                code.invokedynamic(DynamicCallSiteDesc.of(
                        BSM_SCALAR_TARGET,
                        "read" + argument,
                        MethodTypeDesc.of(descriptor(carrier), CD_VECTOR, CD_int)));
            }
            else if (inputForm != InputForm.ACCESSOR) {
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
        if (result.isPrimitive()) {
            store(code, result);
        }
        else {
            code.invokedynamic(DynamicCallSiteDesc.of(
                    BSM_SCALAR_TARGET,
                    "write",
                    MethodTypeDesc.of(CD_void, CD_RESULT_WRITER, CD_int, CD_OBJECT)));
        }

        code.labelBinding(next);
        code.iinc(index, 1);
        code.goto_(loop);
        code.labelBinding(done);
        code.return_();
    }

    private static void validate(
            BoundSignature signature,
            FunctionSemantics semantics,
            MethodHandle target,
            ScalarResultWriterFactory resultWriterFactory)
    {
        checkArgument(semantics.argumentNullConventions().size() == signature.argumentTypes().size(),
                "Scalar semantics argument count does not match bound signature");
        checkArgument(semantics.argumentNullConventions().stream().allMatch(RETURN_NULL_ON_NULL::equals),
                "Framework-managed scalar currently requires strict arguments");
        checkArgument(!semantics.nullableResult(), "Framework-managed scalar currently requires a non-null result");
        validateTarget(signature, target, resultWriterFactory);
    }

    private static void validateTarget(
            BoundSignature signature,
            MethodHandle target,
            ScalarResultWriterFactory resultWriterFactory)
    {
        signature.argumentTypes().forEach(ScalarAdapterGenerator::checkArgumentCarrier);
        checkResultCarrier(signature.resultType(), resultWriterFactory);
        MethodType expected = MethodType.methodType(
                signature.resultType().carrierType(),
                signature.argumentTypes().stream().map(type -> type.carrierType()).toArray(Class<?>[]::new));
        checkArgument(target.type().equals(expected), "Scalar target type %s does not match bound signature %s", target.type(), expected);
    }

    private static ScalarResultWriterFactory resultWriterFactory(TypeBinding type)
    {
        Class<?> carrier = type.carrierType();
        if (carrier.isPrimitive()) {
            checkPrimitiveCarrier(carrier);
            return null;
        }
        ScalarResultWriterFactory factory = type.scalarResultWriterFactory().orElseThrow(() ->
                new IllegalArgumentException("Reference result carrier %s does not provide a result writer".formatted(carrier.getTypeName())));
        return factory;
    }

    private static void checkResultCarrier(TypeBinding type, ScalarResultWriterFactory factory)
    {
        Class<?> carrier = type.carrierType();
        if (carrier.isPrimitive()) {
            checkArgument(factory == null, "Primitive result carrier has a result writer");
            return;
        }
        requireNonNull(factory, "factory is null");
        MethodType expected = MethodType.methodType(void.class, ScalarResultWriter.class, int.class, carrier);
        checkArgument(factory.appendTarget().type().equals(expected), "Result writer type %s does not match %s", factory.appendTarget().type(), expected);
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

    private static void checkArgumentCarrier(TypeBinding type)
    {
        Class<?> carrier = type.carrierType();
        if (carrier.isPrimitive()) {
            checkPrimitiveCarrier(carrier);
            return;
        }
        MethodHandle reader = type.scalarValueReader().orElseThrow(() ->
                new IllegalArgumentException("Reference carrier %s does not provide a value reader".formatted(carrier.getTypeName())));
        MethodType expected = MethodType.methodType(carrier, org.weakref.nitro.data.Vector.class, int.class);
        checkArgument(reader.type().equals(expected), "Value reader type %s does not match %s", reader.type(), expected);
    }

    private static void checkPrimitiveCarrier(Class<?> carrier)
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
        if (!carrier.isPrimitive()) {
            // Host reference carriers are erased in generated bytecode. Constant call-site adapters retain the
            // exact registry-supplied carrier checks without requiring Nitro's defining class loader to resolve the
            // host class from the hidden class's constant pool.
            return CD_OBJECT;
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
